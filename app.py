"""
LinkedIn Cold Outreach Pipeline - Backend
==========================================
What this file does in plain English:
  1. Serves index.html when you open the site in a browser
  2. Accepts a POST to /generate with LinkedIn URLs + company name
  3. Streams results back one profile at a time using Server-Sent Events (SSE)
     — each profile appears in the table as soon as its message is written,
       rather than waiting for the whole batch to finish
  4. For each profile: runs the Tier 1 scrapers (atomus + supreme_coder) in parallel,
     falls back to Tier 2 if both miss, then calls Groq to write the message

Why FastAPI instead of Flask:
  Flask's synchronous request handler conflicts with asyncio.
  FastAPI is built for async Python natively — no workarounds needed.

Why SSE (Server-Sent Events) instead of a regular response:
  A full scrape run takes 2–5 minutes. Render's free tier kills requests
  after 30 seconds. SSE keeps the connection open and streams data as it
  arrives, so Render never times out the request.
"""

import os
import json
import asyncio
import aiohttp
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from dotenv import load_dotenv

# Load API keys from the .env file
load_dotenv()
APIFY_TOKEN      = os.getenv("APIFY_TOKEN")
GROQ_API_KEY     = os.getenv("GROQ_API_KEY")
NVIDIA_API_KEY   = os.getenv("NVIDIA_API_KEY")
SUPABASE_URL     = os.getenv("SUPABASE_URL")       # e.g. https://xxxx.supabase.co
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")   # anon public key

app = FastAPI()


# ─────────────────────────────────────────────
# SECTION 1: SERVE THE FRONTEND
# ─────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def home():
    with open("home.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/outreach", response_class=HTMLResponse)
async def index():
    with open("index.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/debug")
async def debug():
    """Quick health check — tests Groq directly and reports status."""
    result = {"groq_key_set": bool(GROQ_API_KEY), "groq_model": GROQ_MODEL, "apify_key_set": bool(APIFY_TOKEN), "apify_token_prefix": (APIFY_TOKEN or "")[:12]}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                GROQ_URL,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={"model": GROQ_MODEL, "messages": [{"role": "user", "content": "Say hi."}], "max_tokens": 10},
                timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                body = await r.text()
                result["groq_status"] = r.status
                result["groq_response"] = body[:300]
    except Exception as e:
        result["groq_error"] = str(e)
    return result


@app.get("/test-apify")
async def test_apify():
    """Test Apify actor start + result fetch directly."""
    import traceback
    result = {"apify_token_set": bool(APIFY_TOKEN), "apify_token_prefix": (APIFY_TOKEN or "")[:12]}
    actor = "supreme_coder/linkedin-profile-scraper"
    url_to_scrape = "https://www.linkedin.com/in/amber-sahu/"
    try:
        async with aiohttp.ClientSession() as s:
            slug = actor.replace("/", "~")
            start_url = f"{APIFY_BASE}/acts/{slug}/runs?token={APIFY_TOKEN}"
            payload = {"urls": [{"url": url_to_scrape}]}
            print(f"[test-apify] POST {start_url[:80]}")
            async with s.post(start_url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as r:
                body = await r.text()
                result["start_status"] = r.status
                result["start_body"] = body[:500]
                if r.status in (200, 201):
                    run_id = json.loads(body).get("data", {}).get("id")
                    result["run_id"] = run_id
                    if run_id:
                        succeeded = await wait_for_apify_run(s, run_id)
                        result["run_succeeded"] = succeeded
                        if succeeded:
                            items_url = f"{APIFY_BASE}/actor-runs/{run_id}/dataset/items?token={APIFY_TOKEN}"
                            async with s.get(items_url, timeout=aiohttp.ClientTimeout(total=30)) as r2:
                                result["items_status"] = r2.status
                                result["items_body"] = (await r2.text())[:1000]
    except Exception as e:
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
    return result




# ─────────────────────────────────────────────
# SECTION 2: APIFY HELPERS
# ─────────────────────────────────────────────
# Apify works in two steps:
#   Step A — start a scraper run, get back a run ID
#   Step B — wait, then fetch the results using that run ID

APIFY_BASE = "https://api.apify.com/v2"


async def run_apify_actor(session, actor_slug, input_payload):
    """
    Starts one Apify actor run.
    Returns the run ID string, or None if it failed to start.
    """
    # Apify API uses username~actor-name format, not username/actor-name
    slug_for_url = actor_slug.replace("/", "~")
    url = f"{APIFY_BASE}/acts/{slug_for_url}/runs?token={APIFY_TOKEN}"
    try:
        async with session.post(url, json=input_payload,
                                timeout=aiohttp.ClientTimeout(total=30)) as resp:
            body = await resp.text()
            if resp.status in (200, 201):
                data = json.loads(body)
                run_id = data.get("data", {}).get("id")
                print(f"[Apify] {actor_slug} started run={run_id}")
                return run_id
            else:
                print(f"[Apify] {actor_slug} start failed status={resp.status} body={body[:200]}")
    except Exception as e:
        print(f"[Apify] {actor_slug} failed to start: {e}")
    return None


async def wait_for_apify_run(session, run_id, max_wait=90, poll_interval=5):
    """
    Polls Apify every poll_interval seconds until the run reaches a terminal
    state (SUCCEEDED, FAILED, ABORTED, TIMED-OUT) or max_wait is exceeded.
    Returns True if succeeded, False otherwise.
    """
    url = f"{APIFY_BASE}/actor-runs/{run_id}?token={APIFY_TOKEN}"
    elapsed = 0
    while elapsed < max_wait:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status = data.get("data", {}).get("status", "")
                    print(f"[Apify] run={run_id} status={status} elapsed={elapsed}s")
                    if status == "SUCCEEDED":
                        return True
                    if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                        return False
        except Exception as e:
            print(f"[Apify] poll error run={run_id}: {e}")
    print(f"[Apify] run={run_id} timed out after {max_wait}s")
    return False


async def get_apify_results(session, run_id):
    """
    Fetches the dataset items for a completed Apify run.
    Returns a list of result dicts, or [] if nothing came back.
    """
    url = f"{APIFY_BASE}/actor-runs/{run_id}/dataset/items?token={APIFY_TOKEN}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 200:
                items = await resp.json()
                print(f"[Apify] run={run_id} got {len(items)} items")
                return items
            else:
                body = await resp.text()
                print(f"[Apify] run={run_id} fetch failed status={resp.status} body={body[:200]}")
    except Exception as e:
        print(f"[Apify] run={run_id} fetch exception: {e}")
    return []


# ─────────────────────────────────────────────
# SECTION 3: FIELD NORMALIZERS
# ─────────────────────────────────────────────
# Every scraper returns data with different key names.
# Each normalizer converts one scraper's raw output into the same
# clean dict shape: first_name, last_name, headline, summary,
# experiences (list of dicts), current_company, education.

def normalize_atomus(raw):
    if not raw:
        return None
    exps = raw.get("experiences", [])[:3]
    return {
        "first_name":      raw.get("firstName", ""),
        "last_name":       raw.get("lastName", ""),
        "headline":        raw.get("headline", ""),
        "summary":         raw.get("summary", ""),
        "experiences":     [{"title": e.get("title",""), "company": e.get("companyName",""), "description": e.get("description","")} for e in exps],
        "current_company": raw.get("currentCompany","") or (exps[0].get("companyName","") if exps else ""),
        "education":       [{"school": e.get("schoolName",""), "degree": e.get("degreeName","")} for e in raw.get("education",[])[:2]],
    }


def normalize_supreme_coder(raw):
    if not raw:
        return None
    # supreme_coder returns positions as company-groups, each with a nested
    # positions[] array of individual roles. Flatten to a list of role dicts.
    raw_positions = raw.get("positions", raw.get("experiences", []))
    exps = []
    for group in raw_positions:
        company_name = group.get("companyName") or group.get("company", {}).get("name", "")
        inner_roles = group.get("positions", [])
        if inner_roles:
            for role in inner_roles:
                exps.append({
                    "title":       role.get("title", ""),
                    "company":     company_name,
                    "description": role.get("description", ""),
                })
        else:
            # Flat entry (older actor version)
            exps.append({
                "title":       group.get("title", ""),
                "company":     company_name,
                "description": group.get("description", ""),
            })
    exps = exps[:3]
    current_co = raw.get("currentCompany", {})
    if isinstance(current_co, dict):
        current_co = current_co.get("name", "")
    return {
        "first_name":      raw.get("firstName", ""),
        "last_name":       raw.get("lastName", ""),
        "headline":        raw.get("headline", ""),
        "summary":         raw.get("about","") or raw.get("summary",""),
        "experiences":     exps,
        "current_company": current_co or raw.get("companyName","") or (exps[0].get("company","") if exps else ""),
        "education":       [{"school": e.get("schoolName",""), "degree": e.get("degreeName","")} for e in raw.get("educations", raw.get("education",[]))[:2]],
    }


def normalize_apimaestro(raw):
    if not raw:
        return None
    basic = raw.get("basic_info", {})
    exps  = raw.get("experience", [])[:3]
    return {
        "first_name":      basic.get("first_name",""),
        "last_name":       basic.get("last_name",""),
        "headline":        basic.get("headline",""),
        "summary":         basic.get("about",""),
        "experiences":     [{"title": e.get("title",""), "company": e.get("company",""), "description": e.get("description","")} for e in exps],
        "current_company": basic.get("current_company","") or (exps[0].get("company","") if exps else ""),
        "education":       [{"school": e.get("school",""), "degree": e.get("degree","")} for e in raw.get("education",[])[:2]],
    }


def normalize_harvestapi(raw):
    if not raw:
        return None
    exps = raw.get("currentPosition", [{}])[:3]
    return {
        "first_name":      raw.get("firstName",""),
        "last_name":       raw.get("lastName",""),
        "headline":        raw.get("headline",""),
        "summary":         raw.get("about",""),
        "experiences":     [{"title": e.get("position",""), "company": e.get("companyName",""), "description": e.get("description","")} for e in exps],
        "current_company": exps[0].get("companyName","") if exps else "",
        "education":       [{"school": e.get("schoolName",""), "degree": e.get("degreeName","")} for e in raw.get("education",[])[:2]],
    }


def normalize_datadoping(raw):
    if not raw:
        return None
    exps = raw.get("experiences", [])[:3]
    return {
        "first_name":      raw.get("firstName",""),
        "last_name":       raw.get("lastName",""),
        "headline":        raw.get("headline",""),
        "summary":         raw.get("about","") or raw.get("summary",""),
        "experiences":     [{"title": e.get("title",""), "company": e.get("companyName",""), "description": e.get("description","")} for e in exps],
        "current_company": raw.get("currentCompany","") or (exps[0].get("companyName","") if exps else ""),
        "education":       [{"school": e.get("schoolName",""), "degree": e.get("degreeName","")} for e in raw.get("education",[])[:2]],
    }


def normalize_sabania(raw):
    if not raw:
        return None
    exps = raw.get("experience", raw.get("experiences", []))[:3]
    return {
        "first_name":      raw.get("firstName", raw.get("first_name","")),
        "last_name":       raw.get("lastName",  raw.get("last_name","")),
        "headline":        raw.get("headline",""),
        "summary":         raw.get("summary", raw.get("about","")),
        "experiences":     [{"title": e.get("title",""), "company": e.get("company", e.get("companyName","")), "description": e.get("description","")} for e in exps],
        "current_company": raw.get("currentCompany", exps[0].get("company", exps[0].get("companyName","")) if exps else ""),
        "education":       [{"school": e.get("school", e.get("schoolName","")), "degree": e.get("degree", e.get("degreeName",""))} for e in raw.get("education",[])[:2]],
    }


def normalize_automation_lab(raw):
    """
    # TODO: verify automation-lab fields after first test run.
    Update the key names below if the actual output uses different field names.
    """
    if not raw:
        return None
    exps = raw.get("experience", raw.get("experiences", []))[:3]
    return {
        "first_name":      raw.get("firstName", raw.get("first_name","")),
        "last_name":       raw.get("lastName",  raw.get("last_name","")),
        "headline":        raw.get("headline", raw.get("title","")),
        "summary":         raw.get("summary", raw.get("about","")),
        "experiences":     [{"title": e.get("title",""), "company": e.get("company", e.get("companyName","")), "description": e.get("description", e.get("summary",""))} for e in exps],
        "current_company": raw.get("currentCompany", exps[0].get("company","") if exps else ""),
        "education":       [{"school": e.get("school", e.get("schoolName","")), "degree": e.get("degree", e.get("degreeName",""))} for e in raw.get("education",[])[:2]],
    }


# Scraper registry — maps actor slug to its normalizer and input format
SCRAPERS = {
    "atomus/linkedin-profile-scraper": {
        "normalize": normalize_atomus,
        "input_fn":  lambda url: {"profileUrls": [url]},
    },
    "supreme_coder/linkedin-profile-scraper": {
        "normalize": normalize_supreme_coder,
                "input_fn":  lambda url: {"urls": [{"url": url}]},
    },
    "apimaestro/linkedin-profile-detail": {
        "normalize": normalize_apimaestro,
        "input_fn":  lambda url: {"url": url},
    },
    "harvestapi/linkedin-profile-scraper": {
        "normalize": normalize_harvestapi,
                "input_fn":  lambda url: {"urls": [{"url": url}]},
    },
    "datadoping/linkedin-profile-scraper": {
        "normalize": normalize_datadoping,
                "input_fn":  lambda url: {"urls": [{"url": url}]},
    },
    "sabania/linkedin-scraper": {
        "normalize": normalize_sabania,
        "input_fn":  lambda url: {"url": url},
    },
    "automation-lab/linkedin-profile-scraper": {
        "normalize": normalize_automation_lab,
                "input_fn":  lambda url: {"urls": [{"url": url}]},
        # TODO: confirm input key — may be "url" instead of "profileUrl"
    },
}

# apimaestro daily cap — resets when the server restarts
apimaestro_used_today = 0
APIMAESTRO_DAILY_CAP  = 20


# ─────────────────────────────────────────────
# SECTION 4: SCRAPER WATERFALL
# ─────────────────────────────────────────────

async def scrape_profile_tier1(session, url):
    """
    Fires Tier 1 scrapers for one URL.
    Returns normalized profile data from whichever returns data, or None.
    """
    tier1 = ["supreme_coder/linkedin-profile-scraper"]
    run_ids = {}
    for actor in tier1:
        run_id = await run_apify_actor(session, actor, SCRAPERS[actor]["input_fn"](url))
        if run_id:
            run_ids[actor] = run_id

    if not run_ids:
        return None

    # Poll each run until it completes (typically 15-20s, max 90s)
    for actor, run_id in run_ids.items():
        succeeded = await wait_for_apify_run(session, run_id)
        if not succeeded:
            print(f"[Tier1] {actor} run={run_id} did not succeed, skipping")
            continue
        results = await get_apify_results(session, run_id)
        if results:
            normalized = SCRAPERS[actor]["normalize"](results[0])
            if normalized and normalized.get("first_name"):
                return normalized

    return None


async def scrape_profile_tier2(session, url):
    """
    Tries Tier 2 scrapers one at a time for a profile Tier 1 missed.
    Returns normalized data or None if all miss.
    """
    global apimaestro_used_today

    tier2 = []
    if apimaestro_used_today < APIMAESTRO_DAILY_CAP:
        tier2.append("apimaestro/linkedin-profile-detail")
    tier2 += [
        "harvestapi/linkedin-profile-scraper",
        "datadoping/linkedin-profile-scraper",
        "sabania/linkedin-scraper",
        "automation-lab/linkedin-profile-scraper",
    ]

    for actor in tier2:
        run_id = await run_apify_actor(session, actor, SCRAPERS[actor]["input_fn"](url))
        if not run_id:
            continue

        if actor == "apimaestro/linkedin-profile-detail":
            apimaestro_used_today += 1

        succeeded = await wait_for_apify_run(session, run_id, max_wait=60)
        if not succeeded:
            print(f"[Tier2] {actor} run={run_id} did not succeed, trying next")
            continue
        results = await get_apify_results(session, run_id)

        if results:
            normalized = SCRAPERS[actor]["normalize"](results[0])
            if normalized and normalized.get("first_name"):
                return normalized

    return None


def company_match(profile, company_name):
    """
    Returns True if the profile's current company matches the target company name.
    Checks current_company field first, then falls back to the most recent experience.
    Does a loose substring check in both directions to handle "Snowflake Inc" vs "Snowflake".
    """
    target = company_name.lower().strip()

    def _matches(name):
        n = (name or "").lower().strip()
        return bool(n and (target in n or n in target))

    # Check current_company field
    if _matches(profile.get("current_company")):
        return True

    # Fall back: check experiences — look for any role with no end_date (still there)
    # or the most recent experience if end dates are missing entirely
    experiences = profile.get("experiences") or []
    for exp in experiences:
        company = exp.get("company") or exp.get("company_name") or ""
        end_date = exp.get("end_date") or exp.get("ends_at") or ""
        # No end date = current role
        if not end_date and _matches(company):
            return True

    # Last resort: check if the first (most recent) experience matches,
    # even if end_date is present — some scrapers set end_date to "Present"
    if experiences:
        first_exp = experiences[0]
        company = first_exp.get("company") or first_exp.get("company_name") or ""
        end_date = str(first_exp.get("end_date") or first_exp.get("ends_at") or "").lower()
        if _matches(company) and ("present" in end_date or not end_date):
            return True

    return False


# ─────────────────────────────────────────────
# SECTION 5: MESSAGE GENERATION (GROQ)
# ─────────────────────────────────────────────

GROQ_URL   = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "qwen/qwen3.8-27b"  # primary model

# NVIDIA NIM (disabled — API key issues; keeping constants for future use)
NIM_URL   = "https://integrate.api.nvidia.com/v1/chat/completions"
NIM_MODEL = "nvidia/llama-3.1-nemotron-70b-instruct"

SYSTEM_PROMPT = """You write short LinkedIn connection request messages for Amber Sahu, a recent BU MS in Management graduate targeting sales roles at tech companies.

TEMPLATE:
"Hey [First name], [opener]. I'm a recent BU MS grad targeting sales roles at [company] and would love 15 min to hear your take. Open to a quick chat?"

IMPORTANT: Replace [First name] with the actual first name provided in the input. Never output the literal text "[First name]".

HOOK SELECTION — use this priority order:
1. Specific sales achievements, deal sizes, revenue closed, or quota performance.
2. Career progression: BDR/SDR → AE → Enterprise AE, promotions, or rapid advancement.
3. Experience building a sales function, entering a new market, or moving upmarket.
4. A meaningful connection to the target company, including previous experience there or a BU alumni connection.
5. A distinctive customer segment, sales motion, or industry change.
6. General work history only when nothing more specific is available.
Override this order when another verified detail is clearly more relevant or distinctive.

HOOK WRITING RULES:
- Describe what the person actually did before interpreting it. Lead with the fact.
  Prefer: "Saw you went from SDR to Enterprise AE at [company]..."
  Prefer: "Closing $14M+ in public-sector revenue..."
  Prefer: "Building the outbound motion from scratch at [company]..."
  Avoid: "Your career progression is a path I want to understand."
  Avoid: "That transition is a specific skill set I'm trying to learn."
- Never write: "I'd love to understand your transition", "that's a path I want to map out", "is a transition I want to learn about", or any variation. The fixed closing already handles the ask.
- Build a natural bridge to the ask. The hook should give a clear reason why Amber wants to speak with this specific person. A direct question works when natural; a concise observation is enough otherwise.
- One main idea. Choose one detail. Mention two facts only when they form a genuinely meaningful connection. Do not list multiple employers, titles, or technologies in one hook.
- Write like a person, not a profile analyst. Everyday professional language. No abstract phrases, no elaborate career summaries, no forced enthusiasm.
- Vary sentence structures. Do not repeatedly begin with "Moving from," "Building," or "Closing."
- Use only facts supported by the supplied profile. Do not infer deal sizes, motivations, or achievements not mentioned.

CHARACTER BUDGET:
- Opener target: 90–125 characters. Allow longer when necessary, but never pad to hit a number.
- Total message must stay within 300 characters (LinkedIn's limit).

BANNED WORDS AND PHRASES:
- Compliments: impressive, speaks volumes, bold pivot, is rare, I admire, remarkable
- Filler openers: I came across your profile, I'd love to connect, I noticed
- AI vocabulary: delve, leverage, seamless, robust, synergy, spearhead, dynamic
- Em dashes (— or --)
- Abstract meta-commentary: "is a transition I want to understand", "is a path I'm trying to map out", "is a skill set I want to learn"

Good examples (fact-first, natural bridge):
- "Saw you went from BDR to AE at Salesforce in under two years — curious how you approached that ramp." (99 chars)
- "Closing $14M in public-sector deals at Weka is a motion I haven't seen many people do." (87 chars)
- "Three years at HubSpot then a jump to a Series B — would love to hear what made you pull the trigger." (101 chars)
- "You've been on the sales side at Weka since early days — want to hear what that growth looked like from the inside." (114 chars)

Bad examples:
- "Your career progression is a path I want to understand better." (✗ — interpretation, not description)
- "Storage background at Weka is useful for tech sales." (✗ — goes nowhere)
- "Moving from HubSpot to a startup is a transition I'd love to learn about." (✗ — banned meta-commentary)

For company-angle openers (no profile data): write 1–2 sentences about the company's sales motion, market timing, or customer target. Same rules apply — no puffery, fact-first.

FINAL CHECK before returning:
- Does the hook contain a specific, verifiable detail?
- Does it sound natural when read aloud?
- Is it under 300 characters total?
- Does it avoid all banned phrases?

End with the exact phrase: "Open to a quick chat?"
Middle section must say exactly: "I'm a recent BU MS grad targeting sales roles at [company] and would love 15 min to hear your take."
COMPANY NAME: Use the company name exactly as provided — same capitalization, same spelling. Do not alter it.
OUTPUT: Return ONLY the final message text. No explanation, no quotes around it, no label."""


async def generate_message(session, profile, company_name):
    """
    Calls Groq once to write a LinkedIn message.
    profile=None → company-angle opener (no personalization).
    Returns the message string, trimmed to 300 chars if needed.
    """
    if profile:
        first_name = profile.get("first_name", "there")

        exp_text = ""
        for i, exp in enumerate(profile.get("experiences", [])):
            title = exp.get("title","")
            comp  = exp.get("company","")
            desc  = (exp.get("description","") or "").strip()
            if title or comp:
                exp_text += f"\n  Role {i+1}: {title} at {comp}"
                if desc and len(desc) > 10:
                    exp_text += f"\n    Description: {desc[:300]}"

        edu_text = ""
        for edu in profile.get("education", []):
            school = edu.get("school","")
            degree = edu.get("degree","")
            if school:
                edu_text += f"\n  {degree} at {school}" if degree else f"\n  {school}"

        user_content = (
            f"Write a personalized LinkedIn connection message for this person.\n\n"
            f"Name: {first_name}\n"
            f"Company: {company_name}\n"
            f"Headline: {profile.get('headline','')}\n"
            f"Summary: {(profile.get('summary','') or '')[:400] or 'none'}\n"
            f"Experience:{exp_text or ' none provided'}\n"
            f"Education:{edu_text or ' none provided'}"
        )
    else:
        first_name   = "there"
        user_content = (
            f"Write a LinkedIn connection message with a company-angle opener for someone at {company_name}.\n"
            f"You have no profile data. Use a sharp observation about {company_name}'s sales motion, "
            f"market timing, customer target, growth/funding, or differentiation as the opener.\n"
            f'Address them as "Hey [First name]" — use that placeholder literally.'
        )

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model":       GROQ_MODEL,
        "messages":    [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_content},
        ],
        "temperature": 0.7,
        "max_tokens":  300,
    }

    print(f"[Groq] Calling model={GROQ_MODEL} key_set={bool(GROQ_API_KEY)}")
    try:
        async with session.post(GROQ_URL, headers=headers, json=payload,
                                timeout=aiohttp.ClientTimeout(total=30)) as resp:
            raw = await resp.text()
            print(f"[Groq] status={resp.status} body_preview={raw[:200]}")
            if resp.status == 200:
                data    = json.loads(raw)
                msg_obj = data["choices"][0]["message"]
                message = (msg_obj.get("content") or "").strip()
                # If the LLM dropped the greeting, prepend it
                if not message.lower().startswith("hey"):
                    message = f"Hey {first_name}, {message}"
                if len(message) > 300:
                    message = message[:297] + "..."
                print(f"[Groq] OK len={len(message)}")
                return message
            else:
                print(f"[Groq] Error {resp.status}: {raw}")
    except Exception as e:
        import traceback
        print(f"[Groq] Exception: {e}\n{traceback.format_exc()}")

    return f"Hey {first_name}, [message generation failed — retry]"


PRIORITY_SYSTEM = """You classify LinkedIn profiles into sales outreach priority tiers.
Output exactly one word: high, medium, or low.

HIGH: Account Executives (AE), Sales Development Reps (SDR/BDR), Sales Managers, VP/Director of Sales, Revenue/GTM roles that own quota or pipeline, Enterprise Sales, Regional Sales Manager, Inside Sales.
MEDIUM: Customer Success Managers (CSM), Sales Ops, Partnerships, Business Development (non-SDR), Enablement.
LOW: Technical Account Managers (TAM), Solutions Engineers, Sales Engineers, Engineering, Product, Marketing, Finance, HR, Legal, Data Science, operations roles with no direct sales involvement, C-suite at small companies where sales title is unclear.

Output only: high, medium, or low"""


async def classify_priority(session, title: str, headline: str) -> str:
    """Classify a contact's sales priority using Groq. Returns 'high', 'medium', or 'low'."""
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model":    GROQ_MODEL,
        "messages": [
            {"role": "system", "content": PRIORITY_SYSTEM},
            {"role": "user",   "content": f"Title: {title}\nHeadline: {headline}"},
        ],
        "temperature": 0,
        "max_tokens":  5,
    }
    try:
        async with session.post(GROQ_URL, headers=headers, json=payload,
                                timeout=aiohttp.ClientTimeout(total=15)) as resp:
            raw = await resp.text()
            if resp.status == 200:
                data = json.loads(raw)
                msg_obj = data["choices"][0]["message"]
                result = (msg_obj.get("content") or "medium").strip().lower()
                if result in ("high", "medium", "low"):
                    return result
            else:
                print(f"[Priority] Groq error {resp.status}: {raw[:200]}")
    except Exception as e:
        print(f"[Priority] classify exception: {e}")
    return "medium"


# ─────────────────────────────────────────────
# SECTION 5b: SUPABASE LOGGER
# ─────────────────────────────────────────────

async def create_run_in_supabase(company: str) -> int | None:
    """
    Creates a run record immediately at pipeline start.
    Returns the new run id, or None on failure.
    """
    if not SUPABASE_URL or not SUPABASE_API_KEY:
        print("[Supabase] env vars not set, skipping run log")
        return None

    url = f"{SUPABASE_URL}/rest/v1/runs"
    headers = {
        "apikey":        SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=representation",
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url, headers=headers,
                              json={"company": company, "total": 0, "personalized": 0,
                                    "not_at_company": 0, "not_found": 0, "avg_char_count": 0},
                              timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    run_id = data[0]["id"] if data else None
                    print(f"[Supabase] run created id={run_id} company={company}")
                    return run_id
                else:
                    body = await resp.text()
                    print(f"[Supabase] run create failed status={resp.status} body={body[:300]}")
    except Exception as e:
        print(f"[Supabase] run create exception: {e}")
    return None


async def update_run_in_supabase(run_id: int, totals: dict):
    """Updates the run record with final totals once the pipeline finishes."""
    if not run_id or not SUPABASE_URL or not SUPABASE_API_KEY:
        return
    url = f"{SUPABASE_URL}/rest/v1/runs?id=eq.{run_id}"
    headers = {
        "apikey":        SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.patch(url, headers=headers, json=totals,
                               timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status in (200, 204):
                    print(f"[Supabase] run {run_id} totals updated")
                else:
                    body = await resp.text()
                    print(f"[Supabase] run update failed status={resp.status} body={body[:300]}")
    except Exception as e:
        print(f"[Supabase] run update exception: {e}")


async def log_contacts_to_supabase(run_id, company: str, results: list):
    """
    Inserts one row per profile result into the Supabase 'contacts' table.
    run_id: the id returned from log_run_to_supabase (may be None — contacts
            are still logged but won't have a run foreign key).
    """
    if not SUPABASE_URL or not SUPABASE_API_KEY:
        print("[Supabase] env vars not set, skipping contacts log")
        return

    url = f"{SUPABASE_URL}/rest/v1/contacts"
    headers = {
        "apikey":        SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

    rows = []
    for r in results:
        msg = r.get("message", "")
        # Determine initial status: not_at_company, not_found, or sent
        if "No longer at" in msg:
            initial_status = "no_response"
        elif r.get("personalized") == "No" and "No longer at" not in msg:
            initial_status = "sent"
        else:
            initial_status = "sent"

        rows.append({
            "run_id":   run_id,          # FK to runs table (nullable)
            "company":  company,
            "name":     r.get("name", ""),
            "url":      r.get("url", ""),
            "message":  msg,
            "priority": r.get("priority", "medium"),
        })

    if not rows:
        return

    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url, headers=headers, json=rows,
                              timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status in (200, 201):
                    print(f"[Supabase] {len(rows)} contacts logged for run_id={run_id}")
                else:
                    body = await resp.text()
                    print(f"[Supabase] contacts log failed status={resp.status} body={body[:300]}")
    except Exception as e:
        print(f"[Supabase] contacts log exception: {e}")


# ─────────────────────────────────────────────
# SECTION 6: SSE STREAMING ENDPOINT
# ─────────────────────────────────────────────
# What SSE means in plain English:
#   Normal HTTP: browser asks → server waits → server replies → connection closes.
#   SSE: browser asks → server keeps connection open → server sends data in chunks
#        as it becomes available → connection closes when done.
#
# Each chunk we send is a JSON object for one profile result.
# The frontend receives it, parses it, and adds a row to the table immediately.
# This means: profile 1 appears while profiles 2–10 are still being scraped.
#
# SSE format (required by the browser's EventSource API):
#   data: {"key": "value"}\n\n
# Each message must end with two newlines.

@app.post("/generate")
async def generate(request: Request):
    body    = await request.json()
    raw_urls = body.get("urls", [])
    company  = body.get("company", "").strip()

    if not raw_urls or not company:
        return {"error": "Provide at least one URL and a company name."}

    # Deduplicate while preserving order
    seen, urls = set(), []
    for u in raw_urls:
        u = u.strip()
        if u and u not in seen:
            seen.add(u)
            urls.append(u)

    # StreamingResponse runs the generator and sends each yielded string to the browser
    return StreamingResponse(
        _stream_pipeline(urls, company),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # tells Render/nginx not to buffer the stream
        },
    )


async def _stream_pipeline(urls, company):
    """
    The full pipeline as an async generator.
    Yields SSE-formatted JSON strings one profile at a time.

    yield format:  data: <json>\n\n
    Special events:
      {"event": "status",   "message": "..."}  — progress updates shown to the user
      {"event": "result",   ...profile data...} — one finished row
      {"event": "done",     "total": N}         — signals the stream is complete
      {"event": "error",    "message": "..."}   — fatal error
    """

    def sse(obj):
        """Formats a dict as an SSE data line."""
        return f"data: {json.dumps(obj)}\n\n"

    BATCH_SIZE   = 4
    batches      = [urls[i:i+BATCH_SIZE] for i in range(0, len(urls), BATCH_SIZE)]
    sr_no           = 1
    _run_results    = []   # collect every result row for Supabase logging
    _logged_results = []   # track which results have already been saved to Supabase

    yield sse({"event": "status", "message": f"Starting pipeline for {len(urls)} profiles across {len(batches)} batch(es)..."})

    # Create the run record immediately so contacts can be saved after each batch
    run_id = await create_run_in_supabase(company)

    async with aiohttp.ClientSession() as session:

        for batch_index, batch in enumerate(batches):

            yield sse({"event": "status", "message": f"Batch {batch_index+1}/{len(batches)}: scraping {len(batch)} profiles with Tier 1..."})

            # ── Tier 1: fire scrapers in parallel for all profiles in batch ──
            tier1_tasks  = [scrape_profile_tier1(session, url) for url in batch]
            tier1_results = await asyncio.gather(*tier1_tasks)

            batch_profiles = {}  # url → profile or None
            for url, profile in zip(batch, tier1_results):
                batch_profiles[url] = profile  # None if missed

            # ── Tier 2: for any profile Tier 1 missed ──
            missed = [u for u, p in batch_profiles.items() if p is None]
            if missed:
                yield sse({"event": "status", "message": f"Tier 1 missed {len(missed)} profile(s), trying Tier 2..."})
                for url in missed:
                    profile = await scrape_profile_tier2(session, url)
                    batch_profiles[url] = profile  # stays None → company-angle opener

            # ── Generate messages for this batch ──
            for url in batch:
                profile = batch_profiles.get(url)

                name_label = url.rstrip("/").split("/")[-1]  # slug as placeholder

                # ── Extract title/headline for priority classification ──
                if profile:
                    name         = f"{profile.get('first_name','')} {profile.get('last_name','')}".strip() or name_label
                    personalized = "Yes"
                    title        = ""
                    headline     = profile.get("headline", "")
                    if not title and profile.get("experiences"):
                        title = profile["experiences"][0].get("title", "")
                else:
                    name         = name_label
                    personalized = "No"
                    title        = ""
                    headline     = ""

                # ── Classify priority BEFORE generating message ──
                priority = await classify_priority(session, title, headline)

                # ── Skip low-priority profiles entirely ──
                if priority == "low":
                    yield sse({"event": "status", "message": f"Skipping {name} (low priority)"})
                    sr_no += 1
                    continue

                yield sse({"event": "status", "message": f"Writing message for {name}..."})
                message = await generate_message(session, profile, company)

                row = {
                    "event":        "result",
                    "sr":           sr_no,
                    "name":         name,
                    "personalized": personalized,
                    "url":          url,          # always the original URL, never reconstructed
                    "message":      message,
                    "char_count":   len(message),
                    "priority":     priority,
                }
                _run_results.append(row)
                yield sse(row)
                sr_no += 1

            # ── Save this batch's contacts to Supabase immediately ──
            batch_results = [r for r in _run_results if r not in _logged_results]
            if batch_results:
                await log_contacts_to_supabase(run_id, company, batch_results)
                _logged_results.extend(batch_results)

            # ── Wait between batches (skip after last) ──
            if batch_index < len(batches) - 1:
                yield sse({"event": "status", "message": "Waiting 45 seconds before next batch..."})
                await asyncio.sleep(45)

    total        = sr_no - 1
    personalized = sum(1 for r in _run_results if r.get("personalized") == "Yes")
    not_at_co    = sum(1 for r in _run_results if r.get("char_count") == 0 and "No longer at" in r.get("message",""))
    not_found    = sum(1 for r in _run_results if r.get("personalized") == "No" and "No longer at" not in r.get("message",""))
    char_counts  = [r["char_count"] for r in _run_results if r.get("char_count", 0) > 0]
    avg_chars    = int(sum(char_counts) / len(char_counts)) if char_counts else 0

    # Update the run record with final totals
    await update_run_in_supabase(run_id, {
        "total":          total,
        "personalized":   personalized,
        "not_at_company": not_at_co,
        "not_found":      not_found,
        "avg_char_count": avg_chars,
        "messages":       _run_results,
    })

    yield sse({"event": "done", "total": total})


# ─────────────────────────────────────────────
# SECTION 7: DASHBOARD & CRM ENDPOINTS
# ─────────────────────────────────────────────

def _sb_headers():
    return {
        "apikey":        SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type":  "application/json",
    }


@app.get("/history")
async def history(limit: int = 50):
    """Returns the last N runs from Supabase as JSON."""
    if not SUPABASE_URL or not SUPABASE_API_KEY:
        return {"error": "Supabase not configured"}
    url = f"{SUPABASE_URL}/rest/v1/runs?order=created_at.desc&limit={limit}"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_sb_headers(),
                             timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    body = await resp.text()
                    return {"error": f"Supabase {resp.status}", "detail": body[:300]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/contacts")
async def get_contacts(company: str = None, limit: int = 200):
    """Returns contacts from Supabase, optionally filtered by company."""
    if not SUPABASE_URL or not SUPABASE_API_KEY:
        return {"error": "Supabase not configured"}

    filters = ["order=created_at.desc", f"limit={limit}", "deleted=eq.false"]
    if company:
        filters.append(f"company=eq.{company}")

    url = f"{SUPABASE_URL}/rest/v1/contacts?" + "&".join(filters)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_sb_headers(),
                             timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    body = await resp.text()
                    return {"error": f"Supabase {resp.status}", "detail": body[:300]}
    except Exception as e:
        return {"error": str(e)}


@app.patch("/contacts/{contact_id}")
async def update_contact(contact_id: int, request: Request):
    """
    Updates a contact's CRM fields.
    Accepted body fields: accepted, replied, warm_intro (booleans), notes, follow_up_date
    """
    if not SUPABASE_URL or not SUPABASE_API_KEY:
        return {"error": "Supabase not configured"}

    body = await request.json()
    allowed = {"accepted", "replied", "warm_intro", "notes", "follow_up_date", "priority", "deleted"}
    patch   = {k: v for k, v in body.items() if k in allowed}
    if not patch:
        return {"error": "No valid fields to update"}

    patch["updated_at"] = "now()"

    url = f"{SUPABASE_URL}/rest/v1/contacts?id=eq.{contact_id}"
    headers = {**_sb_headers(), "Prefer": "return=representation"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.patch(url, headers=headers, json=patch,
                               timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status in (200, 204):
                    data = await resp.json() if resp.status == 200 else []
                    return data[0] if data else {"ok": True}
                else:
                    body_text = await resp.text()
                    return {"error": f"Supabase {resp.status}", "detail": body_text[:300]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Analytics dashboard with charts."""
    with open("dashboard.html", "r") as f:
        return HTMLResponse(content=f.read())


@app.get("/crm", response_class=HTMLResponse)
async def crm():
    """CRM contact tracker (by company / by contact views)."""
    with open("crm.html", "r") as f:
        return HTMLResponse(content=f.read())


# ─────────────────────────────────────────────
# SECTION 8: START THE SERVER
# ─────────────────────────────────────────────
# uvicorn is the server that runs FastAPI (like gunicorn for Flask).
# Render sets PORT automatically; defaults to 8000 locally.

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)

