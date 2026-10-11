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
SUPABASE_URL     = os.getenv("SUPABASE_URL")       # e.g. https://xxxx.supabase.co
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")   # anon public key

app = FastAPI()


# ─────────────────────────────────────────────
# SECTION 1: SERVE THE FRONTEND
# ─────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
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
    Does a loose substring check in both directions to handle "Snowflake Inc" vs "Snowflake".
    """
    current = (profile.get("current_company") or "").lower().strip()
    target  = company_name.lower().strip()
    return target in current or current in target


# ─────────────────────────────────────────────
# SECTION 5: MESSAGE GENERATION (GROQ)
# ─────────────────────────────────────────────

GROQ_URL   = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "qwen/qwen3.8-27b"

SYSTEM_PROMPT = """You write short LinkedIn connection request messages for Amber Sahu, a recent BU MS in Management graduate targeting SDR and sales roles at tech companies.

TEMPLATE:
"Hey [First name], [specific observation]. I'm a recent BU MS grad actively targeting SDR and sales roles at [company] and would love 15 minutes to hear your perspective on the team. Open to a quick chat?"

RULES — follow every one of these:
- Total message must be 300 characters or fewer (count carefully)
- Opener (the [specific observation] part) must be 85 characters or fewer
- End with the exact phrase: "Open to a quick chat?"
- The middle section must say: "I'm a recent BU MS grad actively targeting SDR and sales roles at [company] and would love 15 minutes to hear your perspective on the team."
- No em dashes (— or --)
- No filler: never start with "I came across your profile", "I'd love to connect", "Hope this finds you well"
- No AI vocabulary: never use delve, leverage, seamless, robust, synergy, spearhead, dynamic
- Natural sentence rhythm — write like a person, not a press release

OPENER PRIORITY (for personalized messages):
1. Experience descriptions — if any role has specific achievements, deal types, territory built, team led, use that
2. Career arc — interesting pivot, fast BDR→AE progression, long tenure, non-linear path
3. Education — shared school (BU is an instant connection), unusual degree for someone in sales
4. Current role/headline — fallback only if nothing above is distinctive
Ignore generic boilerplate in description fields (company copy-paste, vague mission statements).

For company-angle openers (no profile data): write one sharp observation about the company's sales motion, market timing, customer target, growth/funding, or differentiation. Do not make it generic.

OUTPUT: Return ONLY the final message text. No explanation, no quotes around it, no label like "Here is the message:", just the raw message itself."""


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
                message = (msg_obj.get("content") or msg_obj.get("reasoning") or "").strip()
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


# ─────────────────────────────────────────────
# SECTION 5b: SUPABASE LOGGER
# ─────────────────────────────────────────────

async def log_run_to_supabase(run_data: dict):
    """
    Inserts one run record into the Supabase 'runs' table.
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
            async with s.post(url, headers=headers, json=run_data,
                              timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    run_id = data[0]["id"] if data else None
                    print(f"[Supabase] run logged OK id={run_id} company={run_data.get('company')}")
                    return run_id
                else:
                    body = await resp.text()
                    print(f"[Supabase] run log failed status={resp.status} body={body[:300]}")
    except Exception as e:
        print(f"[Supabase] run log exception: {e}")
    return None


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
            "run_id":  run_id,          # FK to runs table (nullable)
            "company": company,
            "name":    r.get("name", ""),
            "url":     r.get("url", ""),
            "message": msg,
            "status":  initial_status,
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
    sr_no        = 1
    _run_results = []   # collect every result row for Supabase logging

    yield sse({"event": "status", "message": f"Starting pipeline for {len(urls)} profiles across {len(batches)} batch(es)..."})

    async with aiohttp.ClientSession() as session:

        for batch_index, batch in enumerate(batches):

            yield sse({"event": "status", "message": f"Batch {batch_index+1}/{len(batches)}: scraping {len(batch)} profiles with Tier 1..."})

            # ── Tier 1: fire scrapers in parallel for all profiles in batch ──
            tier1_tasks  = [scrape_profile_tier1(session, url) for url in batch]
            tier1_results = await asyncio.gather(*tier1_tasks)

            batch_profiles = {}  # url → profile or None
            for url, profile in zip(batch, tier1_results):
                if profile and company_match(profile, company):
                    batch_profiles[url] = profile
                elif profile:
                    # Scraped but company doesn't match — mark so we can surface it
                    batch_profiles[url] = "NOT_AT_COMPANY"
                else:
                    batch_profiles[url] = None  # Tier 1 missed

            # ── Tier 2: for any profile Tier 1 missed ──
            missed = [u for u, p in batch_profiles.items() if p is None]
            if missed:
                yield sse({"event": "status", "message": f"Tier 1 missed {len(missed)} profile(s), trying Tier 2..."})
                for url in missed:
                    profile = await scrape_profile_tier2(session, url)
                    if profile and company_match(profile, company):
                        batch_profiles[url] = profile
                    elif profile:
                        batch_profiles[url] = "NOT_AT_COMPANY"
                    # else: stays None → company-angle opener

            # ── Generate messages for this batch ──
            for url in batch:
                profile = batch_profiles.get(url)

                if profile == "NOT_AT_COMPANY":
                    name_label = url.rstrip("/").split("/")[-1]
                    row = {
                        "event":        "result",
                        "sr":           sr_no,
                        "name":         name_label,
                        "personalized": "No",
                        "url":          url,
                        "message":      f"⚠️ No longer at {company} (per LinkedIn)",
                        "char_count":   0,
                    }
                    _run_results.append(row)
                    yield sse(row)
                    sr_no += 1
                    continue

                name_label = url.rstrip("/").split("/")[-1]  # slug as placeholder
                yield sse({"event": "status", "message": f"Writing message for {name_label}..."})

                message = await generate_message(session, profile, company)

                if profile:
                    name         = f"{profile.get('first_name','')} {profile.get('last_name','')}".strip() or name_label
                    personalized = "Yes"
                else:
                    name         = name_label
                    personalized = "No"

                row = {
                    "event":        "result",
                    "sr":           sr_no,
                    "name":         name,
                    "personalized": personalized,
                    "url":          url,          # always the original URL, never reconstructed
                    "message":      message,
                    "char_count":   len(message),
                }
                _run_results.append(row)
                yield sse(row)
                sr_no += 1

            # ── Wait between batches (skip after last) ──
            if batch_index < len(batches) - 1:
                yield sse({"event": "status", "message": "Waiting 90 seconds before next batch..."})
                await asyncio.sleep(90)

    total        = sr_no - 1
    personalized = sum(1 for r in _run_results if r.get("personalized") == "Yes")
    not_at_co    = sum(1 for r in _run_results if r.get("char_count") == 0 and "No longer at" in r.get("message",""))
    not_found    = sum(1 for r in _run_results if r.get("personalized") == "No" and "No longer at" not in r.get("message",""))
    char_counts  = [r["char_count"] for r in _run_results if r.get("char_count", 0) > 0]
    avg_chars    = int(sum(char_counts) / len(char_counts)) if char_counts else 0

    async def _log_all():
        run_id = await log_run_to_supabase({
            "company":        company,
            "total":          total,
            "personalized":   personalized,
            "not_at_company": not_at_co,
            "not_found":      not_found,
            "avg_char_count": avg_chars,
            "messages":       _run_results,
        })
        await log_contacts_to_supabase(run_id, company, _run_results)

    asyncio.create_task(_log_all())

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
async def get_contacts(company: str = None, status: str = None, limit: int = 200):
    """Returns contacts from Supabase, optionally filtered by company and/or status."""
    if not SUPABASE_URL or not SUPABASE_API_KEY:
        return {"error": "Supabase not configured"}

    filters = ["order=created_at.desc", f"limit={limit}"]
    if company:
        filters.append(f"company=eq.{company}")
    if status:
        filters.append(f"status=eq.{status}")

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
    Accepted body fields: status, notes, follow_up_date
    """
    if not SUPABASE_URL or not SUPABASE_API_KEY:
        return {"error": "Supabase not configured"}

    body = await request.json()
    allowed = {"status", "notes", "follow_up_date"}
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
    with open("dashboard.html", "r") as f:
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

