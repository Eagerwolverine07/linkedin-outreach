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
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

# Load API keys from the .env file
load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

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
    import aiohttp as _aiohttp
    result = {"groq_key_set": bool(GROQ_API_KEY), "groq_model": GROQ_MODEL, "apify_key_set": bool(APIFY_TOKEN)}
    try:
        async with _aiohttp.ClientSession() as s:
            async with s.post(
                GROQ_URL,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={"model": GROQ_MODEL, "messages": [{"role": "user", "content": "Say hi."}], "max_tokens": 10},
                timeout=_aiohttp.ClientTimeout(total=15)
            ) as r:
                body = await r.text()
                result["groq_status"] = r.status
                result["groq_response"] = body[:300]
    except Exception as e:
        result["groq_error"] = str(e)
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
    url = f"{APIFY_BASE}/acts/{actor_slug}/runs?token={APIFY_TOKEN}"
    try:
        async with session.post(url, json=input_payload,
                                timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status in (200, 201):
                data = await resp.json()
                return data.get("data", {}).get("id")
    except Exception as e:
        print(f"[run_apify_actor] {actor_slug} failed to start: {e}")
    return None


async def get_apify_results(session, run_id):
    """
    Fetches the dataset items for a completed Apify run.
    Call this after you've already waited for the scraper to finish.
    Returns a list of result dicts, or [] if nothing came back.
    """
    url = f"{APIFY_BASE}/actor-runs/{run_id}/dataset/items?token={APIFY_TOKEN}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception as e:
        print(f"[get_apify_results] run {run_id} fetch failed: {e}")
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
    exps = raw.get("positions", raw.get("experiences", []))[:3]
    current_co = raw.get("currentCompany", {})
    if isinstance(current_co, dict):
        current_co = current_co.get("name", "")
    return {
        "first_name":      raw.get("firstName", ""),
        "last_name":       raw.get("lastName", ""),
        "headline":        raw.get("headline", ""),
        "summary":         raw.get("about","") or raw.get("summary",""),
        "experiences":     [{"title": e.get("title",""), "company": e.get("companyName", e.get("company",{}).get("name","")), "description": e.get("description","")} for e in exps],
        "current_company": current_co or raw.get("companyName","") or (exps[0].get("companyName","") if exps else ""),
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
    pos  = raw.get("currentPosition", [{}])
    exps = pos[:3]
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
    Fires atomus and supreme_coder simultaneously for one URL.
    Returns normalized profile data from whichever returns data, or None.
    """
    tier1 = ["supreme_coder/linkedin-profile-scraper"]

    # Start both runs at the same time
    run_ids = {}
    for actor in tier1:
        run_id = await run_apify_actor(session, actor, SCRAPERS[actor]["input_fn"](url))
        if run_id:
            run_ids[actor] = run_id

    if not run_ids:
        return None

    # Wait 25 seconds for scrapers to finish, then fetch results
    await asyncio.sleep(25)

    for actor, run_id in run_ids.items():
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

        await asyncio.sleep(25)
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
GROQ_MODEL = "openai/gpt-oss-20b"

SYSTEM_PROMPT = """You write short LinkedIn connection request messages for Amber Sahu, a recent BU MS in Management graduate targeting sales roles at tech companies.

TEMPLATE:
"Hey [First name], [specific observation]. I'm a BU MS grad exploring sales and would love your perspective on making an impact at [company]. Open to a quick 15-min chat?"

RULES — follow every one of these:
- Total message must be 300 characters or fewer (count carefully)
- Opener (the [specific observation] part) must be 85 characters or fewer
- End with the exact question: "Open to a quick 15-min chat?"
- No em dashes (— or --)
- No filler: never start with "I came across your profile", "I'd love to connect", "Hope this finds you well"
- No AI vocabulary: never use delve, leverage, seamless, robust, synergy, spearhead, dynamic
- Natural sentence rhythm — write like a person, not a press release
- Use "sales" not "GTM", "sales roles" not "GTM roles"

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
        "max_tokens":  150,
    }

    key_set = bool(GROQ_API_KEY)
    print(f"[Groq] Calling model={GROQ_MODEL} key_set={key_set}")
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

    BATCH_SIZE = 4
    batches    = [urls[i:i+BATCH_SIZE] for i in range(0, len(urls), BATCH_SIZE)]
    sr_no      = 1

    yield sse({"event": "status", "message": f"Starting pipeline for {len(urls)} profiles across {len(batches)} batch(es)..."})

    async with aiohttp.ClientSession() as session:

        for batch_index, batch in enumerate(batches):

            yield sse({"event": "status", "message": f"Batch {batch_index+1}/{len(batches)}: scraping {len(batch)} profiles with Tier 1..."})

            # ── Tier 1: fire atomus + supreme_coder in parallel for all profiles in batch ──
            tier1_tasks  = [scrape_profile_tier1(session, url) for url in batch]
            tier1_results = await asyncio.gather(*tier1_tasks)

            batch_profiles = {}  # url → profile or None
            for url, profile in zip(batch, tier1_results):
                if profile and company_match(profile, company):
                    batch_profiles[url] = profile
                elif profile:
                    # scraper returned data but company doesn't match — filtered
                    yield sse({"event": "status", "message": f"Filtered (company mismatch): {url.split('/')[-1]}"})
                    batch_profiles[url] = "FILTERED"
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
                        yield sse({"event": "status", "message": f"Filtered after Tier 2 (company mismatch): {url.split('/')[-1]}"})
                        batch_profiles[url] = "FILTERED"
                    # else: stays None → company-angle opener

            # ── Generate messages for this batch ──
            for url in batch:
                profile = batch_profiles.get(url)
                if profile == "FILTERED":
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

                yield sse({
                    "event":        "result",
                    "sr":           sr_no,
                    "name":         name,
                    "personalized": personalized,
                    "url":          url,          # always the original URL, never reconstructed
                    "message":      message,
                    "char_count":   len(message),
                })
                sr_no += 1

            # ── Wait between batches (skip after last) ──
            if batch_index < len(batches) - 1:
                yield sse({"event": "status", "message": "Waiting 90 seconds before next batch..."})
                await asyncio.sleep(90)

    yield sse({"event": "done", "total": sr_no - 1})


# ─────────────────────────────────────────────
# SECTION 7: START THE SERVER
# ─────────────────────────────────────────────
# uvicorn is the server that runs FastAPI (like gunicorn for Flask).
# Render sets PORT automatically; defaults to 8000 locally.

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)

