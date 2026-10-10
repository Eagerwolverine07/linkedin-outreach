"""
LinkedIn Cold Outreach Pipeline — Backend
==========================================
What this file does in plain English:
  1. Serves the frontend (index.html) when you open the site in a browser
  2. Listens for a POST request to /generate containing LinkedIn URLs + company name
  3. Splits those URLs into batches of 3–5
  4. For each batch, runs the Tier 1 scrapers (atomus + supreme_coder) in parallel
  5. Any profile Tier 1 missed gets passed to Tier 2 (apimaestro → harvestapi → datadoping → sabania → automation-lab)
  6. Normalizes the raw scraper output into a clean dict: first_name, last_name, headline, summary, experiences
  7. Calls Groq (free LLM) once per profile to write a ≤300-char LinkedIn message
  8. Returns a JSON list of results back to the frontend
"""

import os
import time
import json
import math
import asyncio
import aiohttp
from flask import Flask, request, jsonify, send_from_directory
from dotenv import load_dotenv

# Load API keys from the .env file (APIFY_TOKEN and GROQ_API_KEY)
load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

app = Flask(__name__, static_folder=".", static_url_path="")


# ─────────────────────────────────────────────
# SECTION 1: SERVE THE FRONTEND
# ─────────────────────────────────────────────
# When someone opens the site in a browser, Flask sends them index.html.
# That's the only page the site has.

@app.route("/")
def index():
    return send_from_directory(".", "index.html")


# ─────────────────────────────────────────────
# SECTION 2: APIFY HELPERS
# ─────────────────────────────────────────────
# Apify works like this:
#   Step A — you "run" an actor (a scraper) by sending it a profile URL
#   Step B — Apify runs the scraper in the cloud (takes ~15–25 seconds)
#   Step C — you ask Apify for the results from that run
#
# run_apify_actor() handles Step A and returns a run ID.
# get_apify_results() handles Step C and returns the scraped data.

APIFY_BASE = "https://api.apify.com/v2"

async def run_apify_actor(session, actor_slug, input_payload):
    """
    Starts a single Apify actor run.
    Returns the run ID (a string like "abc123xyz") so we can fetch results later.
    Returns None if the request fails.
    """
    url = f"{APIFY_BASE}/acts/{actor_slug}/runs?token={APIFY_TOKEN}"
    try:
        async with session.post(url, json=input_payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status in (200, 201):
                data = await resp.json()
                return data.get("data", {}).get("id")
    except Exception as e:
        print(f"[run_apify_actor] {actor_slug} failed to start: {e}")
    return None


async def get_apify_results(session, run_id, wait_seconds=25):
    """
    Waits for a run to finish, then fetches the results.
    wait_seconds: how long to wait before asking for results.
    Returns a list of raw result dicts, or an empty list if nothing came back.
    """
    await asyncio.sleep(wait_seconds)
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
# Each Apify scraper returns data in a slightly different shape.
# These functions all take the raw dict from one scraper and return
# a clean, consistent dict with the same keys every time:
#   first_name, last_name, headline, summary, experiences (list), current_company

def normalize_atomus(raw):
    """
    atomus/linkedin-profile-scraper field mapping.
    Fields used: firstName, lastName, headline, summary, experiences (first 3), currentCompany
    """
    if not raw:
        return None
    exps = raw.get("experiences", [])[:3]
    return {
        "first_name":      raw.get("firstName", ""),
        "last_name":       raw.get("lastName", ""),
        "headline":        raw.get("headline", ""),
        "summary":         raw.get("summary", ""),
        "experiences":     [{"title": e.get("title",""), "company": e.get("companyName",""), "description": e.get("description","")} for e in exps],
        "current_company": raw.get("currentCompany", "") or (exps[0].get("companyName","") if exps else ""),
        "education":       [{"school": e.get("schoolName",""), "degree": e.get("degreeName","")} for e in raw.get("education", [])[:2]],
    }


def normalize_supreme_coder(raw):
    """
    supreme_coder/linkedin-profile-scraper field mapping.
    Similar shape to atomus.
    """
    if not raw:
        return None
    exps = raw.get("experiences", [])[:3]
    return {
        "first_name":      raw.get("firstName", ""),
        "last_name":       raw.get("lastName", ""),
        "headline":        raw.get("headline", ""),
        "summary":         raw.get("about", "") or raw.get("summary", ""),
        "experiences":     [{"title": e.get("title",""), "company": e.get("companyName",""), "description": e.get("description","")} for e in exps],
        "current_company": raw.get("currentCompany", "") or (exps[0].get("companyName","") if exps else ""),
        "education":       [{"school": e.get("schoolName",""), "degree": e.get("degreeName","")} for e in raw.get("education", [])[:2]],
    }


def normalize_apimaestro(raw):
    """
    apimaestro/linkedin-profile-detail field mapping.
    This scraper nests data differently — basic info is under basic_info{}.
    """
    if not raw:
        return None
    basic = raw.get("basic_info", {})
    exps  = raw.get("experience", [])[:3]
    return {
        "first_name":      basic.get("first_name", ""),
        "last_name":       basic.get("last_name", ""),
        "headline":        basic.get("headline", ""),
        "summary":         basic.get("about", ""),
        "experiences":     [{"title": e.get("title",""), "company": e.get("company",""), "description": e.get("description","")} for e in exps],
        "current_company": basic.get("current_company", "") or (exps[0].get("company","") if exps else ""),
        "education":       [{"school": e.get("school",""), "degree": e.get("degree","")} for e in raw.get("education", [])[:2]],
    }


def normalize_harvestapi(raw):
    """
    harvestapi/linkedin-profile-scraper field mapping.
    Uses currentPosition[] for the most recent role.
    """
    if not raw:
        return None
    pos  = raw.get("currentPosition", [{}])
    exps = pos[:3]
    return {
        "first_name":      raw.get("firstName", ""),
        "last_name":       raw.get("lastName", ""),
        "headline":        raw.get("headline", ""),
        "summary":         raw.get("about", ""),
        "experiences":     [{"title": e.get("position",""), "company": e.get("companyName",""), "description": e.get("description","")} for e in exps],
        "current_company": exps[0].get("companyName","") if exps else "",
        "education":       [{"school": e.get("schoolName",""), "degree": e.get("degreeName","")} for e in raw.get("education", [])[:2]],
    }


def normalize_datadoping(raw):
    """
    datadoping/linkedin-profile-scraper field mapping.
    Similar shape to atomus/supreme_coder — adjust if actual output differs.
    """
    if not raw:
        return None
    exps = raw.get("experiences", [])[:3]
    return {
        "first_name":      raw.get("firstName", ""),
        "last_name":       raw.get("lastName", ""),
        "headline":        raw.get("headline", ""),
        "summary":         raw.get("about", "") or raw.get("summary", ""),
        "experiences":     [{"title": e.get("title",""), "company": e.get("companyName",""), "description": e.get("description","")} for e in exps],
        "current_company": raw.get("currentCompany", "") or (exps[0].get("companyName","") if exps else ""),
        "education":       [{"school": e.get("schoolName",""), "degree": e.get("degreeName","")} for e in raw.get("education", [])[:2]],
    }


def normalize_sabania(raw):
    """
    sabania/linkedin-scraper field mapping.
    Best-guess based on common patterns — verify after first test run.
    """
    if not raw:
        return None
    exps = raw.get("experience", raw.get("experiences", []))[:3]
    return {
        "first_name":      raw.get("firstName", raw.get("first_name", "")),
        "last_name":       raw.get("lastName",  raw.get("last_name",  "")),
        "headline":        raw.get("headline", ""),
        "summary":         raw.get("summary", raw.get("about", "")),
        "experiences":     [{"title": e.get("title",""), "company": e.get("company", e.get("companyName","")), "description": e.get("description","")} for e in exps],
        "current_company": raw.get("currentCompany", exps[0].get("company", exps[0].get("companyName","")) if exps else ""),
        "education":       [{"school": e.get("school", e.get("schoolName","")), "degree": e.get("degree", e.get("degreeName",""))} for e in raw.get("education", [])[:2]],
    }


def normalize_automation_lab(raw):
    """
    automation-lab/linkedin-profile-scraper field mapping.
    # TODO: verify automation-lab fields after first test run.
    Best-guess based on common patterns — check the actual Apify output and update keys here.
    """
    if not raw:
        return None
    exps = raw.get("experience", raw.get("experiences", []))[:3]
    return {
        "first_name":      raw.get("firstName", raw.get("first_name", "")),
        "last_name":       raw.get("lastName",  raw.get("last_name",  "")),
        "headline":        raw.get("headline", raw.get("title", "")),
        "summary":         raw.get("summary", raw.get("about", "")),
        "experiences":     [{"title": e.get("title",""), "company": e.get("company", e.get("companyName","")), "description": e.get("description", e.get("summary",""))} for e in exps],
        "current_company": raw.get("currentCompany", exps[0].get("company","") if exps else ""),
        "education":       [{"school": e.get("school", e.get("schoolName","")), "degree": e.get("degree", e.get("degreeName",""))} for e in raw.get("education", [])[:2]],
    }


# Map each actor slug to its normalizer function and input format
# "input_fn" is a function that takes a LinkedIn URL and returns the dict Apify expects
SCRAPERS = {
    "atomus/linkedin-profile-scraper": {
        "normalize": normalize_atomus,
        "input_fn":  lambda url: {"profileUrls": [url]},
    },
    "supreme_coder/linkedin-profile-scraper": {
        "normalize": normalize_supreme_coder,
        "input_fn":  lambda url: {"profileUrl": url},
    },
    "apimaestro/linkedin-profile-detail": {
        "normalize": normalize_apimaestro,
        "input_fn":  lambda url: {"url": url},
        "daily_cap": 20,   # apimaestro hard limit; tracked in memory for this session
    },
    "harvestapi/linkedin-profile-scraper": {
        "normalize": normalize_harvestapi,
        "input_fn":  lambda url: {"profileUrl": url},
    },
    "datadoping/linkedin-profile-scraper": {
        "normalize": normalize_datadoping,
        "input_fn":  lambda url: {"profileUrl": url},
    },
    "sabania/linkedin-scraper": {
        "normalize": normalize_sabania,
        "input_fn":  lambda url: {"url": url},
    },
    "automation-lab/linkedin-profile-scraper": {
        "normalize": normalize_automation_lab,
        "input_fn":  lambda url: {"profileUrl": url},
        # TODO: confirm input key for automation-lab — may be "url" instead of "profileUrl"
    },
}

# Simple in-memory counter for apimaestro's daily cap (resets when the server restarts)
apimaestro_used_today = 0
APIMAESTRO_DAILY_CAP = 20


# ─────────────────────────────────────────────
# SECTION 4: SCRAPER WATERFALL
# ─────────────────────────────────────────────
# This is the heart of the pipeline.
# For each batch of profiles:
#   - Fire Tier 1 (atomus + supreme_coder) in parallel
#   - Wait 25 seconds
#   - Check which profiles returned data
#   - For any that missed, try Tier 2 scrapers one at a time
#   - If still no data, mark as "no profile" → will get a company-angle opener

async def scrape_profile_tier1(session, url):
    """
    Fires atomus and supreme_coder simultaneously for one URL.
    Returns normalized profile data from whichever returns first, or None.
    """
    tier1_actors = [
        "atomus/linkedin-profile-scraper",
        "supreme_coder/linkedin-profile-scraper",
    ]

    # Step A: start both runs at the same time
    run_ids = {}
    for actor in tier1_actors:
        cfg = SCRAPERS[actor]
        run_id = await run_apify_actor(session, actor, cfg["input_fn"](url))
        if run_id:
            run_ids[actor] = run_id
            print(f"  [Tier 1] Started {actor} → run {run_id}")

    if not run_ids:
        return None  # both failed to start

    # Step B: wait, then fetch results from both
    await asyncio.sleep(25)

    for actor, run_id in run_ids.items():
        results = await get_apify_results(session, run_id, wait_seconds=0)  # already waited above
        if results:
            normalized = SCRAPERS[actor]["normalize"](results[0])
            if normalized and normalized.get("first_name"):
                print(f"  [Tier 1] {actor} returned data for {url}")
                return normalized

    return None  # both returned empty


async def scrape_profile_tier2(session, url):
    """
    Tries Tier 2 scrapers one at a time for a URL that Tier 1 missed.
    Uses apimaestro first (most reliable), then rotates through the rest.
    Respects apimaestro's 20/day cap.
    Returns normalized profile data or None.
    """
    global apimaestro_used_today

    tier2_actors = []

    # apimaestro goes first if we haven't hit the cap
    if apimaestro_used_today < APIMAESTRO_DAILY_CAP:
        tier2_actors.append("apimaestro/linkedin-profile-detail")

    # Then the rest
    tier2_actors += [
        "harvestapi/linkedin-profile-scraper",
        "datadoping/linkedin-profile-scraper",
        "sabania/linkedin-scraper",
        "automation-lab/linkedin-profile-scraper",
    ]

    for actor in tier2_actors:
        cfg = SCRAPERS[actor]
        print(f"  [Tier 2] Trying {actor} for {url}")
        run_id = await run_apify_actor(session, actor, cfg["input_fn"](url))
        if not run_id:
            continue

        if actor == "apimaestro/linkedin-profile-detail":
            apimaestro_used_today += 1

        await asyncio.sleep(25)
        results = await get_apify_results(session, run_id, wait_seconds=0)

        if results:
            normalized = cfg["normalize"](results[0])
            if normalized and normalized.get("first_name"):
                print(f"  [Tier 2] {actor} returned data for {url}")
                return normalized

    return None  # all Tier 2 scrapers missed


async def scrape_batch(session, batch_urls, company_name):
    """
    Processes one batch of 3–5 URLs through the full waterfall.
    Returns a dict mapping each URL to its profile data (or None if all scrapers missed).
    """
    results = {}

    # Run Tier 1 for all URLs in the batch concurrently
    # (asyncio.gather runs them all at the same time, not one by one)
    tier1_tasks = [scrape_profile_tier1(session, url) for url in batch_urls]
    tier1_results = await asyncio.gather(*tier1_tasks)

    for url, profile in zip(batch_urls, tier1_results):
        if profile:
            # currentCompany filter: drop if they don't work at the target company
            current = (profile.get("current_company") or "").lower()
            if company_name.lower() not in current and current not in company_name.lower():
                print(f"  [Filter] {url} — company mismatch ({current}), dropping")
                results[url] = "FILTERED"
            else:
                results[url] = profile
        else:
            results[url] = None  # needs Tier 2

    # Tier 2: only for profiles Tier 1 missed
    missed_urls = [url for url, data in results.items() if data is None]

    for url in missed_urls:
        profile = await scrape_profile_tier2(session, url)
        if profile:
            current = (profile.get("current_company") or "").lower()
            if company_name.lower() not in current and current not in company_name.lower():
                print(f"  [Filter] {url} — company mismatch after Tier 2 ({current}), dropping")
                results[url] = "FILTERED"
            else:
                results[url] = profile
        else:
            results[url] = None  # will get company-angle opener

    return results


# ─────────────────────────────────────────────
# SECTION 5: MESSAGE GENERATION (GROQ)
# ─────────────────────────────────────────────
# Groq is a free LLM API (runs Llama 3.1 70B).
# We call it once per profile with the profile data and message rules.
# Each call is completely isolated — no context carries over between profiles.
# This is what eliminates the context accumulation problem from Claude.ai.

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.1-70b-versatile"

# This is the system prompt baked into every Groq call.
# Edit the rules here if you ever want to change the message style.
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

OUTPUT: Return ONLY the final message text. No explanation, no quotes around it, no "Here is the message:", just the message itself."""


async def generate_message(session, profile, company_name, linkedin_url):
    """
    Calls Groq once to write a LinkedIn message for one profile.
    If profile is None, writes a company-angle opener with no personalization.
    Returns the message string (trimmed to 300 chars if Groq goes over).
    """
    if profile:
        first_name = profile.get("first_name", "there")
        exp_text = ""
        for i, exp in enumerate(profile.get("experiences", [])):
            title   = exp.get("title", "")
            company = exp.get("company", "")
            desc    = exp.get("description", "").strip()
            if title or company:
                exp_text += f"\n  Role {i+1}: {title} at {company}"
                if desc and len(desc) > 10:  # ignore very short/empty descriptions
                    exp_text += f"\n    Description: {desc[:300]}"  # cap at 300 chars to save tokens

        edu_text = ""
        for edu in profile.get("education", []):
            school = edu.get("school","")
            degree = edu.get("degree","")
            if school:
                edu_text += f"\n  {degree} at {school}" if degree else f"\n  {school}"

        user_content = f"""Write a personalized LinkedIn connection message for this person.

Name: {first_name}
Company: {company_name}
Headline: {profile.get('headline', '')}
Summary: {profile.get('summary', '')[:400] if profile.get('summary') else 'none'}
Experience:{exp_text if exp_text else ' none provided'}
Education:{edu_text if edu_text else ' none provided'}"""

    else:
        # No profile data — company-angle opener
        first_name = "there"
        user_content = f"""Write a LinkedIn connection message with a company-angle opener for someone at {company_name}.
You have no profile data for this person. Use a sharp observation about {company_name}'s sales motion, market timing, customer target, growth/funding, or differentiation as the opener.
Address them as "Hey [First name]" — use that placeholder literally since we don't have their name."""

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model":       GROQ_MODEL,
        "messages":    [
            {"role": "system",  "content": SYSTEM_PROMPT},
            {"role": "user",    "content": user_content},
        ],
        "temperature": 0.7,
        "max_tokens":  150,  # messages are short; 150 tokens is plenty
    }

    try:
        async with session.post(GROQ_URL, headers=headers, json=payload,
                                timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 200:
                data = await resp.json()
                message = data["choices"][0]["message"]["content"].strip()
                # Hard trim to 300 characters if Groq exceeded the limit
                if len(message) > 300:
                    message = message[:297] + "..."
                return message
            else:
                error = await resp.text()
                print(f"[Groq] Error {resp.status}: {error}")
                return f"Hey {first_name}, [message generation failed — retry]"
    except Exception as e:
        print(f"[Groq] Exception: {e}")
        return f"Hey {first_name}, [message generation failed — retry]"


# ─────────────────────────────────────────────
# SECTION 6: MAIN ENDPOINT
# ─────────────────────────────────────────────
# This is the route the frontend calls when you click "Generate Messages".
# It receives a JSON body with:
#   - urls: list of LinkedIn profile URLs
#   - company: company name string
#
# It returns a JSON array of result objects, one per non-filtered profile:
#   { sr, name, personalized, url, message }

@app.route("/generate", methods=["POST"])
def generate():
    data = request.get_json()
    raw_urls    = data.get("urls", [])
    company     = data.get("company", "").strip()

    if not raw_urls or not company:
        return jsonify({"error": "Provide at least one URL and a company name."}), 400

    # Deduplicate URLs while preserving order
    seen = set()
    urls = []
    for u in raw_urls:
        u = u.strip()
        if u and u not in seen:
            seen.add(u)
            urls.append(u)

    # Run the async pipeline inside Flask's synchronous request handler
    # asyncio.run() spins up an event loop, does all the async work, then returns
    results = asyncio.run(_run_pipeline(urls, company))
    return jsonify(results)


async def _run_pipeline(urls, company):
    """
    The full pipeline, run asynchronously.
    Splits URLs into batches of 4, scrapes each batch, then generates messages.
    Returns the final list of result dicts.
    """
    BATCH_SIZE = 4

    # Split into batches: e.g. 10 URLs → [[url1..url4], [url5..url8], [url9,url10]]
    batches = [urls[i:i+BATCH_SIZE] for i in range(0, len(urls), BATCH_SIZE)]

    all_profiles = {}  # url → profile dict (or None)

    async with aiohttp.ClientSession() as session:
        for batch_index, batch in enumerate(batches):
            print(f"\n[Batch {batch_index+1}/{len(batches)}] Processing {len(batch)} profiles...")
            batch_results = await scrape_batch(session, batch, company)
            all_profiles.update(batch_results)

            # Wait 90 seconds between batches (as per pipeline instructions: 1–2 min gap)
            # Skip the wait after the last batch
            if batch_index < len(batches) - 1:
                print(f"[Batch gap] Waiting 90 seconds before next batch...")
                await asyncio.sleep(90)

        # Generate messages for all non-filtered profiles
        final_results = []
        sr_no = 1

        for url in urls:
            profile = all_profiles.get(url)

            if profile == "FILTERED":
                continue  # skip people who don't work at the target company

            message = await generate_message(session, profile, company, url)

            # Build the name string
            if profile:
                name = f"{profile.get('first_name','')} {profile.get('last_name','')}".strip()
                personalized = "Yes"
            else:
                name = url.rstrip("/").split("/")[-1]  # use slug as fallback name
                personalized = "No"

            final_results.append({
                "sr":           sr_no,
                "name":         name,
                "personalized": personalized,
                "url":          url,   # always the original URL — never reconstructed
                "message":      message,
                "char_count":   len(message),
            })
            sr_no += 1

    return final_results


# ─────────────────────────────────────────────
# SECTION 7: START THE SERVER
# ─────────────────────────────────────────────
# When you run "python app.py" in your terminal, this starts the server.
# PORT is set automatically by Render when deployed; defaults to 5000 locally.

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
