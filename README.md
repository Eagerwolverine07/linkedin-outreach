# LinkedIn Outreach Pipeline

A standalone web app that scrapes LinkedIn profiles via Apify and writes personalized connection messages using Groq (free LLM). No Claude credits used.

---

## Files in this project

| File | What it does |
|------|-------------|
| `app.py` | Python backend — handles scraping and message generation |
| `index.html` | The web page you see in your browser |
| `requirements.txt` | List of Python packages to install |
| `.env.example` | Template showing which API keys are needed |
| `.gitignore` | Tells Git not to upload your `.env` file |
| `Procfile` | Tells Render how to start the server |

---

## Step-by-step deployment on Render

### Step 1 — Get your API keys

You need two:

**Apify token** (you already have an account)
1. Go to [console.apify.com](https://console.apify.com)
2. Click your profile icon → **Settings** → **Integrations**
3. Copy the value under **API token**

**Groq API key** (free)
1. Go to [console.groq.com](https://console.groq.com)
2. Sign up (free, no credit card)
3. Click **API Keys** in the left sidebar → **Create API Key**
4. Copy the key (you only see it once)

---

### Step 2 — Put the code on GitHub

1. Go to [github.com](https://github.com) and sign in (or create a free account)
2. Click the **+** icon in the top right → **New repository**
3. Name it `linkedin-outreach` (or anything you like)
4. Make sure it's set to **Private** (keeps your code off the public internet)
5. Click **Create repository**
6. On the next page, look for the section that says **"...or upload an existing file"** and click **uploading an existing file**
7. Drag and drop all 7 files into the upload window:
   - `app.py`
   - `index.html`
   - `requirements.txt`
   - `.env.example`
   - `.gitignore`
   - `Procfile`
   - `README.md`
8. Click **Commit changes**

---

### Step 3 — Deploy on Render

1. Go to [render.com](https://render.com) and sign up (free, use your GitHub account)
2. Click **New +** → **Web Service**
3. Click **Connect account** next to GitHub and authorize Render
4. Find your `linkedin-outreach` repo in the list and click **Connect**
5. Fill in the settings:
   - **Name:** `linkedin-outreach` (or anything)
   - **Region:** US East (Ohio) — closest to you
   - **Branch:** `main`
   - **Runtime:** Python 3
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `gunicorn app:app`
   - **Instance Type:** Free
6. Scroll down to **Environment Variables** — this is where you add your API keys:
   - Click **Add Environment Variable**
   - Key: `APIFY_TOKEN` → Value: paste your Apify token
   - Click **Add Environment Variable** again
   - Key: `GROQ_API_KEY` → Value: paste your Groq key
7. Click **Create Web Service**

Render will now install the packages and start the server. This takes about 2–3 minutes the first time.

---

### Step 4 — Open the site

Once the deployment finishes, Render gives you a URL like:
```
https://linkedin-outreach-xxxx.onrender.com
```

Open that URL in your browser. You should see the pipeline form.

> **Note on Render's free tier:** The server "sleeps" after 15 minutes of no activity. The first request after it sleeps takes ~30 seconds to wake up — after that it's fast. This is normal and free.

---

## How to use the site

1. Type the **company name** (e.g. `Snowflake`)
2. Paste **LinkedIn profile URLs**, one per line
3. Click **Generate Messages**
4. Wait — scraping takes 1–3 minutes depending on how many profiles
5. The results table appears with Name, Personalized (Yes/No), a link to their profile, and the message
6. Click **Copy** next to any message to copy it to your clipboard

---

## How to update the code later

If you want to change something (e.g. update the message template, add a scraper, change the design):

1. Edit the file on your computer
2. Go to your GitHub repo
3. Click the file you changed → click the pencil icon (Edit) → paste the new content → **Commit changes**
4. Render automatically detects the change and redeploys (takes ~1 minute)

Or if you're comfortable with Git, just push from your terminal.

---

## Common issues

**"No valid LinkedIn profile URLs found"**
Make sure each URL starts with `https://www.linkedin.com/in/` — not `http://` or a company page URL.

**The table is empty / all profiles filtered**
The currentCompany filter dropped everyone because the company name in the profile didn't match what you typed. Try a shorter version of the name (e.g. `Snowflake` instead of `Snowflake Inc.`).

**Groq returns a malformed message**
Retry — Groq occasionally produces odd output on the first call. The message will have a `[message generation failed — retry]` placeholder if the API call failed entirely.

**automation-lab scraper returns wrong fields**
Check the raw Apify output for that actor and update the `normalize_automation_lab()` function in `app.py` with the correct field names. There's a `# TODO: verify` comment marking exactly where to look.
