# OpenRouteService API Key Setup Guide

This guide explains how to register for a free OpenRouteService (ORS) API key and configure it locally so the distance features work correctly.

---

## Step 1 — Register for a Free API Key

1. Go to **https://openrouteservice.org/dev/#/signup**
2. Fill in your name, email, and password, then click **Sign Up**
3. Verify your email address
4. Log in and go to the **Dashboard**
5. Under **Tokens**, click **Create Token**
6. Give it any name (e.g. `aida-project`) and click **Create**
7. Copy the token — this is your API key

> **Free tier limits:** 2,000 routing requests/day, 40 requests/minute.  
> This is more than enough for our use case.

---

## Step 2 — Create a `.env` File

In the **project root directory** (`aida-final-project-001/`), create a file named exactly `.env` (no filename before the dot).

**On Windows (Command Prompt):**
```bash
cd aida-final-project-001
echo ORS_API_KEY=paste_your_key_here > .env
```

**On Mac/Linux:**
```bash
cd aida-final-project-001
echo ORS_API_KEY=paste_your_key_here > .env
```

Verify it was created correctly:
```bash
# Windows
type .env

# Mac/Linux
cat .env
```

Expected output:
```
ORS_API_KEY=your_key_here
```

> ⚠️ **Important:** The format must be exactly `KEY=value` with no spaces around the `=` sign.

---

## Step 3 — Install `python-dotenv`

```bash
pip install python-dotenv
```

---

## Step 4 — Verify It Works

Run a quick test from the `datascience/` folder:

```bash
cd aida-final-project-001/datascience
python inference/predict.py
```

You should see output like:
```
[API] Fetching road distances for: RED DEER ...
[API] Edmonton: 148.5 km  |  Calgary: 131.2 km

─────────────────────────────────────────────
  Predicted Price       :    $24,500.00 CAD
  Distance from Edmonton:       148.5 km
  Distance from Calgary :       131.2 km
─────────────────────────────────────────────
```

---

## ⚠️ Security Rules

| Rule | Why |
|---|---|
| **Never commit `.env` to GitHub** | Anyone can see and use your API key |
| **Never paste your key in chat or email** | Treat it like a password |
| **Each team member uses their own key** | Free tier is per account |

The `.env` file is already listed in `.gitignore` — do not remove it from there.

---

## Troubleshooting

**`Prediction failed: ORS_API_KEY not set`**
- Check that the file is named `.env` and not `.env.txt`
- On Windows, run `dir .env` in the project root to confirm the filename
- Check there are no spaces around `=` in the file

**`[ORS ROUTING ERROR]`**
- Check your internet connection
- Verify your API key is correct by logging into the ORS dashboard
- You may have hit the rate limit (40 requests/minute) — wait a moment and retry