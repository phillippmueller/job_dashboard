# Job Dashboard

A Streamlit dashboard that scrapes entry-level and internship job postings from multiple sources and displays them by category for a given location.

---

## What it does

Scrapes **Indeed**, **LinkedIn**, **jobs.ch**, and **jobup.ch** for entry-level / internship roles across two categories:

- **Insurance** — underwriting, actuarial, claims, reinsurance, broking
- **Private Markets** — private equity, venture capital, private credit, infrastructure

Results are stored in a local SQLite database. The dashboard filters by location, category, source, and keyword — and keeps a scrape history in an Analytics tab.

---

## Setup

**Requirements:** Python 3.11+, conda/mamba recommended.

```bash
mamba create -n job_dashboard python=3.11
mamba install -n job_dashboard -c conda-forge requests beautifulsoup4 pandas lxml
mamba run -n job_dashboard pip install streamlit python-jobspy
```

Or with pip only:

```bash
pip install -r requirements.txt
```

**Run:**

```bash
streamlit run app.py
```

Dashboard opens at `http://localhost:8501`.

---

## How it works

1. **Scrape** — `run_all_categories()` builds a single OR query per category using `_build_search_term()` and fires one request per scraper per category
2. **Filter** — each result passes `_is_entry_level()` and `_is_target_domain()` to reduce false positives
3. **Store** — `upsert_jobs()` writes new results to SQLite, tagged with `category` and `search_location`; `log_scrape_run()` records metadata per scrape
4. **Display** — `get_jobs()` queries by `category` and `search_location` so the tabs always reflect the currently selected location

---

## Key files

| File | Purpose |
|---|---|
| `app.py` | Streamlit UI — sidebar, 3 tabs (Insurance, Private Markets, Analytics), job card renderer |
| `scraper.py` | All scraping logic — jobspy, jobs.ch, jobup.ch, keyword lists, filters |
| `db.py` | SQLite interface — init, upsert, query, hide, scrape run logging |
| `menubar_app.py` | macOS menu bar wrapper (rumps) — launches server, triggers scrapes |
| `flowchart.drawio` | Architecture flowchart — open with draw.io |

---

## macOS menu bar app

`menubar_app.py` runs as a native macOS menu bar icon (via `rumps`). It auto-starts the Streamlit server on launch and exposes **Open Dashboard**, **Scrape Now**, and **Start / Stop Server** actions. Double-click `Launch Dashboard.command` to start it.
