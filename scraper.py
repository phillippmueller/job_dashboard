"""
Scrapers for Zurich insurance jobs (entry-level / internship).

Sources:
  - Indeed + LinkedIn  via python-jobspy
  - jobs.ch            via requests + BeautifulSoup
  - jobup.ch           via requests + BeautifulSoup
"""

import time
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

LOCATION = "Zurich, Switzerland"

SEARCH_KEYWORDS = [
    "insurance",
    "reinsurance",
    "specialty insurance",
    "actuary",
    "underwriting",
    "claims",
    "risk analyst",
    "sales insurance",
    "business development insurance",
    "product strategy insurance",
    "sales reinsurance",
    "business development reinsurance",
]

# Title keywords that indicate entry-level / internship roles
LEVEL_KEYWORDS = [
    "intern",
    "internship",
    "junior",
    "entry",
    "graduate",
    "trainee",
    "werkstudent",
    "praktikum",
    "student",
    "working student",
    "associate",
    "analyst",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


# ---------------------------------------------------------------------------
# Level filter
# ---------------------------------------------------------------------------

def _is_entry_level(title: str) -> bool:
    title_lower = (title or "").lower()
    return any(kw in title_lower for kw in LEVEL_KEYWORDS)


# ---------------------------------------------------------------------------
# jobspy scraper (Indeed + LinkedIn)
# ---------------------------------------------------------------------------

def scrape_jobspy(progress_callback=None) -> pd.DataFrame:
    """Scrape Indeed and LinkedIn via python-jobspy."""
    try:
        from jobspy import scrape_jobs
    except ImportError:
        logger.warning("python-jobspy not installed. Run: pip install python-jobspy")
        return pd.DataFrame()

    all_frames = []
    total = len(SEARCH_KEYWORDS)

    for i, keyword in enumerate(SEARCH_KEYWORDS):
        if progress_callback:
            progress_callback(f"jobspy: searching '{keyword}'…", (i + 1) / total * 0.5)
        try:
            df = scrape_jobs(
                site_name=["indeed", "linkedin"],
                search_term=keyword,
                location=LOCATION,
                results_wanted=50,
                hours_old=720,          # ~30 days
                country_indeed="Switzerland",
            )
            if not df.empty:
                all_frames.append(df)
            time.sleep(1.5)             # be polite
        except Exception as e:
            logger.warning(f"jobspy failed for keyword '{keyword}': {e}")

    if not all_frames:
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["job_url"])

    # Normalize columns
    combined = combined.rename(columns={"site": "source"})
    combined["source"] = combined["source"].str.title()

    # Keep only entry-level
    mask = combined["title"].apply(_is_entry_level)
    filtered = combined[mask].copy()

    logger.info(f"jobspy: {len(filtered)} entry-level jobs (from {len(combined)} total)")
    return filtered


# ---------------------------------------------------------------------------
# jobs.ch scraper
# ---------------------------------------------------------------------------

def scrape_jobs_ch(progress_callback=None) -> pd.DataFrame:
    """Scrape jobs.ch for Zurich insurance jobs."""
    results = []
    total = len(SEARCH_KEYWORDS)

    for i, keyword in enumerate(SEARCH_KEYWORDS):
        if progress_callback:
            progress_callback(f"jobs.ch: searching '{keyword}'…", 0.5 + (i + 1) / total * 0.25)
        try:
            url = (
                f"https://www.jobs.ch/en/vacancies/"
                f"?term={requests.utils.quote(keyword)}"
                f"&location=zurich"
            )
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                logger.warning(f"jobs.ch returned {resp.status_code} for '{keyword}'")
                time.sleep(2)
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            # jobs.ch renders job cards in <article> or <div> tags; try multiple selectors
            cards = (
                soup.select("article[data-cy='job-card']")
                or soup.select("div[data-cy='job-element']")
                or soup.select("article.job-tile")
                or soup.select("li.job-item")
            )

            if not cards:
                # Fallback: look for any <a> tags with /en/vacancies/ in href
                links = soup.find_all("a", href=lambda h: h and "/en/vacancies/" in h)
                for link in links:
                    title = link.get_text(strip=True)
                    if not title:
                        continue
                    job_url = link["href"]
                    if not job_url.startswith("http"):
                        job_url = "https://www.jobs.ch" + job_url
                    results.append({
                        "title": title,
                        "company": "",
                        "location": "Zurich",
                        "job_url": job_url,
                        "source": "jobs.ch",
                        "job_type": "",
                        "date_posted": "",
                    })
            else:
                for card in cards:
                    title_el = card.select_one("h2, h3, [data-cy='job-title'], .job-title")
                    company_el = card.select_one("[data-cy='company-name'], .company-name, .employer")
                    link_el = card.select_one("a[href]")

                    title = title_el.get_text(strip=True) if title_el else ""
                    company = company_el.get_text(strip=True) if company_el else ""
                    job_url = ""
                    if link_el:
                        job_url = link_el["href"]
                        if not job_url.startswith("http"):
                            job_url = "https://www.jobs.ch" + job_url

                    if title and job_url:
                        results.append({
                            "title": title,
                            "company": company,
                            "location": "Zurich",
                            "job_url": job_url,
                            "source": "jobs.ch",
                            "job_type": "",
                            "date_posted": "",
                        })

            time.sleep(1.5)

        except Exception as e:
            logger.warning(f"jobs.ch scraper failed for '{keyword}': {e}")

    df = pd.DataFrame(results)
    if df.empty:
        return df

    df = df.drop_duplicates(subset=["job_url"])
    df = df[df["title"].apply(_is_entry_level)]
    logger.info(f"jobs.ch: {len(df)} entry-level jobs found")
    return df


# ---------------------------------------------------------------------------
# jobup.ch scraper
# ---------------------------------------------------------------------------

def scrape_jobup_ch(progress_callback=None) -> pd.DataFrame:
    """Scrape jobup.ch for Zurich insurance jobs."""
    results = []
    total = len(SEARCH_KEYWORDS)

    for i, keyword in enumerate(SEARCH_KEYWORDS):
        if progress_callback:
            progress_callback(f"jobup.ch: searching '{keyword}'…", 0.75 + (i + 1) / total * 0.25)
        try:
            url = (
                f"https://www.jobup.ch/en/jobs/"
                f"?term={requests.utils.quote(keyword)}"
                f"&location=zurich"
            )
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                logger.warning(f"jobup.ch returned {resp.status_code} for '{keyword}'")
                time.sleep(2)
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            # jobup.ch job cards
            cards = (
                soup.select("article[data-feat='job']")
                or soup.select("li[data-job-id]")
                or soup.select("div.job-item")
                or soup.select("article.job-listing")
            )

            if not cards:
                # Fallback: find links to job detail pages
                links = soup.find_all(
                    "a", href=lambda h: h and ("/en/jobs/" in h) and h.count("/") > 3
                )
                seen = set()
                for link in links:
                    title = link.get_text(strip=True)
                    job_url = link["href"]
                    if not job_url.startswith("http"):
                        job_url = "https://www.jobup.ch" + job_url
                    if not title or job_url in seen:
                        continue
                    seen.add(job_url)
                    results.append({
                        "title": title,
                        "company": "",
                        "location": "Zurich",
                        "job_url": job_url,
                        "source": "jobup.ch",
                        "job_type": "",
                        "date_posted": "",
                    })
            else:
                for card in cards:
                    title_el = card.select_one("h2, h3, .job-title, [data-feat='job-title']")
                    company_el = card.select_one(".company, .employer, [data-feat='company']")
                    link_el = card.select_one("a[href]")

                    title = title_el.get_text(strip=True) if title_el else ""
                    company = company_el.get_text(strip=True) if company_el else ""
                    job_url = ""
                    if link_el:
                        job_url = link_el["href"]
                        if not job_url.startswith("http"):
                            job_url = "https://www.jobup.ch" + job_url

                    if title and job_url:
                        results.append({
                            "title": title,
                            "company": company,
                            "location": "Zurich",
                            "job_url": job_url,
                            "source": "jobup.ch",
                            "job_type": "",
                            "date_posted": "",
                        })

            time.sleep(1.5)

        except Exception as e:
            logger.warning(f"jobup.ch scraper failed for '{keyword}': {e}")

    df = pd.DataFrame(results)
    if df.empty:
        return df

    df = df.drop_duplicates(subset=["job_url"])
    df = df[df["title"].apply(_is_entry_level)]
    logger.info(f"jobup.ch: {len(df)} entry-level jobs found")
    return df


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_all_scrapers(progress_callback=None) -> pd.DataFrame:
    """Run all scrapers and return a combined, deduplicated DataFrame."""
    frames = []

    for scraper_fn in [scrape_jobspy, scrape_jobs_ch, scrape_jobup_ch]:
        try:
            df = scraper_fn(progress_callback=progress_callback)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            logger.error(f"Scraper {scraper_fn.__name__} failed: {e}")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["job_url"])
    logger.info(f"Total: {len(combined)} unique entry-level jobs across all sources")
    return combined
