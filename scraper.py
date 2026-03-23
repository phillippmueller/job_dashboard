"""
Scrapers for Zurich entry-level / internship jobs.

Sources:
  - Indeed + LinkedIn  via python-jobspy
  - jobs.ch            via requests + BeautifulSoup
  - jobup.ch           via requests + BeautifulSoup

Categories:
  - insurance
  - private_markets
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

DEFAULT_LOCATION = "Zurich, Switzerland"

# --- Insurance ---
INSURANCE_KEYWORDS = [
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

INSURANCE_DOMAIN_KEYWORDS = [
    "insurance",
    "reinsurance",
    "underwriting",
    "underwriter",
    "actuary",
    "actuarial",
    "claims",
    "specialty",
    "p&c",
    "life insurance",
    "broker",
    "brokerage",
    "treaty",
    "facultative",
    "cedant",
    "lloyd",
    "coverage",
    "policy",
    "insurer",
    "zurich insurance",
    "swiss re",
    "axa",
    "allianz",
    "generali",
    "helvetia",
    "baloise",
    "mobiliar",
    "vaudoise",
]

# --- Private Markets ---
PM_KEYWORDS = [
    "private equity",
    "venture capital",
    "private credit",
    "private debt",
    "infrastructure fund",
    "real assets",
    "private markets",
    "alternative investments",
    "alternatives investing",
    "buyout",
    "growth equity",
    "secondaries",
    "co-investment",
    "fund of funds",
]

PM_DOMAIN_KEYWORDS = [
    "private equity",
    "venture capital",
    "private credit",
    "private debt",
    "infrastructure",
    "real assets",
    "private markets",
    "alternatives",
    "buyout",
    "growth equity",
    "secondaries",
    "co-investment",
    "fund of funds",
    "general partner",
    "limited partner",
    "partners group",
    "lgt capital",
    "capital dynamics",
    "adveq",
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

CATEGORY_KEYWORDS = {
    "insurance": (INSURANCE_KEYWORDS, INSURANCE_DOMAIN_KEYWORDS),
    "private_markets": (PM_KEYWORDS, PM_DOMAIN_KEYWORDS),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_search_term(keywords: list) -> str:
    """Combine keywords into a single OR query, quoting multi-word phrases."""
    parts = [f'"{kw}"' if " " in kw else kw for kw in keywords]
    return " OR ".join(parts)


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

def _is_entry_level(title: str) -> bool:
    title_lower = (title or "").lower()
    return any(kw in title_lower for kw in LEVEL_KEYWORDS)


def _is_target_domain(title: str, company: str = "", domain_keywords: list = None) -> bool:
    text = ((title or "") + " " + (company or "")).lower()
    return any(kw in text for kw in (domain_keywords or []))


# ---------------------------------------------------------------------------
# jobspy scraper (Indeed + LinkedIn)
# ---------------------------------------------------------------------------

def scrape_jobspy(
    search_keywords: list,
    domain_keywords: list,
    location: str = DEFAULT_LOCATION,
    progress_callback=None,
    base: float = 0.0,
    share: float = 1.0,
) -> pd.DataFrame:
    """Scrape Indeed and LinkedIn via python-jobspy (single OR query)."""
    try:
        from jobspy import scrape_jobs
    except ImportError:
        logger.warning("python-jobspy not installed. Run: pip install python-jobspy")
        return pd.DataFrame()

    search_term = _build_search_term(search_keywords)
    if progress_callback:
        progress_callback("jobspy: searching Indeed + LinkedIn…", base + share * 0.25)

    try:
        df = scrape_jobs(
            site_name=["indeed", "linkedin"],
            search_term=search_term,
            location=location,
            results_wanted=50,
            hours_old=720,
            country_indeed="Switzerland",
        )
        time.sleep(1.5)
    except Exception as e:
        logger.warning(f"jobspy failed: {e}")
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    df = df.drop_duplicates(subset=["job_url"])
    df = df.rename(columns={"site": "source"})
    df["source"] = df["source"].str.title()

    mask = (
        df["title"].apply(_is_entry_level)
        & df.apply(
            lambda r: _is_target_domain(r["title"], r.get("company", ""), domain_keywords),
            axis=1,
        )
    )
    filtered = df[mask].copy()
    logger.info(f"jobspy: {len(filtered)} jobs (from {len(df)} total)")
    return filtered


# ---------------------------------------------------------------------------
# jobs.ch scraper
# ---------------------------------------------------------------------------

def scrape_jobs_ch(
    search_keywords: list,
    domain_keywords: list,
    location: str = DEFAULT_LOCATION,
    progress_callback=None,
    base: float = 0.0,
    share: float = 1.0,
) -> pd.DataFrame:
    """Scrape jobs.ch (single OR query)."""
    results = []
    location_slug = location.split(",")[0].strip().lower()
    search_term = _build_search_term(search_keywords)

    if progress_callback:
        progress_callback("jobs.ch: searching…", base + share * 0.5)

    try:
        url = (
            f"https://www.jobs.ch/en/vacancies/"
            f"?term={requests.utils.quote(search_term)}"
            f"&location={requests.utils.quote(location_slug)}"
        )
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"jobs.ch returned {resp.status_code}")
            return pd.DataFrame()

        soup = BeautifulSoup(resp.text, "lxml")
        cards = (
            soup.select("article[data-cy='job-card']")
            or soup.select("div[data-cy='job-element']")
            or soup.select("article.job-tile")
            or soup.select("li.job-item")
        )

        if not cards:
            links = soup.find_all("a", href=lambda h: h and "/en/vacancies/" in h)
            for link in links:
                title = link.get_text(strip=True)
                if not title:
                    continue
                job_url = link["href"]
                if not job_url.startswith("http"):
                    job_url = "https://www.jobs.ch" + job_url
                results.append({
                    "title": title, "company": "", "location": location_slug.title(),
                    "job_url": job_url, "source": "jobs.ch", "job_type": "", "date_posted": "",
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
                        "title": title, "company": company, "location": location_slug.title(),
                        "job_url": job_url, "source": "jobs.ch", "job_type": "", "date_posted": "",
                    })

        time.sleep(1.5)
    except Exception as e:
        logger.warning(f"jobs.ch scraper failed: {e}")

    df = pd.DataFrame(results)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["job_url"])
    df = df[
        df["title"].apply(_is_entry_level)
        & df.apply(lambda r: _is_target_domain(r["title"], r["company"], domain_keywords), axis=1)
    ]
    logger.info(f"jobs.ch: {len(df)} jobs found")
    return df


# ---------------------------------------------------------------------------
# jobup.ch scraper
# ---------------------------------------------------------------------------

def scrape_jobup_ch(
    search_keywords: list,
    domain_keywords: list,
    location: str = DEFAULT_LOCATION,
    progress_callback=None,
    base: float = 0.0,
    share: float = 1.0,
) -> pd.DataFrame:
    """Scrape jobup.ch (single OR query)."""
    results = []
    location_slug = location.split(",")[0].strip().lower()
    search_term = _build_search_term(search_keywords)

    if progress_callback:
        progress_callback("jobup.ch: searching…", base + share * 0.75)

    try:
        url = (
            f"https://www.jobup.ch/en/jobs/"
            f"?term={requests.utils.quote(search_term)}"
            f"&location={requests.utils.quote(location_slug)}"
        )
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"jobup.ch returned {resp.status_code}")
            return pd.DataFrame()

        soup = BeautifulSoup(resp.text, "lxml")
        cards = (
            soup.select("article[data-feat='job']")
            or soup.select("li[data-job-id]")
            or soup.select("div.job-item")
            or soup.select("article.job-listing")
        )

        if not cards:
            seen = set()
            links = soup.find_all(
                "a", href=lambda h: h and ("/en/jobs/" in h) and h.count("/") > 3
            )
            for link in links:
                title = link.get_text(strip=True)
                job_url = link["href"]
                if not job_url.startswith("http"):
                    job_url = "https://www.jobup.ch" + job_url
                if not title or job_url in seen:
                    continue
                seen.add(job_url)
                results.append({
                    "title": title, "company": "", "location": location_slug.title(),
                    "job_url": job_url, "source": "jobup.ch", "job_type": "", "date_posted": "",
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
                        "title": title, "company": company, "location": location_slug.title(),
                        "job_url": job_url, "source": "jobup.ch", "job_type": "", "date_posted": "",
                    })

        time.sleep(1.5)
    except Exception as e:
        logger.warning(f"jobup.ch scraper failed: {e}")

    df = pd.DataFrame(results)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["job_url"])
    df = df[
        df["title"].apply(_is_entry_level)
        & df.apply(lambda r: _is_target_domain(r["title"], r["company"], domain_keywords), axis=1)
    ]
    logger.info(f"jobup.ch: {len(df)} jobs found")
    return df


# ---------------------------------------------------------------------------
# Category runner
# ---------------------------------------------------------------------------

def run_all_scrapers(
    category: str = "insurance",
    location: str = DEFAULT_LOCATION,
    progress_callback=None,
    base: float = 0.0,
    share: float = 1.0,
) -> pd.DataFrame:
    """Run all three scrapers for a given category and location."""
    search_keywords, domain_keywords = CATEGORY_KEYWORDS[category]
    frames = []

    for scraper_fn in [scrape_jobspy, scrape_jobs_ch, scrape_jobup_ch]:
        try:
            df = scraper_fn(
                search_keywords=search_keywords,
                domain_keywords=domain_keywords,
                location=location,
                progress_callback=progress_callback,
                base=base,
                share=share,
            )
            if not df.empty:
                frames.append(df)
        except Exception as e:
            logger.error(f"Scraper {scraper_fn.__name__} failed: {e}")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["job_url"])
    logger.info(f"[{category}] Total: {len(combined)} unique jobs")
    return combined


def run_all_categories(location: str = DEFAULT_LOCATION, progress_callback=None):
    """Run scrapers for all categories. Returns (insurance_df, private_markets_df)."""
    insurance = run_all_scrapers(
        category="insurance",
        location=location,
        progress_callback=progress_callback,
        base=0.0,
        share=0.5,
    )
    private_markets = run_all_scrapers(
        category="private_markets",
        location=location,
        progress_callback=progress_callback,
        base=0.5,
        share=0.5,
    )
    return insurance, private_markets
