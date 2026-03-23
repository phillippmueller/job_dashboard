"""
Zurich Job Dashboard
---------------------
Aggregates entry-level / internship job postings from
Indeed, LinkedIn, jobs.ch, and jobup.ch.

Categories: Insurance | Private Markets
"""

import streamlit as st
import pandas as pd
from db import init_db, upsert_jobs, get_jobs, hide_job, get_last_scraped, log_scrape_run, get_scrape_runs
from scraper import run_all_categories, DEFAULT_LOCATION, LEVEL_KEYWORDS

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Zurich Jobs Dashboard",
    page_icon="🏢",
    layout="wide",
)

init_db()


# ---------------------------------------------------------------------------
# Shared job card renderer
# ---------------------------------------------------------------------------
def _render_jobs(df: pd.DataFrame, show_hidden: bool, tab_key: str):
    if df.empty:
        st.warning("No jobs match your filters.")
        return

    display_df = df[["title", "company", "location", "source", "job_type", "date_posted", "url"]].copy()
    display_df.columns = ["Title", "Company", "Location", "Source", "Type", "Posted", "URL"]

    st.markdown("### Job Listings")

    for idx, row in display_df.iterrows():
        url = row["URL"]
        title = row["Title"] or "(no title)"
        company = row["Company"] or "—"
        source = row["Source"] or "—"
        job_type = row["Type"] or "—"
        posted = row["Posted"] or "—"
        location = row["Location"] or "Zurich"

        with st.container(border=True):
            c1, c2 = st.columns([5, 1])
            with c1:
                if url:
                    st.markdown(f"#### [{title}]({url})")
                else:
                    st.markdown(f"#### {title}")
                meta_cols = st.columns(4)
                meta_cols[0].caption(f"🏢 {company}")
                meta_cols[1].caption(f"📍 {location}")
                meta_cols[2].caption(f"🔖 {source} · {job_type}")
                meta_cols[3].caption(f"📅 {posted}")
            with c2:
                if not show_hidden:
                    if st.button("Hide", key=f"hide_{tab_key}_{url}_{idx}", use_container_width=True):
                        hide_job(url)
                        st.rerun()


# ---------------------------------------------------------------------------
# Sidebar — global controls
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("🏢 Zurich Jobs")
    st.caption("Entry Level / Internship · Zurich")

    st.divider()

    location = st.text_input(
        "📍 Location",
        value=DEFAULT_LOCATION,
        placeholder="e.g. Geneva, Switzerland",
    )

    if st.button("🔄 Scrape Now", use_container_width=True, type="primary"):
        progress_bar = st.progress(0.0, text="Starting scrapers…")

        def update_progress(msg, pct):
            progress_bar.progress(min(pct, 1.0), text=msg)

        scrape_location = location.strip() or DEFAULT_LOCATION
        with st.spinner("Scraping job boards…"):
            ins_jobs, pm_jobs = run_all_categories(
                location=scrape_location,
                progress_callback=update_progress,
            )

        progress_bar.progress(1.0, text="Saving to database…")

        n_ins = upsert_jobs(ins_jobs, category="insurance", search_location=scrape_location) if not ins_jobs.empty else 0
        n_pm = upsert_jobs(pm_jobs, category="private_markets", search_location=scrape_location) if not pm_jobs.empty else 0
        total = n_ins + n_pm

        log_scrape_run(scrape_location, "insurance", len(ins_jobs), n_ins)
        log_scrape_run(scrape_location, "private_markets", len(pm_jobs), n_pm)

        if total:
            st.success(f"Added {total} new job(s). ({n_ins} insurance, {n_pm} private markets)")
        else:
            st.info("No new jobs found.")

        progress_bar.empty()
        st.rerun()

    last = get_last_scraped()
    if last:
        st.caption(f"Last scraped: {last}")

    st.divider()

    show_hidden = st.toggle("Show hidden jobs", value=False)

    st.divider()
    st.caption("Click **Hide** on a row to remove it from view.")


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
SOURCE_OPTIONS = ["Indeed", "Linkedin", "jobs.ch", "jobup.ch"]

tab1, tab2, tab3 = st.tabs(["Insurance", "Private Markets", "Analytics"])

# --- Insurance tab ---
with tab1:
    col_f1, col_f2 = st.columns([2, 3])
    with col_f1:
        ins_sources = st.multiselect(
            "Source", options=SOURCE_OPTIONS, default=SOURCE_OPTIONS, key="ins_sources"
        )
    with col_f2:
        ins_keyword = st.text_input(
            "Search title / company", placeholder="e.g. actuary", key="ins_keyword"
        )

    df_ins = get_jobs(include_hidden=show_hidden, category="insurance", search_location=location.strip() or DEFAULT_LOCATION)

    if df_ins.empty:
        st.info("No insurance jobs yet. Click **Scrape Now** to fetch postings.")
    else:
        if ins_sources:
            df_ins = df_ins[df_ins["source"].str.lower().isin([s.lower() for s in ins_sources])]
        if ins_keyword.strip():
            kw = ins_keyword.strip().lower()
            df_ins = df_ins[
                df_ins["title"].str.lower().str.contains(kw, na=False)
                | df_ins["company"].str.lower().str.contains(kw, na=False)
            ]

        c1, c2, c3 = st.columns(3)
        c1.metric("Total shown", len(df_ins))
        c2.metric("Sources", df_ins["source"].nunique())
        c3.metric("Companies", df_ins["company"].nunique())
        st.divider()

        _render_jobs(df_ins, show_hidden, tab_key="ins")

# --- Private Markets tab ---
with tab2:
    col_f1, col_f2 = st.columns([2, 3])
    with col_f1:
        pm_sources = st.multiselect(
            "Source", options=SOURCE_OPTIONS, default=SOURCE_OPTIONS, key="pm_sources"
        )
    with col_f2:
        pm_keyword = st.text_input(
            "Search title / company", placeholder="e.g. private equity", key="pm_keyword"
        )

    df_pm = get_jobs(include_hidden=show_hidden, category="private_markets", search_location=location.strip() or DEFAULT_LOCATION)

    if df_pm.empty:
        st.info("No private markets jobs yet. Click **Scrape Now** to fetch postings.")
    else:
        if pm_sources:
            df_pm = df_pm[df_pm["source"].str.lower().isin([s.lower() for s in pm_sources])]
        if pm_keyword.strip():
            kw = pm_keyword.strip().lower()
            df_pm = df_pm[
                df_pm["title"].str.lower().str.contains(kw, na=False)
                | df_pm["company"].str.lower().str.contains(kw, na=False)
            ]

        c1, c2, c3 = st.columns(3)
        c1.metric("Total shown", len(df_pm))
        c2.metric("Sources", df_pm["source"].nunique())
        c3.metric("Companies", df_pm["company"].nunique())
        st.divider()

        _render_jobs(df_pm, show_hidden, tab_key="pm")

# --- Analytics tab ---
with tab3:
    st.markdown("### Scrape History")

    runs_df = get_scrape_runs()

    if runs_df.empty:
        st.info("No scrape runs recorded yet. Click **Scrape Now** to get started.")
    else:
        # --- Summary metrics ---
        total_runs = len(runs_df)
        total_found = runs_df["jobs_found"].sum()
        total_new = runs_df["jobs_new"].sum()

        m1, m2, m3 = st.columns(3)
        m1.metric("Total scrape runs", total_runs)
        m2.metric("Total jobs found", int(total_found))
        m3.metric("Total new jobs added", int(total_new))

        st.divider()

        # --- Scrape history table ---
        st.markdown("#### Run Log")
        display_runs = runs_df[["timestamp", "location", "category", "jobs_found", "jobs_new"]].copy()
        display_runs.columns = ["Timestamp", "Location", "Category", "Found", "New"]
        display_runs["Category"] = display_runs["Category"].str.replace("_", " ").str.title()
        st.dataframe(display_runs, use_container_width=True, hide_index=True)

        st.divider()

        # --- New jobs over time (per category) ---
        st.markdown("#### New Jobs Per Scrape Run")
        chart_df = runs_df[["timestamp", "category", "jobs_new"]].copy()
        chart_df["timestamp"] = pd.to_datetime(chart_df["timestamp"])
        chart_df = chart_df.sort_values("timestamp")
        ins_series = chart_df[chart_df["category"] == "insurance"].set_index("timestamp")["jobs_new"].rename("Insurance")
        pm_series = chart_df[chart_df["category"] == "private_markets"].set_index("timestamp")["jobs_new"].rename("Private Markets")
        trend_df = pd.concat([ins_series, pm_series], axis=1).fillna(0)
        st.line_chart(trend_df)

        st.divider()

        # --- Top companies ---
        st.markdown("#### Top Companies by Job Count")
        all_jobs = get_jobs(include_hidden=True)
        if not all_jobs.empty:
            col_a, col_b = st.columns(2)

            with col_a:
                st.caption("Insurance")
                ins_companies = (
                    all_jobs[all_jobs["category"] == "insurance"]["company"]
                    .replace("", pd.NA).dropna()
                    .value_counts()
                    .head(10)
                    .rename_axis("Company")
                    .reset_index(name="Jobs")
                )
                st.bar_chart(ins_companies.set_index("Company"))

            with col_b:
                st.caption("Private Markets")
                pm_companies = (
                    all_jobs[all_jobs["category"] == "private_markets"]["company"]
                    .replace("", pd.NA).dropna()
                    .value_counts()
                    .head(10)
                    .rename_axis("Company")
                    .reset_index(name="Jobs")
                )
                if pm_companies.empty:
                    st.info("No private markets data yet.")
                else:
                    st.bar_chart(pm_companies.set_index("Company"))

        st.divider()

        # --- Position level breakdown ---
        st.markdown("#### Position Level Breakdown")
        if not all_jobs.empty:
            level_counts = {}
            titles_lower = all_jobs["title"].str.lower().fillna("")
            for kw in LEVEL_KEYWORDS:
                count = titles_lower.str.contains(kw, regex=False).sum()
                if count > 0:
                    level_counts[kw] = int(count)
            if level_counts:
                level_df = (
                    pd.Series(level_counts, name="Count")
                    .sort_values(ascending=False)
                    .rename_axis("Level Keyword")
                    .reset_index()
                )
                st.bar_chart(level_df.set_index("Level Keyword"))
