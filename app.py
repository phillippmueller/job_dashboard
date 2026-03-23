"""
Zurich Insurance Job Dashboard
--------------------------------
Aggregates entry-level / internship insurance job postings from
Indeed, LinkedIn, jobs.ch, and jobup.ch.
"""

import streamlit as st
import pandas as pd
from db import init_db, upsert_jobs, get_jobs, hide_job, get_last_scraped
from scraper import run_all_scrapers

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Zurich Insurance Jobs",
    page_icon="🏢",
    layout="wide",
)

init_db()


# ---------------------------------------------------------------------------
# Sidebar — controls
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("🏢 Insurance Jobs")
    st.caption("Zurich · Entry Level / Internship")

    st.divider()

    # Scrape button
    if st.button("🔄 Scrape Now", use_container_width=True, type="primary"):
        progress_bar = st.progress(0.0, text="Starting scrapers…")

        def update_progress(msg, pct):
            progress_bar.progress(min(pct, 1.0), text=msg)

        with st.spinner("Scraping job boards…"):
            new_jobs = run_all_scrapers(progress_callback=update_progress)

        progress_bar.progress(1.0, text="Saving to database…")

        if not new_jobs.empty:
            n = upsert_jobs(new_jobs)
            st.success(f"Added {n} new job(s).")
        else:
            st.info("No new jobs found.")

        progress_bar.empty()
        st.rerun()

    last = get_last_scraped()
    if last:
        st.caption(f"Last scraped: {last}")

    st.divider()

    # --- Filters ---
    st.subheader("Filters")

    source_options = ["Indeed", "Linkedin", "jobs.ch", "jobup.ch"]
    selected_sources = st.multiselect(
        "Source",
        options=source_options,
        default=source_options,
    )

    keyword_filter = st.text_input("Search title / company", placeholder="e.g. actuary")

    show_hidden = st.toggle("Show hidden jobs", value=False)

    st.divider()
    st.caption("Click **Hide** on a row to remove it from view.")


# ---------------------------------------------------------------------------
# Load & filter data
# ---------------------------------------------------------------------------
df = get_jobs(include_hidden=show_hidden)

if df.empty:
    st.info("No jobs yet. Click **Scrape Now** in the sidebar to fetch postings.")
    st.stop()

# Source filter (case-insensitive)
if selected_sources:
    df = df[df["source"].str.lower().isin([s.lower() for s in selected_sources])]

# Keyword filter
if keyword_filter.strip():
    kw = keyword_filter.strip().lower()
    df = df[
        df["title"].str.lower().str.contains(kw, na=False)
        | df["company"].str.lower().str.contains(kw, na=False)
    ]

# ---------------------------------------------------------------------------
# Header stats
# ---------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)
col1.metric("Total shown", len(df))
col2.metric("Sources", df["source"].nunique() if not df.empty else 0)
col3.metric("Companies", df["company"].nunique() if not df.empty else 0)

st.divider()

# ---------------------------------------------------------------------------
# Job table
# ---------------------------------------------------------------------------
if df.empty:
    st.warning("No jobs match your filters.")
    st.stop()

# Build display DataFrame
display_df = df[["title", "company", "location", "source", "job_type", "date_posted", "url"]].copy()
display_df.columns = ["Title", "Company", "Location", "Source", "Type", "Posted", "URL"]

# Render each job as a row with an inline link and hide button
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
            # Clickable title
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
                if st.button("Hide", key=f"hide_{url}_{idx}", use_container_width=True):
                    hide_job(url)
                    st.rerun()
