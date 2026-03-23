import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "data" / "jobs.db"


def init_db():
    DB_PATH.parent.mkdir(exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                url        TEXT PRIMARY KEY,
                title      TEXT,
                company    TEXT,
                location   TEXT,
                source     TEXT,
                job_type   TEXT,
                date_posted TEXT,
                date_scraped TEXT,
                hidden     INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()


def upsert_jobs(df: pd.DataFrame):
    if df.empty:
        return 0
    now = datetime.now().isoformat(timespec="seconds")
    with sqlite3.connect(DB_PATH) as conn:
        inserted = 0
        for _, row in df.iterrows():
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO jobs
                    (url, title, company, location, source, job_type, date_posted, date_scraped)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("job_url") or row.get("url", ""),
                    row.get("title", ""),
                    row.get("company", ""),
                    row.get("location", ""),
                    row.get("source", ""),
                    row.get("job_type", ""),
                    str(row.get("date_posted", "")),
                    now,
                ),
            )
            inserted += cur.rowcount
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_scraped', ?)", (now,)
        )
        conn.commit()
    return inserted


def get_jobs(include_hidden: bool = False) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        query = "SELECT * FROM jobs"
        if not include_hidden:
            query += " WHERE hidden = 0"
        query += " ORDER BY date_posted DESC, date_scraped DESC"
        df = pd.read_sql_query(query, conn)
    return df


def hide_job(url: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE jobs SET hidden = 1 WHERE url = ?", (url,))
        conn.commit()


def get_last_scraped() -> str | None:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT value FROM meta WHERE key = 'last_scraped'"
            ).fetchone()
            return row[0] if row else None
    except Exception:
        return None
