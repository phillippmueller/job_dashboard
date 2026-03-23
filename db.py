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
                url             TEXT PRIMARY KEY,
                title           TEXT,
                company         TEXT,
                location        TEXT,
                source          TEXT,
                job_type        TEXT,
                date_posted     TEXT,
                date_scraped    TEXT,
                hidden          INTEGER DEFAULT 0,
                category        TEXT DEFAULT 'insurance',
                search_location TEXT DEFAULT 'Zurich, Switzerland'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scrape_runs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp  TEXT NOT NULL,
                location   TEXT,
                category   TEXT,
                jobs_found INTEGER DEFAULT 0,
                jobs_new   INTEGER DEFAULT 0
            )
        """)
        # Migrate existing DBs that predate the category column
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN category TEXT DEFAULT 'insurance'")
        except sqlite3.OperationalError:
            pass  # column already exists
        # Migrate existing DBs that predate the search_location column
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN search_location TEXT DEFAULT 'Zurich, Switzerland'")
        except sqlite3.OperationalError:
            pass  # column already exists
        conn.commit()


def upsert_jobs(df: pd.DataFrame, category: str = "insurance", search_location: str = "Zurich, Switzerland"):
    if df.empty:
        return 0
    now = datetime.now().isoformat(timespec="seconds")
    records = [
        (
            row.get("job_url") or row.get("url", ""),
            row.get("title", ""),
            row.get("company", ""),
            row.get("location", ""),
            row.get("source", ""),
            row.get("job_type", ""),
            str(row.get("date_posted", "")),
            now,
            category,
            search_location,
        )
        for _, row in df.iterrows()
    ]
    with sqlite3.connect(DB_PATH) as conn:
        before = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        conn.executemany(
            """
            INSERT OR IGNORE INTO jobs
                (url, title, company, location, source, job_type, date_posted, date_scraped, category, search_location)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )
        after = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_scraped', ?)", (now,)
        )
        conn.commit()
    return after - before


def get_jobs(include_hidden: bool = False, category: str = None, search_location: str = None) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        conditions = []
        params = []
        if not include_hidden:
            conditions.append("hidden = 0")
        if category is not None:
            conditions.append("category = ?")
            params.append(category)
        if search_location is not None:
            conditions.append("search_location = ?")
            params.append(search_location)
        query = "SELECT * FROM jobs"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY date_posted DESC, date_scraped DESC"
        df = pd.read_sql_query(query, conn, params=params if params else None)
    return df


def hide_job(url: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE jobs SET hidden = 1 WHERE url = ?", (url,))
        conn.commit()


def log_scrape_run(location: str, category: str, jobs_found: int, jobs_new: int):
    now = datetime.now().isoformat(timespec="seconds")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO scrape_runs (timestamp, location, category, jobs_found, jobs_new) VALUES (?, ?, ?, ?, ?)",
            (now, location, category, jobs_found, jobs_new),
        )
        conn.commit()


def get_scrape_runs() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(
            "SELECT * FROM scrape_runs ORDER BY timestamp DESC", conn
        )


def get_last_scraped() -> str | None:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT value FROM meta WHERE key = 'last_scraped'"
            ).fetchone()
            return row[0] if row else None
    except Exception:
        return None
