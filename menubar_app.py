"""
macOS Menu Bar app for the Zurich Insurance Job Dashboard.

Run with:
    mamba run -n job_dashboard python menubar_app.py

A 💼 icon will appear in your menu bar (top-right of screen).
"""

import rumps
import subprocess
import threading
import webbrowser
import time
import sys
import os

DASHBOARD_URL = "http://localhost:8501"
APP_DIR = os.path.dirname(os.path.abspath(__file__))
CONDA_ENV = "job_dashboard"

# Find the mamba/conda python for this env
PYTHON = sys.executable
STREAMLIT = os.path.join(os.path.dirname(PYTHON), "streamlit")


class JobDashboardApp(rumps.App):
    def __init__(self):
        super().__init__("🌝", quit_button=None)
        self.streamlit_proc = None

        self.menu = [
            rumps.MenuItem("Open Dashboard", callback=self.open_dashboard),
            rumps.MenuItem("🔄 Scrape Jobs Now", callback=self.scrape_now),
            None,  # separator
            rumps.MenuItem("▶ Start Server", callback=self.start_server),
            rumps.MenuItem("■ Stop Server", callback=self.stop_server),
            None,  # separator
            rumps.MenuItem("Quit", callback=self.quit_app),
        ]

        # Auto-start server on launch
        threading.Thread(target=self._start_streamlit, daemon=True).start()

    # ------------------------------------------------------------------
    def _start_streamlit(self):
        if self.streamlit_proc and self.streamlit_proc.poll() is None:
            return  # already running
        self.streamlit_proc = subprocess.Popen(
            [STREAMLIT, "run", "app.py", "--server.headless", "true",
             "--server.port", "8501"],
            cwd=APP_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Wait for server to be ready then open browser
        time.sleep(3)
        webbrowser.open(DASHBOARD_URL)
        rumps.notification(
            title="Insurance Jobs",
            subtitle="Dashboard is ready",
            message=DASHBOARD_URL,
        )

    def start_server(self, _):
        if self.streamlit_proc and self.streamlit_proc.poll() is None:
            rumps.notification("Insurance Jobs", "", "Server is already running.")
            webbrowser.open(DASHBOARD_URL)
            return
        threading.Thread(target=self._start_streamlit, daemon=True).start()

    def stop_server(self, _):
        if self.streamlit_proc and self.streamlit_proc.poll() is None:
            self.streamlit_proc.terminate()
            self.streamlit_proc = None
            rumps.notification("Insurance Jobs", "", "Server stopped.")
        else:
            rumps.notification("Insurance Jobs", "", "Server is not running.")

    def open_dashboard(self, _):
        webbrowser.open(DASHBOARD_URL)

    def scrape_now(self, _):
        rumps.notification("Insurance Jobs", "Scraping started…", "This may take a minute.")
        def _run():
            subprocess.run(
                [PYTHON, "-c",
                 "from scraper import run_all_scrapers; from db import init_db, upsert_jobs; "
                 "init_db(); df = run_all_scrapers(); n = upsert_jobs(df) if not df.empty else 0; "
                 "print(n)"],
                cwd=APP_DIR,
            )
            rumps.notification("Insurance Jobs", "Scrape complete ✓", "Refresh the dashboard to see new jobs.")
        threading.Thread(target=_run, daemon=True).start()

    def quit_app(self, _):
        self.stop_server(None)
        rumps.quit_application()


if __name__ == "__main__":
    JobDashboardApp().run()
