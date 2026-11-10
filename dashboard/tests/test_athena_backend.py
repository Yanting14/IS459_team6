"""Run the page tests and the athena-mode tests with DASH_SOURCE=athena and a fake Athena.

This has to be a subprocess: the data source is read from the environment when `dashlib.config` is imported.
It is the proof that the dashboard works end to end through the Athena code path; it does not prove that the
real Athena accepts the SQL (the fake is DuckDB), which `python athena/run_sql.py` and the app itself show.
"""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_all_pages_work_through_the_athena_code_path():
    env = {**os.environ, "DASH_SOURCE": "athena", "DASH_TEST_FAKE_ATHENA": "1"}
    done = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
         "tests/test_pages.py", "tests/test_athena_mode.py"],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=600)
    assert done.returncode == 0, done.stdout[-4000:] + done.stderr[-2000:]
    assert " passed" in done.stdout and "skipped" not in done.stdout
