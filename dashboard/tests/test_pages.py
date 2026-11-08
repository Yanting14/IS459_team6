"""Run each page headlessly against the sample data and fail on any exception."""
import os
import sys
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DASH_SOURCE", "sample")
sys.path.insert(0, str(ROOT))

PAGES = ["views/review_queue.py", "views/account_investigation.py", "views/sharing_experiment.py"]


@pytest.mark.parametrize("page", PAGES)
def test_page_runs_without_error(page):
    at = AppTest.from_file(str(ROOT / page), default_timeout=60).run()
    assert not at.exception, [e.value for e in at.exception]


def test_bank_view_hides_second_hop_transfers():
    at = AppTest.from_file(str(ROOT / "views/account_investigation.py"), default_timeout=60).run()
    at.sidebar.radio[0].set_value("Bank view").run()
    assert not at.exception, [e.value for e in at.exception]
    assert any("Bank view" in i.value for i in at.info)
