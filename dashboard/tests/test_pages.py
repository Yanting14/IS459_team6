"""Run each page headlessly against the sample data and fail on any exception."""
import json
import os
import sys
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DASH_SOURCE", "sample")
sys.path.insert(0, str(ROOT))

PAGES = ["views/review_queue.py", "views/account_investigation.py", "views/sharing_experiment.py"]
DAYS = ["2022-09-15", "2022-09-16"]


@pytest.mark.parametrize("page", PAGES)
def test_page_runs_without_error(page):
    at = AppTest.from_file(str(ROOT / page), default_timeout=60).run()
    assert not at.exception, [e.value for e in at.exception]


def test_bank_view_hides_second_hop_transfers():
    at = AppTest.from_file(str(ROOT / "views/account_investigation.py"), default_timeout=60).run()
    at.sidebar.radio[0].set_value("Bank view").run()
    assert not at.exception, [e.value for e in at.exception]
    assert any("Bank view" in i.value for i in at.info)


def test_queue_shows_reasons_and_no_transfer_columns():
    at = AppTest.from_file(str(ROOT / "views/review_queue.py"), default_timeout=60).run()
    queue = at.dataframe[0].value
    assert list(queue.columns[:7]) == ["rank", "bank_id", "account_id", "score_exact",
                                       "reason_1", "reason_2", "reason_3"]
    assert "supporting_transfer_id" not in queue.columns
    assert queue["reason_1"].str.contains(" in top ").all()
    assert queue["bank_id"].str.len().eq(3).all()  # leading zeros kept


@pytest.mark.parametrize("day", DAYS)
def test_comparison_panel_shows_each_days_metrics_and_no_gin(day):
    at = AppTest.from_file(str(ROOT / "views/review_queue.py"), default_timeout=60).run()
    at.sidebar.selectbox[0].set_value(day).run()
    assert not at.exception, [e.value for e in at.exception]
    panel = at.dataframe[1].value.set_index("approach")
    assert list(panel.index) == ["Gradient-boosted trees", "Logistic regression", "Rules (label-free)",
                                 "Best-case rules (weights tuned on validation labels)"]
    models = json.loads((ROOT / "sample_data/metrics/models.json").read_text())
    gbt = next(d for d in models["results"]["gbt"]["test"] if d["date"] == day)
    assert panel.loc["Gradient-boosted trees", "precision_at_k"] == gbt["precision_at_k"]
    assert panel.loc["Gradient-boosted trees", "recall_at_k"] == gbt["recall_at_k"]
    assert at.metric[1].value == day
    assert at.metric[2].value == f"{gbt['precision_at_k'] * 100:.1f}%"
    text = " ".join(str(e.value) for e in [*at.markdown, *at.caption])
    assert "GIN" not in text and "graph neural" not in text
    assert "using only what the account's own bank can see" in text


def test_investigation_lists_the_reasons_without_transfer_scores():
    at = AppTest.from_file(str(ROOT / "views/account_investigation.py"), default_timeout=60).run()
    assert not at.exception, [e.value for e in at.exception]
    assert any(" in top " in m.value for m in at.markdown)
    table = at.dataframe[0].value
    assert "risk_score" not in table.columns


def test_the_day_picked_on_the_queue_carries_to_the_investigation():
    at = AppTest.from_file(str(ROOT / "views/account_investigation.py"), default_timeout=60)
    at.session_state["day"] = DAYS[1]
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    assert at.sidebar.selectbox[0].value == DAYS[1]
