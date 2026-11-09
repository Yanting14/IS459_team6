"""Tests for the BQ2 comparison on a hand-built day whose answers are worked out below.

Banks A, B and C. Transfers on the day:

    t1  A/a1 -> B/b1   laundering
    t2  B/b1 -> C/c1   laundering
    t3  A/a2 -> A/a3
    t4  A/a3 -> A/a3   self-transfer, laundering
    t5  C/c1 -> C/c1   self-transfer

Bank A's cohort is a1, a2, a3 (a3 qualifies through t3, not its self-transfer). Its
positives are a1 (t1) and a3 (t4: a laundering self-transfer still marks an eligible
account, as in scoring.pipeline.evaluate).
"""
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

from bq2 import sharing

ROOT = Path(__file__).resolve().parents[1]


def day():
    transfers = pd.DataFrame(
        [("t1", "A", "a1", "B", "b1"), ("t2", "B", "b1", "C", "c1"), ("t3", "A", "a2", "A", "a3"),
         ("t4", "A", "a3", "A", "a3"), ("t5", "C", "c1", "C", "c1")],
        columns=["transfer_id", "sender_bank_id", "sender_account_number",
                 "receiver_bank_id", "receiver_account_number"])
    labels = pd.DataFrame({"transfer_id": ["t1", "t2", "t3", "t4", "t5"],
                           "laundering_mark": [1, 1, 0, 1, 0]})
    return transfers, labels


def scores(**values):
    return pd.DataFrame({"transfer_id": list(values), "risk_score": list(values.values())})


SHARED = scores(t1=.9, t2=.8, t3=.1, t4=.2, t5=.5)
# Bank A's own graph: only t1, t3, t4 are visible, and t1 now looks harmless.
BANK_A = scores(t1=.3, t3=.6, t4=.2)


def accounts(frame):
    return set(map(tuple, frame[["bank_id", "account_id"]].to_numpy()))


def test_a_self_transfer_is_one_leg():
    transfers, _ = day()
    legs = sharing.legs(transfers)
    assert len(legs) == 2 * 3 + 2
    assert legs[legs.transfer_id == "t4"].shape[0] == 1


def test_cohort_ignores_self_transfers_and_other_banks():
    legs = sharing.legs(day()[0])
    assert accounts(sharing.cohort(legs, "A")) == {("A", "a1"), ("A", "a2"), ("A", "a3")}
    assert accounts(sharing.cohort(legs, "C")) == {("C", "c1")}


def test_positives_include_a_laundering_self_transfer_of_an_eligible_account():
    transfers, labels = day()
    legs = sharing.legs(transfers)
    eligible = sharing.cohort(legs, "A")
    assert accounts(sharing.positives(legs, labels, eligible)) == {("A", "a1"), ("A", "a3")}


def test_account_score_is_the_maximum_incident_score_including_self_transfers():
    legs = sharing.legs(day()[0])
    got = sharing.account_scores(SHARED, legs).set_index(["bank_id", "account_id"]).risk_score
    assert got[("B", "b1")] == pytest.approx(.9)
    assert got[("C", "c1")] == pytest.approx(.8)
    assert got[("A", "a3")] == pytest.approx(.2)


def test_review_breaks_ties_on_account_identity():
    legs = sharing.legs(day()[0])
    top = sharing.review(sharing.account_scores(BANK_A, legs), sharing.cohort(legs, "A"), 1)
    assert accounts(top) == {("A", "a2")}  # a2 and a3 tie at 0.6


def test_review_refuses_eligible_accounts_without_scores():
    legs = sharing.legs(day()[0])
    with pytest.raises(ValueError, match="no scored transfer"):
        sharing.review(sharing.account_scores(scores(t1=.5), legs), sharing.cohort(legs, "A"), 1)


def test_k_is_a_share_of_the_cohort_with_a_floor():
    assert sharing.k_for(100_000, 0.0011) == 110
    assert sharing.k_for(500, 0.0011) == 10
    with pytest.raises(ValueError):
        sharing.k_for(10, 0)


def test_compare_day_scores_both_views_on_the_same_cohort_and_budget():
    transfers, labels = day()
    out = sharing.compare_day(transfers, labels, SHARED, {"A": BANK_A}, ratio=0.1, floor=1,
                              day="2022-09-15").set_index("view")
    assert out.loc["bank", "true_positives"] == 0
    assert out.loc["shared", "true_positives"] == 1
    for column in ("k", "eligible", "positive_accounts"):
        assert out.loc["bank", column] == out.loc["shared", column]
    assert out.loc["shared", "recall_at_k"] == pytest.approx(.5)


def test_summarise_sums_days_then_recomputes_rates():
    transfers, labels = day()
    one = sharing.compare_day(transfers, labels, SHARED, {"A": BANK_A}, ratio=0.1, floor=1, day="d1")
    both = sharing.summarise(pd.concat([one, one.assign(date="d2")]))
    assert list(both.columns) == sharing.COMPARISON_COLUMNS
    shared = both.set_index("view").loc["shared"]
    assert (shared.true_positives, shared.positive_accounts, shared.reviewed) == (2, 4, 2)
    assert shared.recall_at_k == pytest.approx(.5)


def test_choose_banks_keeps_banks_with_enough_positives():
    transfers, labels = day()
    table = sharing.choose_banks(transfers, labels, n=5, min_positives=2)
    assert list(table.bank_id) == ["A"]
    assert table.loc[0, "positive_accounts"] == 2 and bool(table.loc[0, "selected"])


def test_check_shared_ranking_matches_a_published_queue():
    transfers, _ = day()
    published = pd.DataFrame({"bank_id": ["A", "B"], "account_id": ["a1", "b1"]})
    assert sharing.check_shared_ranking(transfers, SHARED, published, 2)["identical_set"]


def write_curated(root: Path, date: str):
    transfers, labels = day()
    for name, frame in (("transfers", transfers), ("transfer_labels", labels)):
        folder = root / "curated" / name / f"date={date}"
        folder.mkdir(parents=True)
        frame.to_parquet(folder / "part-0.parquet")


def test_cli_writes_a_comparison_the_dashboard_can_read(tmp_path, capsys):
    date = "2022-09-15"
    write_curated(tmp_path, date)
    SHARED.to_parquet(tmp_path / "shared.parquet")
    BANK_A.to_parquet(tmp_path / "bank_A.parquet")
    manifest = tmp_path / "runs.json"
    manifest.write_text(json.dumps({date: {"shared": str(tmp_path / "shared.parquet"),
                                           "banks": {"A": str(tmp_path / "bank_A.parquet")}}}))
    sharing.main(["compare", "--curated", str(tmp_path / "curated"), "--manifest", str(manifest),
                  "--ratio", "0.1", "--floor", "1", "--out-dir", str(tmp_path / "sharing")])
    comparison = pd.read_csv(tmp_path / "sharing" / "comparison.csv", dtype={"bank_id": str})
    assert len(comparison) == 2 and (tmp_path / "sharing" / "comparison_by_day.csv").exists()

    sys.path.insert(0, str(ROOT / "dashboard"))
    try:
        from dashlib import analysis
    finally:
        sys.path.remove(str(ROOT / "dashboard"))
    uplift = analysis.uplift_table(comparison)
    assert uplift.loc[0, "extra_true_positives"] == 1


def test_sfn_inputs_put_the_shared_run_first(tmp_path, capsys):
    pd.DataFrame({"bank_id": ["010", "020"], "selected": [True, False]}).to_csv(tmp_path / "b.csv", index=False)
    sharing.main(["sfn-inputs", "--banks", str(tmp_path / "b.csv"), "--dates", "2022-09-15"])
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [line["scoring"]["bank"] for line in lines] == ["", "010"]
    assert lines[0]["scoring"]["cutoff"] == "2022-09-16"
