import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dashlib import analysis  # noqa: E402


def rings(rows):
    return pd.DataFrame(rows, columns=["ring_id", "pattern_type", "sender_bank_id", "receiver_bank_id"])


def test_fan_out_from_one_bank_is_fully_visible_to_that_bank():
    # bank A sends to B, C and D: A is party to all three transfers
    df = rings([(1, "FAN-OUT", "A", "B"), (1, "FAN-OUT", "A", "C"), (1, "FAN-OUT", "A", "D")])
    out = analysis.ring_visibility(df).iloc[0]
    assert out.best_bank == "A" and out.seen == 3 and out.transfers == 3
    assert bool(out.fully_visible) and out.best_bank_share == 1.0


def test_cycle_across_three_banks_is_not_fully_visible_to_any_single_bank():
    # A->B, B->C, C->A: every bank is party to exactly two of the three transfers
    df = rings([(1, "CYCLE", "A", "B"), (1, "CYCLE", "B", "C"), (1, "CYCLE", "C", "A")])
    out = analysis.ring_visibility(df).iloc[0]
    assert out.seen == 2 and out.transfers == 3 and not out.fully_visible
    assert out.best_bank_share == pytest.approx(2 / 3)
    assert out.banks_involved == 3


def test_transfer_inside_one_bank_counts_once():
    df = rings([(1, "RANDOM", "A", "A"), (1, "RANDOM", "A", "B")])
    out = analysis.ring_visibility(df).iloc[0]
    assert out.seen == 2 and out.fully_visible


def test_summary_and_pattern_breakdown():
    df = rings([(1, "CYCLE", "A", "B"), (1, "CYCLE", "B", "C"), (1, "CYCLE", "C", "A"),
                (2, "FAN-OUT", "A", "B"), (2, "FAN-OUT", "A", "C")])
    per_ring = analysis.ring_visibility(df)
    s = analysis.visibility_summary(df, per_ring)
    assert s["rings"] == 2 and s["share_invisible"] == 0.5
    assert s["share_cross_bank_transfers"] == 1.0
    pat = analysis.by_pattern(per_ring).set_index("pattern_type")
    assert pat.loc["CYCLE", "not_fully_visible"] == 1.0 and pat.loc["FAN-OUT", "not_fully_visible"] == 0.0


def test_missing_columns_are_reported():
    with pytest.raises(ValueError, match="missing columns"):
        analysis.ring_visibility(pd.DataFrame({"ring_id": [1]}))


def test_bank_view_keeps_only_transfers_touching_the_bank():
    t = pd.DataFrame({"sender_bank_id": ["A", "B", "C"], "receiver_bank_id": ["B", "C", "A"]})
    assert analysis.apply_bank_view(t, "A").index.tolist() == [0, 2]


def test_uplift_table():
    c = pd.DataFrame({
        "bank_id": ["1", "1"], "view": ["bank", "shared"], "true_positives": [3, 8],
        "positive_accounts": [10, 10], "precision_at_k": [0.1, 0.27], "recall_at_k": [0.3, 0.8]})
    u = analysis.uplift_table(c).iloc[0]
    assert u.extra_true_positives == 5 and u.recall_uplift == pytest.approx(0.5)
