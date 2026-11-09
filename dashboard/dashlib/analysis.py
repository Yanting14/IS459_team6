"""Pure functions (no Streamlit, no I/O) so they can be tested on their own."""
from __future__ import annotations

import pandas as pd

RING_COLUMNS = {"ring_id", "pattern_type", "sender_bank_id", "receiver_bank_id"}


def ring_visibility(rings: pd.DataFrame) -> pd.DataFrame:
    """For each laundering ring, how much of it can the best-placed single bank see?

    A bank sees a transfer when it is the sender's bank or the receiver's bank (the
    glossary's *bank view*). For every ring this finds the bank that is party to the most
    of the ring's transfers. ``fully_visible`` means one bank is party to all of them.

    This is a ceiling on what a single bank could notice, not a detection result: a bank
    party to a transfer still cannot see what the other side's account does elsewhere.
    """
    missing = RING_COLUMNS - set(rings.columns)
    if missing:
        raise ValueError(f"rings table is missing columns: {sorted(missing)}")
    r = rings[["ring_id", "pattern_type", "sender_bank_id", "receiver_bank_id"]].copy()
    r["transfer"] = range(len(r))  # the patterns file gives rows no ID of their own
    sender = r.rename(columns={"sender_bank_id": "bank"})[["ring_id", "pattern_type", "transfer", "bank"]]
    receiver = r.rename(columns={"receiver_bank_id": "bank"})[["ring_id", "pattern_type", "transfer", "bank"]]
    # a transfer inside one bank is seen by that bank once, not twice
    seen = pd.concat([sender, receiver]).drop_duplicates(["ring_id", "transfer", "bank"])
    per_bank = seen.groupby(["ring_id", "pattern_type", "bank"]).size().rename("seen").reset_index()
    best = (
        per_bank.sort_values(["ring_id", "seen", "bank"], ascending=[True, False, True])
        .drop_duplicates("ring_id")
        .rename(columns={"bank": "best_bank"})
    )
    sizes = r.groupby("ring_id").size().rename("transfers").reset_index()
    banks = seen.groupby("ring_id")["bank"].nunique().rename("banks_involved").reset_index()
    out = best.merge(sizes, on="ring_id").merge(banks, on="ring_id")
    out["best_bank_share"] = out["seen"] / out["transfers"]
    out["fully_visible"] = out["seen"] == out["transfers"]
    return out[["ring_id", "pattern_type", "transfers", "banks_involved", "best_bank",
                "seen", "best_bank_share", "fully_visible"]]


def visibility_summary(rings: pd.DataFrame, per_ring: pd.DataFrame) -> dict:
    cross = (rings["sender_bank_id"] != rings["receiver_bank_id"]).mean() if len(rings) else float("nan")
    return {
        "rings": int(len(per_ring)),
        "transfers": int(len(rings)),
        "share_invisible": float((~per_ring["fully_visible"]).mean()) if len(per_ring) else float("nan"),
        "avg_best_share": float(per_ring["best_bank_share"].mean()) if len(per_ring) else float("nan"),
        "share_cross_bank_transfers": float(cross),
        "median_banks_per_ring": float(per_ring["banks_involved"].median()) if len(per_ring) else float("nan"),
    }


def by_pattern(per_ring: pd.DataFrame) -> pd.DataFrame:
    g = per_ring.groupby("pattern_type")
    out = pd.DataFrame({
        "rings": g.size(),
        "not_fully_visible": g["fully_visible"].apply(lambda s: float((~s).mean())),
        "avg_best_share": g["best_bank_share"].mean(),
    }).reset_index()
    return out.sort_values("not_fully_visible", ascending=False).reset_index(drop=True)


def apply_bank_view(transfers: pd.DataFrame, bank: str) -> pd.DataFrame:
    """Keep only the transfers one bank can see: it is the sender's or the receiver's bank."""
    return transfers[(transfers["sender_bank_id"] == bank) | (transfers["receiver_bank_id"] == bank)]


def uplift_table(comparison: pd.DataFrame) -> pd.DataFrame:
    """One row per bank: bank-view vs shared-view metrics side by side."""
    need = {"bank_id", "view", "true_positives", "positive_accounts", "precision_at_k", "recall_at_k"}
    missing = need - set(comparison.columns)
    if missing:
        raise ValueError(f"comparison.csv is missing columns: {sorted(missing)}")
    wide = comparison.pivot(index="bank_id", columns="view",
                            values=["true_positives", "positive_accounts", "precision_at_k", "recall_at_k"])
    wide.columns = [f"{metric}_{view}" for metric, view in wide.columns]
    wide = wide.reset_index()
    wide["extra_true_positives"] = wide["true_positives_shared"] - wide["true_positives_bank"]
    wide["recall_uplift"] = wide["recall_at_k_shared"] - wide["recall_at_k_bank"]
    return wide.sort_values("extra_true_positives", ascending=False).reset_index(drop=True)


MODEL_ROWS = [
    ("models", "gbt", "Gradient-boosted trees"),
    ("models", "lr", "Logistic regression"),
    ("rules", None, "Rules (label-free)"),
    ("rules_tuned", None, "Best-case rules (weights tuned on validation labels)"),
]


def model_comparison(metrics: dict, day: str) -> pd.DataFrame:
    """One row per BQ1 approach for one test day: Precision@K and Recall@K as each job recorded them.

    ``metrics`` maps models / rules / rules_tuned to the jobs' metrics.json (None if absent).
    The models file holds LR and GBT under ``results``; the rules files hold ``test`` directly.
    """
    rows = []
    for source, family, label in MODEL_ROWS:
        doc = metrics.get(source)
        if not doc:
            continue
        block = doc.get("results", {}).get(family) if family else doc
        if not block:
            continue
        hit = [d for d in block.get("test", []) if d.get("date") == day]
        if not hit:
            continue
        d = hit[0]
        rows.append({"approach": label, "k": doc.get("k"), "reviewed": d["reviewed"],
                     "eligible": d["eligible"], "positives": d["positives"], "true_positives": d["tp"],
                     "precision_at_k": d["precision_at_k"], "recall_at_k": d["recall_at_k"]})
    return pd.DataFrame(rows, columns=["approach", "k", "reviewed", "eligible", "positives",
                                       "true_positives", "precision_at_k", "recall_at_k"])
