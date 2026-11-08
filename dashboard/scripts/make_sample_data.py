"""Generate a small SYNTHETIC dataset in the same formats the real pipeline publishes.

It exists so the dashboard can be developed and demoed without AWS access. Nothing here
is real IBM AML data and none of the numbers mean anything about the real model.

    python scripts/make_sample_data.py            # writes ../sample_data

Layout written (mirrors the real zones):
    sample_data/queue/     accounts.csv, transfers.parquet, ranking.json, evaluation.json
    sample_data/curated/   transfers/date=*/, transfer_labels/date=*/, rings/
    sample_data/sharing/   comparison.csv   (synthetic numbers)
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SEED = 459
SHA = "sample"
DAYS = pd.to_datetime(["2022-09-12", "2022-09-13", "2022-09-14"])
SCORED_DAY = "2022-09-14"
K = 150
N_BANKS, ACCOUNTS_PER_BANK = 25, 70
N_BACKGROUND, N_RINGS = 36_000, 90
FORMATS = ["ACH", "Cash", "Cheque", "Credit Card", "Reinvestment", "Wire"]
CURRENCIES = ["US Dollar", "Euro", "UK Pound", "Yen", "Yuan", "Rupee"]
PATTERNS = ["FAN-OUT", "FAN-IN", "CYCLE", "GATHER-SCATTER", "SCATTER-GATHER", "STACK", "BIPARTITE", "RANDOM"]


def main(out: Path) -> None:
    rng = np.random.default_rng(SEED)
    if out.exists():
        shutil.rmtree(out)
    banks = [f"{i:03d}" for i in range(1, N_BANKS + 1)]
    accounts = [(b, f"8{bi:02X}{j:04X}") for bi, b in enumerate(banks) for j in range(ACCOUNTS_PER_BANK)]
    weights = rng.pareto(2.0, len(accounts)) + 1
    weights /= weights.sum()

    # ---- background transfers
    snd = rng.choice(len(accounts), N_BACKGROUND, p=weights)
    rcv = rng.choice(len(accounts), N_BACKGROUND, p=weights)
    selfish = rng.random(N_BACKGROUND) < 0.08
    rcv = np.where(selfish, snd, rcv)
    day = rng.choice(len(DAYS), N_BACKGROUND)
    minutes = rng.integers(0, 24 * 60, N_BACKGROUND)
    ts = DAYS[day] + pd.to_timedelta(minutes, unit="m")
    amount = np.round(rng.lognormal(6.5, 1.4, N_BACKGROUND), 2)
    cur = rng.choice(CURRENCIES, N_BACKGROUND, p=[0.5, 0.15, 0.1, 0.1, 0.1, 0.05])
    background = pd.DataFrame({
        "timestamp": ts,
        "sender_bank_id": [accounts[i][0] for i in snd], "sender_account_number": [accounts[i][1] for i in snd],
        "receiver_bank_id": [accounts[i][0] for i in rcv], "receiver_account_number": [accounts[i][1] for i in rcv],
        "amount_received": amount, "receiving_currency": cur, "amount_paid": amount, "payment_currency": cur,
        "payment_format": rng.choice(FORMATS, N_BACKGROUND), "laundering_mark": 0, "ring_id": -1, "pattern_type": "",
    })

    # ---- laundering rings
    ring_rows = []
    for ring_id in range(1, N_RINGS + 1):
        pattern = PATTERNS[(ring_id - 1) % len(PATTERNS)]
        n_banks = int(rng.choice([1, 2, 3, 4, 5, 6], p=[0.12, 0.28, 0.25, 0.15, 0.12, 0.08]))
        ring_banks = list(rng.choice(banks, n_banks, replace=False))

        def pick(i=0):
            b = ring_banks[int(rng.integers(0, len(ring_banks)))]
            bi = banks.index(b)
            return b, f"8{bi:02X}{int(rng.integers(0, ACCOUNTS_PER_BANK)):04X}"

        def nodes(n):
            seen = []
            while len(seen) < n:
                a = pick()
                if a not in seen:
                    seen.append(a)
            return seen

        edges: list[tuple] = []
        if pattern == "FAN-OUT":
            s, *t = nodes(int(rng.integers(4, 9)))
            edges = [(s, x) for x in t]
        elif pattern == "FAN-IN":
            *s, d = nodes(int(rng.integers(4, 9)))
            edges = [(x, d) for x in s]
        elif pattern == "CYCLE":
            c = nodes(int(rng.integers(3, 7)))
            edges = [(c[i], c[(i + 1) % len(c)]) for i in range(len(c))]
        elif pattern == "GATHER-SCATTER":
            n = nodes(7)
            edges = [(x, n[3]) for x in n[:3]] + [(n[3], x) for x in n[4:]]
        elif pattern == "SCATTER-GATHER":
            n = nodes(6)
            edges = [(n[0], x) for x in n[1:5]] + [(x, n[5]) for x in n[1:5]]
        elif pattern == "STACK":
            n = nodes(7)
            edges = [(a, b) for a in n[:2] for b in n[2:4]] + [(a, b) for a in n[2:4] for b in n[4:]]
        elif pattern == "BIPARTITE":
            n = nodes(6)
            edges = [(a, b) for a in n[:3] for b in n[3:]]
        else:  # RANDOM
            n = nodes(5)
            edges = [tuple(n[i] for i in rng.choice(5, 2, replace=False)) for _ in range(6)]
        t0 = DAYS[0] + pd.Timedelta(minutes=int(rng.integers(0, 2 * 24 * 60 + 12 * 60)))
        for e in edges:
            t0 += pd.Timedelta(minutes=int(rng.integers(5, 240)))
            t = min(t0, DAYS[2] + pd.Timedelta(hours=23, minutes=58))
            amt = round(float(rng.lognormal(9.0, 0.7)), 2)
            c = str(rng.choice(CURRENCIES[:3]))
            ring_rows.append({
                "timestamp": t, "sender_bank_id": e[0][0], "sender_account_number": e[0][1],
                "receiver_bank_id": e[1][0], "receiver_account_number": e[1][1],
                "amount_received": amt, "receiving_currency": c, "amount_paid": amt, "payment_currency": c,
                "payment_format": str(rng.choice(FORMATS)), "laundering_mark": 1,
                "ring_id": ring_id, "pattern_type": pattern,
            })
    rings = pd.DataFrame(ring_rows)

    # ---- one table, stable IDs (sorted by time, then insertion order)
    allx = pd.concat([background, rings], ignore_index=True)
    allx = allx.sort_values("timestamp", kind="stable").reset_index(drop=True)
    allx["source_row_id"] = np.arange(len(allx))
    allx["transfer_id"] = [f"{SHA}:{i}" for i in allx.source_row_id]
    allx["date"] = allx.timestamp.dt.strftime("%Y-%m-%d")
    allx["source_file"], allx["source_sha256"] = "sample.csv", SHA

    # ---- curated zone, hive-partitioned by date like the Spark output
    cur_dir = out / "curated"
    tcols = ["timestamp", "sender_bank_id", "sender_account_number", "receiver_bank_id",
             "receiver_account_number", "amount_received", "receiving_currency", "amount_paid",
             "payment_currency", "payment_format", "transfer_id", "source_file", "source_sha256", "source_row_id"]
    lcols = ["transfer_id", "source_file", "source_sha256", "source_row_id", "laundering_mark"]
    for d, g in allx.groupby("date"):
        for name, cols in (("transfers", tcols), ("transfer_labels", lcols)):
            p = cur_dir / name / f"date={d}"
            p.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pandas(g[cols], preserve_index=False), p / "part-0.parquet")
    ring_out = allx[allx.ring_id > 0][[
        "ring_id", "pattern_type", "timestamp", "sender_bank_id", "sender_account_number",
        "receiver_bank_id", "receiver_account_number", "amount_received", "receiving_currency",
        "amount_paid", "payment_currency", "payment_format", "laundering_mark"]]
    (cur_dir / "rings").mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(ring_out, preserve_index=False), cur_dir / "rings" / "part-0.parquet")

    # ---- scoring run for the scored day: edge scores -> account scores -> ranking
    day_rows = allx[allx.date == SCORED_DAY].copy()
    laundering = day_rows.laundering_mark.to_numpy() == 1
    noisy_clean = rng.random(len(day_rows)) < 0.03  # a few false alarms, like any model
    score = np.where(laundering, rng.beta(5, 2, len(day_rows)),
                     np.where(noisy_clean, rng.beta(4, 3, len(day_rows)), rng.beta(1, 25, len(day_rows))))
    day_rows["risk_score"] = score.astype("float32")
    queue_dir = out / "queue"
    queue_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "transfer_id": day_rows.transfer_id, "risk_score": day_rows.risk_score,
        "model_version": ["sample-gin"] * len(day_rows), "cutoff": ["2022-09-15"] * len(day_rows)}),
        queue_dir / "transfers.parquet")

    inc = pd.concat([
        day_rows.assign(bank_id=day_rows.sender_bank_id, account_id=day_rows.sender_account_number),
        day_rows.assign(bank_id=day_rows.receiver_bank_id, account_id=day_rows.receiver_account_number),
    ])
    inc["is_self"] = (inc.sender_bank_id == inc.receiver_bank_id) & (inc.sender_account_number == inc.receiver_account_number)
    eligible = inc[~inc.is_self][["bank_id", "account_id"]].drop_duplicates()
    best = (inc.sort_values(["risk_score", "source_row_id"], ascending=[False, True])
            .drop_duplicates(["bank_id", "account_id"]))
    acc = eligible.merge(best[["bank_id", "account_id", "risk_score", "transfer_id"]], on=["bank_id", "account_id"])
    acc = acc.sort_values(["risk_score", "bank_id", "account_id"], ascending=[False, True, True]).reset_index(drop=True)
    acc["node_id"] = acc.index  # stand-in for the model's node map
    top = acc.head(K).copy()
    queue = pd.DataFrame({
        "bank_id": top.bank_id, "account_id": top.account_id, "node_id": top.node_id,
        "score": top.risk_score.astype(float), "rank": np.arange(1, len(top) + 1),
        "supporting_transfer_id": top.transfer_id, "model_version": "sample-gin",
        "window_start": SCORED_DAY, "cutoff": "2022-09-15"})
    queue.to_csv(queue_dir / "accounts.csv", index=False)

    pos_tx = inc[(inc.laundering_mark == 1) & (~inc.is_self)][["bank_id", "account_id"]].drop_duplicates()
    positives = eligible.merge(pos_tx, on=["bank_id", "account_id"])
    tp = len(top.merge(positives, on=["bank_id", "account_id"]))
    (queue_dir / "ranking.json").write_text(json.dumps({
        "model_version": "sample-gin", "start": SCORED_DAY, "cutoff": "2022-09-15", "daily": True,
        "requested_k": K, "returned": len(top), "eligible_accounts": len(eligible), "cohort_bank": None,
        "aggregation": "maximum incident transfer score, including self-transfers",
        "eligibility": "non-self transfer that day"}, indent=2))
    (queue_dir / "evaluation.json").write_text(json.dumps({
        "model_version": "sample-gin", "start": SCORED_DAY, "cutoff": "2022-09-15", "daily": True, "k": K,
        "reviewed": len(top), "eligible": len(eligible), "positive_accounts": len(positives),
        "true_positives": tp, "precision_at_k": tp / len(top), "recall_at_k": tp / len(positives)}, indent=2))

    # ---- BQ2 comparison: SYNTHETIC numbers in the format the real experiment should deliver
    share = out / "sharing"
    share.mkdir(parents=True, exist_ok=True)
    rows = []
    for b in banks[:10]:
        pos_b = int(rng.integers(12, 40))
        k_b = 30
        tp_shared = int(pos_b * rng.uniform(0.45, 0.8))
        tp_bank = int(tp_shared * rng.uniform(0.4, 0.75))
        for view, t in (("bank", tp_bank), ("shared", tp_shared)):
            rows.append({"bank_id": b, "view": view, "k": k_b, "reviewed": k_b, "eligible": 4000,
                         "positive_accounts": pos_b, "true_positives": t,
                         "precision_at_k": t / k_b, "recall_at_k": t / pos_b})
    pd.DataFrame(rows).to_csv(share / "comparison.csv", index=False)
    print(f"wrote {out}  ({len(allx):,} transfers, {len(ring_out):,} ring transfers, "
          f"{N_RINGS} rings, queue of {len(top)}, precision@K {tp / len(top):.2f})")


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "sample_data"
    main(target)
