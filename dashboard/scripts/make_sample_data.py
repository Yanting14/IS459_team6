"""Generate a small SYNTHETIC dataset in the same formats the real pipeline publishes.

It exists so the dashboard can be developed and demoed without AWS access. Nothing here
is real IBM AML data and none of the numbers mean anything about the real model.

    python scripts/make_sample_data.py            # writes ../sample_data

Layout written (mirrors the real zones):
    sample_data/queue/     date=<day>/accounts.csv, ranking.json, _SUCCESS   (the BQ1 queue)
    sample_data/metrics/   models.json, rules.json, rules_tuned.json          (synthetic numbers)
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
DAYS = pd.to_datetime(["2022-09-14", "2022-09-15", "2022-09-16"])
SCORED_DAYS = ["2022-09-15", "2022-09-16"]
MODEL = "sample-bq1"
# The reason features, all ones the account's own bank can see; "high" is the unusual side.
REASON_FEATURES = ["fan_in", "fan_out", "cp_banks", "usd_out", "max_usd"]
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

    # ---- BQ1 queues, one per scored day: account-day features -> score -> ranking + reasons
    queue_root = out / "queue"
    metrics = {"gbt": [], "lr": [], "rules": [], "rules_tuned": []}
    for scored_day in SCORED_DAYS:
        cutoff = str((pd.Timestamp(scored_day) + pd.Timedelta(days=1)).date())
        day_rows = allx[(allx.date == scored_day)
                        & ~((allx.sender_bank_id == allx.receiver_bank_id)
                            & (allx.sender_account_number == allx.receiver_account_number))]
        out_side = day_rows.assign(bank_id=day_rows.sender_bank_id, account_id=day_rows.sender_account_number,
                                   cp=day_rows.receiver_bank_id + ":" + day_rows.receiver_account_number,
                                   cp_bank=day_rows.receiver_bank_id, out=day_rows.amount_paid)
        in_side = day_rows.assign(bank_id=day_rows.receiver_bank_id, account_id=day_rows.receiver_account_number,
                                  cp=day_rows.sender_bank_id + ":" + day_rows.sender_account_number,
                                  cp_bank=day_rows.sender_bank_id, out=0.0)
        inc = pd.concat([out_side, in_side])
        keys = ["bank_id", "account_id"]
        feats = inc.groupby(keys).agg(
            cp_banks=("cp_bank", "nunique"), usd_out=("out", "sum"), max_usd=("amount_paid", "max"),
            laundering=("laundering_mark", "max")).reset_index()
        feats = feats.merge(out_side.groupby(keys).cp.nunique().rename("fan_out").reset_index(), on=keys, how="left")
        feats = feats.merge(in_side.groupby(keys).cp.nunique().rename("fan_in").reset_index(), on=keys, how="left")
        feats[["fan_in", "fan_out"]] = feats[["fan_in", "fan_out"]].fillna(0).astype(int)
        feats["usd_out"] = feats["usd_out"].round(2)
        eligible = len(feats)
        positive = feats.laundering.to_numpy() == 1

        # SYNTHETIC scores: launderers tend to score high, a few clean accounts too
        noisy_clean = rng.random(eligible) < 0.03
        feats["score"] = np.where(positive, rng.beta(5, 2, eligible),
                                  np.where(noisy_clean, rng.beta(4, 3, eligible), rng.beta(1, 25, eligible)))
        feats = feats.sort_values(["score", "bank_id", "account_id"], ascending=[False, True, True]).reset_index(drop=True)
        top = feats.head(K).copy()

        # reasons as the score job writes them: the account's three most extreme features that day
        shares = pd.DataFrame({f: feats[f].rank(method="max", ascending=False) / eligible for f in REASON_FEATURES})

        def reasons(i):
            picked = sorted((shares.at[i, f], f) for f in REASON_FEATURES)[:3]
            out_ = []
            for share, f in picked:
                v = float(feats.at[i, f])
                shown = str(int(v)) if v.is_integer() else f"{v:.2f}"
                out_.append(f"{f} in top {share * 100:.2g}% ({shown})")
            return out_

        rows = [reasons(i) for i in top.index]
        queue = pd.DataFrame({
            "rank": np.arange(1, len(top) + 1), "bank_id": top.bank_id, "account_id": top.account_id,
            "score": top.score.astype(float), "model_version": MODEL, "window_start": scored_day, "cutoff": cutoff,
            "reason_1": [r[0] for r in rows], "reason_2": [r[1] for r in rows], "reason_3": [r[2] for r in rows]})
        day_dir = queue_root / f"date={scored_day}"
        day_dir.mkdir(parents=True, exist_ok=True)
        queue.to_csv(day_dir / "accounts.csv", index=False)
        (day_dir / "ranking.json").write_text(json.dumps({
            "k": K, "date": scored_day, "window_start": scored_day, "cutoff": cutoff, "eligible": eligible,
            "reviewed": len(top), "model": f"sample://models/bq1/{MODEL}", "model_version": MODEL,
            "reasons": "SYNTHETIC. Reasons are descriptive flags, not model attributions: the account's most "
                       "extreme features that day among the eligible accounts.",
            "score": "SYNTHETIC. score is a ranking value, not a probability.",
            "order": "score descending, then bank_id, then account_number"}, indent=2))
        (day_dir / "_SUCCESS").write_text("")

        # SYNTHETIC evaluation, measured on the sample labels: the queue, and three weaker rankings
        n_pos = int(positive.sum())

        def measured(order_by):
            tp = int(feats.loc[order_by.head(K).index, "laundering"].sum())
            return {"date": scored_day, "eligible": eligible, "reviewed": min(K, eligible), "positives": n_pos,
                    "tp": tp, "precision_at_k": tp / min(K, eligible), "recall_at_k": tp / n_pos}

        metrics["gbt"].append(measured(feats))
        for name, noise in (("lr", 0.25), ("rules_tuned", 0.45), ("rules", 0.7)):
            alt = feats.score + rng.normal(0, noise, eligible)
            metrics[name].append(measured(feats.loc[alt.sort_values(ascending=False).index]))

    note = "SYNTHETIC sample data from scripts/make_sample_data.py; the numbers mean nothing."
    metrics_dir = out / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    (metrics_dir / "models.json").write_text(json.dumps({
        "note": note, "k": K, "winner": "gbt", "runner_up": "lr",
        "results": {"gbt": {"test": metrics["gbt"]}, "lr": {"test": metrics["lr"]}}}, indent=2))
    for name, file in (("rules", "rules.json"), ("rules_tuned", "rules_tuned.json")):
        (metrics_dir / file).write_text(json.dumps({"note": note, "k": K, "test": metrics[name]}, indent=2))
    tp, n_pos = metrics["gbt"][0]["tp"], metrics["gbt"][0]["positives"]

    # ---- BQ2 comparison: SYNTHETIC numbers in the columns of bq2/runs/results/comparison.csv
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
                         "precision_at_k": t / k_b, "recall_at_k": t / pos_b, "model": "gin"})
    pd.DataFrame(rows).to_csv(share / "comparison.csv", index=False)
    print(f"wrote {out}  ({len(allx):,} transfers, {len(ring_out):,} ring transfers, "
          f"{N_RINGS} rings, queues of {K} for {', '.join(SCORED_DAYS)}, "
          f"precision@K {tp / K:.2f} on the first day)")


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "sample_data"
    main(target)
