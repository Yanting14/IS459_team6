"""BQ2: does a bank catch more laundering with the shared view than with its own view?

The frozen GIN scores one day's transfers twice: once on the shared graph (every bank's
transfers) and once on bank B's view (only transfers where B is the sender's or the
receiver's bank, filtered before the graph is built; ADR-0004). This module turns those
published transfer scores into the same review decision for both views, and compares
them on bank B's own accounts at the same review budget K.

It reimplements the ranking rule of ``scoring.pipeline.rank`` on published outputs, so a
single shared run per day serves every bank:

* an account's score is the maximum score of the transfers it sends or receives that
  day, self-transfers included;
* the cohort is the bank's accounts with at least one non-self transfer that day;
* the top K of the cohort by score are reviewed. Ties break on (bank_id, account_id),
  not on the saved node ID the pipeline uses, so a tied boundary can differ;
* a positive is a cohort account that sends or receives a transfer marked as laundering
  that day.

Because a bank sees every transfer of its own accounts, the cohort and the positives are
the same in both views; only the scores differ. ``compare_day`` checks this.

Run with ``python -m bq2.sharing --help`` from the repository root. Labels are read here,
after scoring, never by the scoring job.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import pandas as pd

COMPARISON_COLUMNS = ["bank_id", "view", "k", "reviewed", "eligible", "positive_accounts",
                      "true_positives", "precision_at_k", "recall_at_k", "model"]
ACCOUNT = ["bank_id", "account_id"]


# ----------------------------------------------------------------------------- core

def legs(transfers: pd.DataFrame) -> pd.DataFrame:
    """One row per (transfer, account it touches): the sender's leg and the receiver's leg.

    A self-transfer touches one account, so it yields one leg. ``is_self`` marks it.
    """
    t = transfers
    is_self = ((t.sender_bank_id == t.receiver_bank_id) &
               (t.sender_account_number == t.receiver_account_number))
    sender = pd.DataFrame({"transfer_id": t.transfer_id, "bank_id": t.sender_bank_id,
                           "account_id": t.sender_account_number, "is_self": is_self})
    receiver = pd.DataFrame({"transfer_id": t.transfer_id, "bank_id": t.receiver_bank_id,
                             "account_id": t.receiver_account_number, "is_self": is_self})
    return pd.concat([sender, receiver[~is_self]], ignore_index=True)


def cohort(day_legs: pd.DataFrame, bank: str | None) -> pd.DataFrame:
    """Accounts eligible for review: a non-self transfer that day, owned by ``bank`` if given."""
    c = day_legs[~day_legs.is_self]
    if bank is not None:
        c = c[c.bank_id == bank]
    return c[ACCOUNT].drop_duplicates().reset_index(drop=True)


def positives(day_legs: pd.DataFrame, labels: pd.DataFrame, eligible: pd.DataFrame) -> pd.DataFrame:
    """Eligible accounts that send or receive a laundering transfer that day."""
    bad = labels.loc[labels.laundering_mark == 1, ["transfer_id"]]
    touched = day_legs.merge(bad, on="transfer_id")[ACCOUNT].drop_duplicates()
    return eligible.merge(touched, on=ACCOUNT)


def account_scores(scores: pd.DataFrame, day_legs: pd.DataFrame) -> pd.DataFrame:
    """Maximum incident transfer score per account, over the transfers this view scored."""
    joined = day_legs.merge(scores[["transfer_id", "risk_score"]], on="transfer_id")
    return joined.groupby(ACCOUNT, as_index=False).risk_score.max()


def review(acct_scores: pd.DataFrame, eligible: pd.DataFrame, k: int) -> pd.DataFrame:
    """The top ``k`` eligible accounts by score, ties broken on the account identity."""
    if k <= 0:
        raise ValueError("Review budget must be positive")
    ranked = eligible.merge(acct_scores, on=ACCOUNT, how="left")
    missing = int(ranked.risk_score.isna().sum())
    if missing:
        raise ValueError(f"{missing} eligible accounts have no scored transfer in this view; "
                         "the scores and the curated day do not match")
    ranked = ranked.sort_values(["risk_score", "bank_id", "account_id"],
                                ascending=[False, True, True], kind="mergesort")
    out = ranked.head(k).reset_index(drop=True)
    out["rank"] = range(1, len(out) + 1)
    return out


def k_for(eligible: int, ratio: float, floor: int = 10) -> int:
    """Review budget for one bank: a fixed share of its eligible accounts, at least ``floor``."""
    if not 0 < ratio <= 1:
        raise ValueError("ratio must be in (0, 1]")
    return max(floor, math.ceil(eligible * ratio))


def evaluate(reviewed: pd.DataFrame, eligible: pd.DataFrame, pos: pd.DataFrame, k: int) -> dict:
    tp = len(reviewed[ACCOUNT].merge(pos, on=ACCOUNT))
    return dict(k=k, reviewed=len(reviewed), eligible=len(eligible), positive_accounts=len(pos),
                true_positives=tp,
                precision_at_k=tp / len(reviewed) if len(reviewed) else None,
                recall_at_k=tp / len(pos) if len(pos) else None)


def compare_day(transfers: pd.DataFrame, labels: pd.DataFrame, shared_scores: pd.DataFrame,
                bank_scores: dict[str, pd.DataFrame], ratio: float, floor: int = 10,
                model: str = "gin", day: str | None = None) -> pd.DataFrame:
    """Bank view vs shared view for each bank on one day, at the same K. Two rows per bank."""
    day_legs = legs(transfers)
    shared = account_scores(shared_scores, day_legs)
    rows = []
    for bank, scores in sorted(bank_scores.items()):
        eligible = cohort(day_legs, bank)
        if eligible.empty:
            raise ValueError(f"Bank {bank} has no eligible accounts on {day}")
        pos = positives(day_legs, labels, eligible)
        k = k_for(len(eligible), ratio, floor)
        for view, s in (("bank", account_scores(scores, day_legs)), ("shared", shared)):
            row = evaluate(review(s, eligible, k), eligible, pos, k)
            rows.append(dict(date=day, bank_id=bank, view=view, model=model, **row))
    out = pd.DataFrame(rows)
    # The two views must differ only in their scores, never in who is reviewed or who is positive.
    check = out.groupby("bank_id")[["k", "eligible", "positive_accounts"]].nunique()
    if (check > 1).any().any():
        raise ValueError("Bank and shared views disagree on K, the cohort or the positives")
    return out


def summarise(by_day: pd.DataFrame) -> pd.DataFrame:
    """One row per bank and view across days, in the dashboard's comparison.csv contract.

    Counts are summed over days (an account reviewed on two days counts twice), and
    precision and recall are recomputed from the sums.
    """
    sums = (by_day.groupby(["bank_id", "view", "model"], as_index=False)
            [["k", "reviewed", "eligible", "positive_accounts", "true_positives"]].sum())
    sums["precision_at_k"] = sums.true_positives / sums.reviewed
    sums["recall_at_k"] = (sums.true_positives / sums.positive_accounts).where(sums.positive_accounts > 0)
    return sums[COMPARISON_COLUMNS].sort_values(["bank_id", "view"]).reset_index(drop=True)


def choose_banks(transfers: pd.DataFrame, labels: pd.DataFrame, n: int = 8,
                 min_positives: int = 20) -> pd.DataFrame:
    """Banks with enough positive accounts on this day, largest first.

    Returns every qualifying bank with its eligible and positive counts; take the first
    ``n`` or document a different choice.
    """
    day_legs = legs(transfers)
    eligible = cohort(day_legs, None)
    pos = positives(day_legs, labels, eligible)
    table = (eligible.groupby("bank_id").size().rename("eligible").to_frame()
             .join(pos.groupby("bank_id").size().rename("positive_accounts"), how="left")
             .fillna({"positive_accounts": 0}).astype(int).reset_index())
    table = table[table.positive_accounts >= min_positives]
    table = table.sort_values(["positive_accounts", "bank_id"], ascending=[False, True])
    table["selected"] = range(len(table))
    table["selected"] = table.selected < n
    return table.reset_index(drop=True)


def check_shared_ranking(transfers: pd.DataFrame, shared_scores: pd.DataFrame,
                         published: pd.DataFrame, k: int) -> dict:
    """Does the reimplemented ranking reproduce the published shared queue at the same K?"""
    day_legs = legs(transfers)
    ours = review(account_scores(shared_scores, day_legs), cohort(day_legs, None), k)
    theirs = published[ACCOUNT].astype(str)
    overlap = len(ours[ACCOUNT].merge(theirs, on=ACCOUNT))
    return dict(k=k, published=len(theirs), reproduced=len(ours), overlap=overlap,
                identical_set=overlap == len(theirs) == len(ours))


# ----------------------------------------------------------------------------- I/O

def _read_parquet(pattern: str, columns: list[str], where: str = "", params=()) -> pd.DataFrame:
    import duckdb
    con = duckdb.connect()
    if pattern.startswith("s3://"):
        con.execute("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;")
        con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain)")
    sql = f"SELECT {', '.join(columns)} FROM read_parquet(?, hive_partitioning=false) {where}"
    return con.execute(sql, [pattern, *params]).fetchdf()


def load_day(curated: str, day: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """One day of curated transfers (endpoints only) and their transfer labels."""
    curated = curated.rstrip("/")
    transfers = _read_parquet(
        f"{curated}/transfers/date={day}/*.parquet",
        ["transfer_id", "sender_bank_id", "sender_account_number",
         "receiver_bank_id", "receiver_account_number"])
    labels = _read_parquet(f"{curated}/transfer_labels/date={day}/*.parquet",
                           ["transfer_id", "laundering_mark"])
    return transfers, labels


def load_scores(path: str) -> pd.DataFrame:
    return _read_parquet(path, ["transfer_id", "risk_score"])


def _cmd_choose_banks(a) -> None:
    tables = []
    for day in a.dates.split(","):
        transfers, labels = load_day(a.curated, day)
        tables.append(choose_banks(transfers, labels, n=10**9, min_positives=0).assign(date=day))
    every = pd.concat(tables)
    per_bank = (every.groupby("bank_id", as_index=False)[["eligible", "positive_accounts"]].min()
                .rename(columns={"eligible": "min_eligible", "positive_accounts": "min_positives"}))
    per_bank = per_bank[per_bank.min_positives >= a.min_positives]
    per_bank = per_bank.sort_values(["min_positives", "bank_id"], ascending=[False, True]).reset_index(drop=True)
    per_bank["selected"] = per_bank.index < a.n
    per_bank.to_csv(a.out, index=False)
    print(per_bank.head(max(a.n, 15)).to_string(index=False))


def _cmd_compare(a) -> None:
    manifest = json.loads(Path(a.manifest).read_text())
    frames = []
    for day, runs in sorted(manifest.items()):
        transfers, labels = load_day(a.curated, day)
        frames.append(compare_day(
            transfers, labels, load_scores(runs["shared"]),
            {bank: load_scores(path) for bank, path in runs["banks"].items()},
            ratio=a.ratio, floor=a.floor, model=a.model, day=day))
    by_day = pd.concat(frames, ignore_index=True)
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    by_day.to_csv(out / "comparison_by_day.csv", index=False)
    summarise(by_day).to_csv(out / "comparison.csv", index=False)
    print(summarise(by_day).to_string(index=False))


def _cmd_check_shared(a) -> None:
    """Reproduce a published shared queue and its all-bank Precision/Recall@K before trusting compare."""
    transfers, labels = load_day(a.curated, a.date)
    scores = load_scores(a.scores)
    published = pd.read_csv(a.accounts, dtype={"bank_id": str, "account_id": str})
    result = check_shared_ranking(transfers, scores, published, len(published))
    day_legs = legs(transfers)
    eligible = cohort(day_legs, None)
    reviewed = review(account_scores(scores, day_legs), eligible, len(published))
    result.update(evaluate(reviewed, eligible, positives(day_legs, labels, eligible), len(published)))
    print(json.dumps(result, indent=2, default=int))


def _cmd_sfn_inputs(a) -> None:
    """Step Functions inputs for the bank-view scoring runs (one per bank and day)."""
    banks = pd.read_csv(a.banks, dtype={"bank_id": str})
    banks = banks[banks.selected] if "selected" in banks else banks
    for day in a.dates.split(","):
        cutoff = (pd.Timestamp(day) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        for bank in [""] + list(banks.bank_id):
            print(json.dumps({"scoring": {"start": day, "cutoff": cutoff, "k": a.k, "bank": bank}}))


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("choose-banks", help="rank banks by positive accounts on every given day")
    c.add_argument("--curated", required=True, help="curated version prefix (local path or s3://)")
    c.add_argument("--dates", default="2022-09-15,2022-09-16")
    c.add_argument("--n", type=int, default=8)
    c.add_argument("--min-positives", type=int, default=20)
    c.add_argument("--out", default="banks.csv")
    c.set_defaults(run=_cmd_choose_banks)

    c = sub.add_parser("compare", help="bank view vs shared view -> comparison.csv")
    c.add_argument("--curated", required=True)
    c.add_argument("--manifest", required=True,
                   help='JSON: {"2022-09-15": {"shared": ".../transfers.parquet", '
                        '"banks": {"010": ".../transfers.parquet"}}}')
    c.add_argument("--ratio", type=float, default=0.0011,
                   help="review budget as a share of each bank's eligible accounts; the default "
                        "matches BQ1's K=1,000 of about 900,000 eligible accounts on 15 Sep")
    c.add_argument("--floor", type=int, default=10)
    c.add_argument("--model", default="gin")
    c.add_argument("--out-dir", default="sharing")
    c.set_defaults(run=_cmd_compare)

    c = sub.add_parser("check-shared", help="reproduce a published shared queue and its metrics")
    c.add_argument("--curated", required=True)
    c.add_argument("--date", required=True)
    c.add_argument("--scores", required=True, help="the shared run's transfers.parquet")
    c.add_argument("--accounts", required=True, help="the shared run's accounts.csv")
    c.set_defaults(run=_cmd_check_shared)

    c = sub.add_parser("sfn-inputs", help="print Step Functions inputs for the scoring runs")
    c.add_argument("--banks", required=True, help="banks.csv from choose-banks")
    c.add_argument("--dates", default="2022-09-15,2022-09-16")
    c.add_argument("--k", type=int, default=1000)
    c.set_defaults(run=_cmd_sfn_inputs)

    a = p.parse_args(argv)
    a.run(a)


if __name__ == "__main__":
    main()
