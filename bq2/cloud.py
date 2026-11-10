"""BQ2 compare step of the pipeline: bank view vs shared view, as an AWS Batch job.

The pipeline's BQ2Score map has already scored each day twice over, once per bank
(``bq2-bank-<bank>-<day>``) and once on the shared graph (``bq2-shared-<day>``), and each
run left a receipt under ``<scoring gold>/executions/``. This job:

1. reads every receipt and checks it completed with the day and bank it should have;
2. downloads each run's ``transfers.parquet`` and checks its sha256 against the receipt;
3. downloads the day's curated transfers and transfer labels;
4. runs ``bq2.sharing.compare_day`` for every day, then ``summarise``;
5. uploads ``comparison_by_day.csv`` and then ``comparison.csv`` to the sharing prefix that
   the Athena table ``sharing_files`` reads.

It replaces ``runs/submit_batch.sh`` + ``runs/fetch_runs.py`` + ``sharing compare`` run by
hand. Labels are read here, after scoring, never by the scoring job (ADR-0004): this job
runs under its own role, which can read labels, and the scoring role still cannot.

Environment (set by the Batch job definition):
  CURATED_URI     s3://<curated>/<version prefix>
  EXECUTIONS_URI  s3://<gold>/<version prefix>/scoring/executions
  SHARING_URI     s3://<gold>/<version prefix>/sharing
  BQ2_DATES       comma-separated days, e.g. 2022-09-15,2022-09-16
  BQ2_BANKS       comma-separated bank ids
  BQ2_RATIO, BQ2_FLOOR, BQ2_MODEL   review budget share, its floor, and the model label
  BQ2_WORK_DIR    scratch directory (optional)
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
from pathlib import Path

import pandas as pd

from . import sharing

log = logging.getLogger(__name__)


def location(uri: str) -> tuple[str, str]:
    m = re.fullmatch(r"s3://([^/]+)/?(.*)", uri.rstrip("/"))
    if not m:
        raise ValueError(f"Not an s3:// URI: {uri}")
    return m.group(1), m.group(2)


def shared_id(day: str) -> str:
    return f"bq2-shared-{day}"


def bank_id(bank: str, day: str) -> str:
    return f"bq2-bank-{bank}-{day}"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def receipt(s3, executions_uri: str, execution_id: str, day: str, bank: str) -> dict:
    """One run's receipt, refused unless it completed for this day and this view."""
    b, p = location(executions_uri)
    r = json.loads(s3.get_object(Bucket=b, Key=f"{p}/{execution_id}.json")["Body"].read())
    opts = r.get("options", {})
    if r.get("status") != "completed" or opts.get("start") != day or (opts.get("bank") or "") != bank:
        raise ValueError(f"{execution_id}: unexpected receipt (status={r.get('status')}, options={opts})")
    return r


def fetch_scores(s3, r: dict, dest: Path) -> Path:
    """Download a run's transfers.parquet and check it against the receipt's sha256."""
    art = r["artifacts"]["transfers.parquet"]
    b, k = location(art["uri"])
    dest.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(b, k, str(dest))
    if sha256(dest) != art["sha256"]:
        raise ValueError(f"{art['uri']}: sha256 does not match its receipt")
    return dest


def fetch_prefix(s3, uri: str, dest: Path) -> int:
    """Download every Parquet file under an S3 prefix; returns how many."""
    b, p = location(uri)
    dest.mkdir(parents=True, exist_ok=True)
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=p + "/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                s3.download_file(b, obj["Key"], str(dest / Path(obj["Key"]).name))
                n += 1
    return n


def run(s3, *, curated_uri: str, executions_uri: str, sharing_uri: str, dates: list[str],
        banks: list[str], ratio: float, floor: int, model: str, work_root: str | None = None) -> pd.DataFrame:
    if not dates or not banks:
        raise ValueError("BQ2 needs at least one day and one bank")
    curated_uri = curated_uri.rstrip("/")
    with tempfile.TemporaryDirectory(prefix="bq2-", dir=work_root) as tmp:
        root = Path(tmp)
        frames = []
        for day in dates:
            log.info("Comparing %d banks on %s", len(banks), day)
            for table in ("transfers", "transfer_labels"):
                if not fetch_prefix(s3, f"{curated_uri}/{table}/date={day}", root / "curated" / table / f"date={day}"):
                    raise ValueError(f"No curated {table} for {day}")
            transfers, labels = sharing.load_day(str(root / "curated"), day)
            shared = sharing.load_scores(str(fetch_scores(
                s3, receipt(s3, executions_uri, shared_id(day), day, ""), root / "runs" / shared_id(day) / "t.parquet")))
            per_bank = {}
            for bank in banks:
                eid = bank_id(bank, day)
                path = fetch_scores(s3, receipt(s3, executions_uri, eid, day, bank), root / "runs" / eid / "t.parquet")
                per_bank[bank] = sharing.load_scores(str(path))
            frames.append(sharing.compare_day(transfers, labels, shared, per_bank,
                                              ratio=ratio, floor=floor, model=model, day=day))
        by_day = pd.concat(frames, ignore_index=True)
        summary = sharing.summarise(by_day)
        out_b, out_p = location(sharing_uri)
        # By-day first: comparison.csv is what the dashboard reads, so it lands last.
        for name, frame in (("comparison_by_day.csv", by_day), ("comparison.csv", summary)):
            path = root / name
            frame.to_csv(path, index=False)
            s3.upload_file(str(path), out_b, f"{out_p}/{name}", ExtraArgs={"ContentType": "text/csv"})
        log.info("Published s3://%s/%s/comparison.csv", out_b, out_p)
        return summary


def _list(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def main() -> None:
    import boto3
    from botocore.config import Config
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    env = os.environ
    summary = run(boto3.client("s3", config=Config(retries={"mode": "standard", "max_attempts": 5})),
                  curated_uri=env["CURATED_URI"], executions_uri=env["EXECUTIONS_URI"],
                  sharing_uri=env["SHARING_URI"], dates=_list(env["BQ2_DATES"]), banks=_list(env["BQ2_BANKS"]),
                  ratio=float(env.get("BQ2_RATIO", "0.0011")), floor=int(env.get("BQ2_FLOOR", "10")),
                  model=env.get("BQ2_MODEL", "gin"), work_root=env.get("BQ2_WORK_DIR"))
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
