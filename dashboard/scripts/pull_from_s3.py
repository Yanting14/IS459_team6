"""Download the files the dashboard needs from S3 into a local folder.

    python scripts/pull_from_s3.py --dry-run     # list what would be copied and how big it is
    python scripts/pull_from_s3.py               # copy it into ./data

Then run:  DASH_SOURCE=local DASH_ROOT=./data streamlit run app.py

What it copies
  data/queue/    accounts.csv, transfers.parquet, ranking.json -- the exact files named by the
                 scoring run's gold receipt (a reader must not glob the attempt folders: failed
                 attempts can leave unreferenced files). Each file's SHA-256 is checked.
  data/curated/  transfers/ and transfer_labels/ for the date range, and rings/ in full.

It uses your normal AWS credentials (set AWS_PROFILE if you use a named profile).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

DEFAULT_RECEIPT = ("s3://is459-team6-gold-207567757101/ibm-aml/e2e-20261105-152512/"
                   "scoring/executions/scoring-e2e-20261105-152512.json")
DEFAULT_CURATED = "s3://is459-team6-curated-207567757101/ibm-aml/e2e-20261105-152512"
QUEUE_FILES = ("accounts.csv", "transfers.parquet", "ranking.json")
DATE_PART = re.compile(r"/date=(\d{4}-\d{2}-\d{2})/")


def split_uri(uri: str) -> tuple[str, str]:
    m = re.fullmatch(r"s3://([^/]+)/?(.*)", uri)
    if not m:
        raise ValueError(f"not an s3:// URI: {uri}")
    return m.group(1), m.group(2).rstrip("/")


def list_objects(s3, bucket: str, prefix: str):
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


def plan_curated(s3, curated_uri: str, date_from: str, date_to: str) -> list[tuple[str, str, int]]:
    """(bucket, key, bytes) for every parquet file the dashboard needs from the curated zone."""
    bucket, prefix = split_uri(curated_uri)
    wanted: list[tuple[str, str, int]] = []
    for table in ("transfers", "transfer_labels", "rings"):
        for obj in list_objects(s3, bucket, f"{prefix}/{table}/"):
            key = obj["Key"]
            if not key.endswith(".parquet"):  # skips Spark's _SUCCESS and .crc files
                continue
            if table != "rings":
                m = DATE_PART.search(key)
                if not m or not (date_from <= m.group(1) <= date_to):
                    continue
            wanted.append((bucket, key, obj["Size"]))
    return wanted


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def pull(s3, receipt_uri: str, curated_uri: str, date_from: str, date_to: str, dest: Path,
         dry_run: bool = False, assume_yes: bool = False, say=print) -> None:
    rb, rk = split_uri(receipt_uri)
    receipt = json.loads(s3.get_object(Bucket=rb, Key=rk)["Body"].read())
    if receipt.get("status") != "completed":
        raise SystemExit(f"Receipt says status={receipt.get('status')!r}, not 'completed'.")
    say(f"Receipt: run {receipt['run_id'][:12]}, model {receipt['model_version']}, "
        f"scoring day {receipt['options'].get('start', '?')}")

    queue = [(name, receipt["artifacts"][name]) for name in QUEUE_FILES]
    curated = plan_curated(s3, curated_uri, date_from, date_to)
    if not curated:
        raise SystemExit(f"No curated parquet files found under {curated_uri} for {date_from}..{date_to}. "
                         "Check the prefix and dates.")
    total = sum(a["bytes"] for _, a in queue) + sum(size for *_, size in curated)
    say(f"Queue files: {len(queue)}. Curated files: {len(curated)}. Total: {total / 1e9:.2f} GB")
    if dry_run:
        say("Dry run: nothing downloaded.")
        return
    if not assume_yes and total > 2e9:
        if input("That is large. Continue? [y/N] ").strip().lower() != "y":
            raise SystemExit("Stopped.")

    (dest / "queue").mkdir(parents=True, exist_ok=True)
    for name, art in queue:
        bucket, key = split_uri(art["uri"])
        target = dest / "queue" / name
        s3.download_file(bucket, key, str(target))
        if sha256_of(target) != art["sha256"]:
            target.unlink()
            raise SystemExit(f"Checksum mismatch for {name}; the download was discarded.")
        say(f"  queue/{name}  ({art['bytes'] / 1e6:.1f} MB, checksum ok)")

    curated_prefix = split_uri(curated_uri)[1] + "/"
    done = 0
    for bucket, key, size in curated:
        target = dest / "curated" / key[len(curated_prefix):]
        if target.exists() and target.stat().st_size == size:  # resume: skip what is already here
            done += 1
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, key, str(target))
        done += 1
        if done % 25 == 0 or done == len(curated):
            say(f"  curated: {done}/{len(curated)} files")
    (dest / "sharing").mkdir(exist_ok=True)
    say(f"Done. Run:  DASH_SOURCE=local DASH_ROOT={dest} streamlit run app.py")
    say("The sharing/ folder is empty until Person 4's comparison.csv is placed there.")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--receipt", default=DEFAULT_RECEIPT, help="gold receipt URI of the scoring run")
    ap.add_argument("--curated", default=DEFAULT_CURATED, help="curated zone URI, version prefix included")
    ap.add_argument("--from", dest="date_from", default="2022-09-01", help="first transfer date to copy")
    ap.add_argument("--to", dest="date_to", default="2022-09-14", help="last transfer date to copy (inclusive)")
    ap.add_argument("--dest", default="data", help="local folder (default ./data)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--yes", action="store_true", help="do not ask before a large download")
    args = ap.parse_args(argv)
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        sys.exit("boto3 is not installed:  pip install boto3")
    s3 = boto3.client("s3", region_name="us-east-1")
    try:
        pull(s3, args.receipt, args.curated, args.date_from, args.date_to, Path(args.dest), args.dry_run, args.yes)
    except (BotoCoreError, ClientError) as err:
        sys.exit(f"AWS error: {err}\nCheck you are logged in (aws sts get-caller-identity) and the right "
                 "AWS_PROFILE is set.")


if __name__ == "__main__":
    main()
