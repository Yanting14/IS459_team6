"""Download the files the dashboard needs from S3 into a local folder.

    python scripts/pull_from_s3.py --dry-run     # list what would be copied and how big it is
    python scripts/pull_from_s3.py               # copy it into ./data

Then run:  DASH_SOURCE=local DASH_ROOT=./data streamlit run app.py

What it copies
  data/queue/    date=<day>/accounts.csv and ranking.json for each day of one BQ1 pipeline
                 execution. A day without _SUCCESS is refused (the score job writes it last).
  data/metrics/  models.json: the metrics.json of the model named in the queues' ranking.json;
                 rules.json, rules_tuned.json: the two rules baselines' metrics.json
  data/curated/  transfers/ and transfer_labels/ for the date range, and rings/ in full.

Every queue and metrics file is checked against its S3 ETag (the MD5 of a single-part upload).
It uses your normal AWS credentials (set AWS_PROFILE if you use a named profile).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

GOLD = "s3://is459-team6-gold-207567757101"
DEFAULT_QUEUES = f"{GOLD}/bq1/queue"
DEFAULT_EXECUTION = "bq1-bankvis-20261109"
DEFAULT_DAYS = ("2022-09-15", "2022-09-16")
DEFAULT_RULES = f"{GOLD}/bq1/baseline/t4-20261108"
DEFAULT_RULES_TUNED = f"{GOLD}/bq1/baseline-tuned/20261109"
DEFAULT_CURATED = "s3://is459-team6-curated-207567757101/ibm-aml/e2e-20261105-152512"
QUEUE_FILES = ("accounts.csv", "ranking.json")
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


def md5_of(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def head(s3, uri: str) -> dict | None:
    """The object's metadata, or None if there is no such object."""
    bucket, key = split_uri(uri)
    found = list(list_objects(s3, bucket, key))
    return next((o for o in found if o["Key"] == key), None)


def finished(s3, folder: str, what: str) -> None:
    if head(s3, f"{folder}/_SUCCESS") is None:
        raise SystemExit(f"{what} has no _SUCCESS at {folder}/; it is not a finished run.")


def plan_artifacts(s3, queues_uri: str, execution: str, days: list[str], rules_uri: str,
                   tuned_uri: str) -> tuple[list[tuple[str, str, int, str]], str]:
    """(uri, local path, bytes, etag) for every queue and metrics file, and the model folder."""
    files: list[tuple[str, str, int, str]] = []
    models = set()
    for day in days:
        folder = f"{queues_uri.rstrip('/')}/{execution}/date={day}"
        finished(s3, folder, f"The {day} queue")
        for name in QUEUE_FILES:
            obj = head(s3, f"{folder}/{name}")
            if obj is None:
                raise SystemExit(f"{folder}/{name} is missing.")
            files.append((f"{folder}/{name}", f"queue/date={day}/{name}", obj["Size"], obj["ETag"].strip('"')))
        bucket, key = split_uri(f"{folder}/ranking.json")
        models.add(json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())["model"].rstrip("/"))
    if len(models) != 1:
        raise SystemExit(f"The queues were scored by different models: {sorted(models)}")
    model = models.pop()
    for folder, local, what in ((model, "metrics/models.json", "The model"),
                                (rules_uri.rstrip("/"), "metrics/rules.json", "The rules baseline"),
                                (tuned_uri.rstrip("/"), "metrics/rules_tuned.json", "The best-case rules")):
        finished(s3, folder, what)
        obj = head(s3, f"{folder}/metrics.json")
        if obj is None:
            raise SystemExit(f"{folder}/metrics.json is missing.")
        files.append((f"{folder}/metrics.json", local, obj["Size"], obj["ETag"].strip('"')))
    return files, model


def pull(s3, queues_uri: str, execution: str, days: list[str], rules_uri: str, tuned_uri: str,
         curated_uri: str, date_from: str, date_to: str, dest: Path,
         dry_run: bool = False, assume_yes: bool = False, say=print) -> None:
    artifacts, model = plan_artifacts(s3, queues_uri, execution, days, rules_uri, tuned_uri)
    say(f"Execution {execution}, days {', '.join(days)}, model {model}")
    curated = plan_curated(s3, curated_uri, date_from, date_to)
    if not curated:
        raise SystemExit(f"No curated parquet files found under {curated_uri} for {date_from}..{date_to}. "
                         "Check the prefix and dates.")
    total = sum(size for _, _, size, _ in artifacts) + sum(size for *_, size in curated)
    say(f"Queue and metrics files: {len(artifacts)}. Curated files: {len(curated)}. Total: {total / 1e9:.2f} GB")
    if dry_run:
        say("Dry run: nothing downloaded.")
        return
    if not assume_yes and total > 2e9:
        if input("That is large. Continue? [y/N] ").strip().lower() != "y":
            raise SystemExit("Stopped.")

    for uri, local, size, etag in artifacts:
        bucket, key = split_uri(uri)
        target = dest / local
        target.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, key, str(target))
        # A multipart upload's ETag is not an MD5; these files are far below the multipart size.
        ok = md5_of(target) == etag if "-" not in etag else target.stat().st_size == size
        if not ok:
            target.unlink()
            raise SystemExit(f"Checksum mismatch for {local}; the download was discarded.")
        say(f"  {local}  ({size / 1e3:.1f} kB, checksum ok)")

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
    ap.add_argument("--queues", default=DEFAULT_QUEUES, help="where the BQ1 queues are published")
    ap.add_argument("--execution", default=DEFAULT_EXECUTION, help="the pipeline execution that scored them")
    ap.add_argument("--days", default=",".join(DEFAULT_DAYS), help="comma-separated queue days")
    ap.add_argument("--rules", default=DEFAULT_RULES, help="the label-free rules baseline's output folder")
    ap.add_argument("--rules-tuned", default=DEFAULT_RULES_TUNED, help="the best-case rules' output folder")
    ap.add_argument("--curated", default=DEFAULT_CURATED, help="curated zone URI, version prefix included")
    ap.add_argument("--from", dest="date_from", default="2022-09-01", help="first transfer date to copy")
    ap.add_argument("--to", dest="date_to", default="2022-09-16",
                    help="last transfer date to copy (inclusive): the last queue day")
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
    days = [d.strip() for d in args.days.split(",") if d.strip()]
    try:
        pull(s3, args.queues, args.execution, days, args.rules, args.rules_tuned, args.curated,
             args.date_from, args.date_to, Path(args.dest), args.dry_run, args.yes)
    except (BotoCoreError, ClientError) as err:
        sys.exit(f"AWS error: {err}\nCheck you are logged in (aws sts get-caller-identity) and the right "
                 "AWS_PROFILE is set.")


if __name__ == "__main__":
    main()
