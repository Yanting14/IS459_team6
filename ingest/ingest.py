"""Download the IBM AML dataset from Kaggle and land it in the S3 raw zone.

Runs as a one-off ECS Fargate task started by Step Functions. Safe to re-run:
files already in S3 are skipped, and the _SUCCESS marker is written only after
every file is in place.

Environment:
  RAW_BUCKET       S3 bucket for the raw zone
  RAW_PREFIX       key prefix, e.g. ibm-aml/v1
  KAGGLE_DATASET   owner/slug of the Kaggle dataset
  KAGGLE_FILES     comma-separated file names, e.g. HI-Medium_Trans.csv,...
  KAGGLE_API_TOKEN injected by ECS from Secrets Manager
"""

import contextlib
import hashlib
import json
import logging
import os
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest")

FILE_RE = re.compile(r"^(HI|LI)-(Small|Medium|Large)_(Trans|Patterns|accounts)\.(csv|txt)$")
TABLES = {"Trans": "transactions", "Patterns": "patterns", "accounts": "accounts"}
CHUNK = 8 * 1024 * 1024
TRANSFER = TransferConfig(multipart_chunksize=64 * 1024 * 1024, max_concurrency=10)


def s3_key(prefix: str, name: str) -> str:
    """Group files by table so Spark/Glue can read e.g. transactions/ as one dataset."""
    m = FILE_RE.match(name)
    if not m:
        raise ValueError(f"Unexpected file name: {name}")
    variant, size, kind, _ = m.groups()
    return f"{prefix}/{TABLES[kind]}/variant={variant}/size={size.lower()}/{name}"


def head(s3, bucket: str, key: str) -> dict | None:
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def download(api, dataset: str, name: str, workdir: Path) -> Path:
    """Kaggle may return the file as-is or zipped, under a name we don't control."""
    dl_dir = workdir / "download"
    dl_dir.mkdir()
    api.dataset_download_file(dataset, name, path=str(dl_dir), force=True, quiet=True)
    files = [p for p in dl_dir.iterdir() if p.is_file()]
    if len(files) != 1:
        raise RuntimeError(f"Expected one downloaded file for {name}, got {files}")
    return files[0]


def materialise(downloaded: Path, out: Path) -> tuple[str, int]:
    """Unzip if needed, writing `out` and returning (sha256, line count) in one pass."""
    sha = hashlib.sha256()
    lines = 0
    with contextlib.ExitStack() as stack:
        if zipfile.is_zipfile(downloaded):
            zf = stack.enter_context(zipfile.ZipFile(downloaded))
            members = [m for m in zf.namelist() if not m.endswith("/")]
            if len(members) != 1:
                raise RuntimeError(f"Expected one file in {downloaded.name}, got {members}")
            src = stack.enter_context(zf.open(members[0]))
        else:
            src = stack.enter_context(open(downloaded, "rb"))
        dst = stack.enter_context(open(out, "wb"))
        while chunk := src.read(CHUNK):
            sha.update(chunk)
            lines += chunk.count(b"\n")
            dst.write(chunk)
    downloaded.unlink()
    return sha.hexdigest(), lines


def ingest_file(s3, api, bucket: str, prefix: str, dataset: str, name: str) -> dict:
    key = s3_key(prefix, name)
    existing = head(s3, bucket, key)
    if existing:
        log.info("Skipping %s, already at s3://%s/%s", name, bucket, key)
        meta = existing["Metadata"]
        return {"file": name, "key": key, "bytes": existing["ContentLength"],
                "sha256": meta.get("sha256"), "lines": int(meta.get("lines", -1))}

    # Process one file at a time so peak disk is roughly one zip + one CSV.
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        log.info("Downloading %s", name)
        downloaded = download(api, dataset, name, workdir)
        log.info("Downloaded %s (%.2f GB)", downloaded.name, downloaded.stat().st_size / 1e9)

        out = workdir / name
        sha, lines = materialise(downloaded, out)
        size = out.stat().st_size
        log.info("Extracted %s: %.2f GB, %d lines", name, size / 1e9, lines)

        content_type = "text/csv" if name.endswith(".csv") else "text/plain"
        s3.upload_file(str(out), bucket, key, Config=TRANSFER, ExtraArgs={
            "ContentType": content_type,
            "Metadata": {"sha256": sha, "lines": str(lines)},
        })
        log.info("Uploaded s3://%s/%s", bucket, key)

    return {"file": name, "key": key, "bytes": size, "sha256": sha, "lines": lines}


def main() -> None:
    bucket = os.environ["RAW_BUCKET"]
    prefix = os.environ["RAW_PREFIX"].strip("/")
    dataset = os.environ["KAGGLE_DATASET"]
    files = [f.strip() for f in os.environ["KAGGLE_FILES"].split(",") if f.strip()]
    marker = f"{prefix}/_SUCCESS"

    s3 = boto3.client("s3")
    if head(s3, bucket, marker):
        log.info("s3://%s/%s exists, nothing to do", bucket, marker)
        return

    # Importing kaggle authenticates immediately from KAGGLE_API_TOKEN.
    import kaggle

    entries = [ingest_file(s3, kaggle.api, bucket, prefix, dataset, name) for name in files]

    manifest = {
        "dataset": dataset,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "files": entries,
    }
    s3.put_object(Bucket=bucket, Key=f"{prefix}/manifest.json",
                  Body=json.dumps(manifest, indent=2).encode(), ContentType="application/json")
    s3.put_object(Bucket=bucket, Key=marker, Body=b"")
    log.info("Ingest complete: %d files, marker written to s3://%s/%s", len(entries), bucket, marker)


if __name__ == "__main__":
    main()
