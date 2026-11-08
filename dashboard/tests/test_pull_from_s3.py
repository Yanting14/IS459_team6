"""Run scripts/pull_from_s3.py against a fake S3 laid out like the real gold/curated buckets."""
import hashlib
import json
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import pull_from_s3 as pull  # noqa: E402

SAMPLE = ROOT / "sample_data"
PREFIX = "ibm-aml/e2e-test"


def put(s3, bucket, key, body):
    s3.put_object(Bucket=bucket, Key=key, Body=body)


@pytest.fixture
def aws():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        for b in ("gold", "curated"):
            s3.create_bucket(Bucket=b)
        yield s3


def populate(s3):
    # curated zone, as Spark writes it (including a _SUCCESS marker and a .crc file)
    for table in ("transfers", "transfer_labels", "rings"):
        for f in (SAMPLE / "curated" / table).rglob("*.parquet"):
            put(s3, "curated", f"{PREFIX}/{f.relative_to(SAMPLE / 'curated').as_posix()}", f.read_bytes())
    put(s3, "curated", f"{PREFIX}/transfers/_SUCCESS", b"")
    put(s3, "curated", f"{PREFIX}/transfers/date=2022-09-13/.part-0.parquet.crc", b"x")
    # gold: the receipt's attempt, plus a decoy attempt that the receipt does not name
    arts = {}
    for name in pull.QUEUE_FILES:
        body = (SAMPLE / "queue" / name).read_bytes()
        key = f"{PREFIX}/scoring/predictions/date=2022-09-14/run=abc/attempts/good/{name}"
        put(s3, "gold", key, body)
        put(s3, "gold", key.replace("/good/", "/decoy/"), b"WRONG")
        arts[name] = {"uri": f"s3://gold/{key}", "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
    receipt = {"status": "completed", "run_id": "abc" * 20, "model_version": "sample-gin",
               "options": {"start": "2022-09-14"}, "artifacts": arts}
    put(s3, "gold", f"{PREFIX}/scoring/executions/exec.json", json.dumps(receipt).encode())
    return f"s3://gold/{PREFIX}/scoring/executions/exec.json", f"s3://curated/{PREFIX}", receipt


def test_pulls_receipt_files_and_only_the_requested_dates(aws, tmp_path):
    receipt_uri, curated_uri, _ = populate(aws)
    pull.pull(aws, receipt_uri, curated_uri, "2022-09-13", "2022-09-14", tmp_path, assume_yes=True, say=lambda *_: None)
    assert (tmp_path / "queue" / "accounts.csv").read_bytes() == (SAMPLE / "queue" / "accounts.csv").read_bytes()
    got = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.parquet"))
    assert "curated/transfers/date=2022-09-13/part-0.parquet" in got
    assert "curated/transfers/date=2022-09-14/part-0.parquet" in got
    assert not any("date=2022-09-12" in p for p in got)          # outside the range
    assert any(p.startswith("curated/rings/") for p in got)       # rings come in full
    assert not list(tmp_path.rglob("_SUCCESS")) and not list(tmp_path.rglob("*.crc"))


def test_checksum_mismatch_is_rejected(aws, tmp_path):
    receipt_uri, curated_uri, receipt = populate(aws)
    receipt["artifacts"]["accounts.csv"]["sha256"] = "0" * 64
    put(aws, "gold", f"{PREFIX}/scoring/executions/exec.json", json.dumps(receipt).encode())
    with pytest.raises(SystemExit, match="Checksum mismatch"):
        pull.pull(aws, receipt_uri, curated_uri, "2022-09-13", "2022-09-14", tmp_path, assume_yes=True, say=lambda *_: None)
    assert not (tmp_path / "queue" / "accounts.csv").exists()


def test_dry_run_downloads_nothing(aws, tmp_path):
    receipt_uri, curated_uri, _ = populate(aws)
    pull.pull(aws, receipt_uri, curated_uri, "2022-09-13", "2022-09-14", tmp_path, dry_run=True, say=lambda *_: None)
    assert not list(tmp_path.rglob("*"))


def test_wrong_prefix_gives_a_clear_error(aws, tmp_path):
    receipt_uri, _, _ = populate(aws)
    with pytest.raises(SystemExit, match="No curated parquet files"):
        pull.pull(aws, receipt_uri, "s3://curated/nope", "2022-09-13", "2022-09-14", tmp_path, say=lambda *_: None)
