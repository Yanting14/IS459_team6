"""Run scripts/pull_from_s3.py against a fake S3 laid out like the real gold/models/curated buckets."""
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
EXECUTION = "bq1-test"
DAYS = ["2022-09-15", "2022-09-16"]
MODEL = "s3://models/bq1/bq1-test"
RULES, TUNED = "s3://gold/bq1/baseline/r1", "s3://gold/bq1/baseline-tuned/t1"


def put(s3, bucket, key, body):
    s3.put_object(Bucket=bucket, Key=key, Body=body)


@pytest.fixture
def aws():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        for b in ("gold", "models", "curated"):
            s3.create_bucket(Bucket=b)
        yield s3


def populate(s3, model=MODEL):
    # curated zone, as Spark writes it (including a _SUCCESS marker and a .crc file)
    for table in ("transfers", "transfer_labels", "rings"):
        for f in (SAMPLE / "curated" / table).rglob("*.parquet"):
            put(s3, "curated", f"{PREFIX}/{f.relative_to(SAMPLE / 'curated').as_posix()}", f.read_bytes())
    put(s3, "curated", f"{PREFIX}/transfers/_SUCCESS", b"")
    put(s3, "curated", f"{PREFIX}/transfers/date=2022-09-15/.part-0.parquet.crc", b"x")
    # gold: one queue per day, as the score job writes it, naming the model in ranking.json
    for day in DAYS:
        folder = f"bq1/queue/{EXECUTION}/date={day}"
        put(s3, "gold", f"{folder}/accounts.csv", (SAMPLE / "queue" / f"date={day}" / "accounts.csv").read_bytes())
        ranking = json.loads((SAMPLE / "queue" / f"date={day}" / "ranking.json").read_text())
        put(s3, "gold", f"{folder}/ranking.json", json.dumps(dict(ranking, model=model)).encode())
        put(s3, "gold", f"{folder}/_SUCCESS", b"")
    for uri, file in ((MODEL, "models.json"), (RULES, "rules.json"), (TUNED, "rules_tuned.json")):
        bucket, key = pull.split_uri(uri)
        put(s3, bucket, f"{key}/metrics.json", (SAMPLE / "metrics" / file).read_bytes())
        put(s3, bucket, f"{key}/_SUCCESS", b"")


def run(s3, dest, **kw):
    args = dict(date_from="2022-09-15", date_to="2022-09-16", assume_yes=True, say=lambda *_: None)
    args.update(kw)
    curated = args.pop("curated", f"s3://curated/{PREFIX}")
    pull.pull(s3, "s3://gold/bq1/queue", EXECUTION, DAYS, RULES, TUNED, curated,
              args.pop("date_from"), args.pop("date_to"), dest, **args)


def test_pulls_both_days_the_metrics_and_only_the_requested_dates(aws, tmp_path):
    populate(aws)
    run(aws, tmp_path)
    for day in DAYS:
        got = (tmp_path / "queue" / f"date={day}" / "accounts.csv").read_bytes()
        assert got == (SAMPLE / "queue" / f"date={day}" / "accounts.csv").read_bytes()
        assert (tmp_path / "queue" / f"date={day}" / "ranking.json").exists()
    for file in ("models.json", "rules.json", "rules_tuned.json"):
        assert (tmp_path / "metrics" / file).read_bytes() == (SAMPLE / "metrics" / file).read_bytes()
    got = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.parquet"))
    assert "curated/transfers/date=2022-09-15/part-0.parquet" in got
    assert "curated/transfer_labels/date=2022-09-16/part-0.parquet" in got
    assert not any("date=2022-09-14" in p for p in got)          # outside the range
    assert any(p.startswith("curated/rings/") for p in got)       # rings come in full
    assert not list(tmp_path.rglob("_SUCCESS")) and not list(tmp_path.rglob("*.crc"))


def test_checksum_mismatch_is_rejected(aws, tmp_path, monkeypatch):
    populate(aws)
    real = pull.plan_artifacts

    def corrupt(*a, **kw):
        files, model = real(*a, **kw)
        uri, local, size, _ = files[0]
        return [(uri, local, size, "0" * 32), *files[1:]], model

    monkeypatch.setattr(pull, "plan_artifacts", corrupt)
    with pytest.raises(SystemExit, match="Checksum mismatch"):
        run(aws, tmp_path)
    assert not (tmp_path / "queue" / f"date={DAYS[0]}" / "accounts.csv").exists()


def test_an_unfinished_day_is_refused(aws, tmp_path):
    populate(aws)
    aws.delete_object(Bucket="gold", Key=f"bq1/queue/{EXECUTION}/date={DAYS[1]}/_SUCCESS")
    with pytest.raises(SystemExit, match="2022-09-16 queue has no _SUCCESS"):
        run(aws, tmp_path)
    assert not list(tmp_path.rglob("*"))


def test_days_scored_by_different_models_are_refused(aws, tmp_path):
    populate(aws)
    put(aws, "gold", f"bq1/queue/{EXECUTION}/date={DAYS[1]}/ranking.json",
        json.dumps({"model": "s3://models/bq1/other"}).encode())
    with pytest.raises(SystemExit, match="different models"):
        run(aws, tmp_path)


def test_dry_run_downloads_nothing(aws, tmp_path):
    populate(aws)
    run(aws, tmp_path, dry_run=True)
    assert not list(tmp_path.rglob("*"))


def test_wrong_prefix_gives_a_clear_error(aws, tmp_path):
    populate(aws)
    with pytest.raises(SystemExit, match="No curated parquet files"):
        run(aws, tmp_path, curated="s3://curated/nope")
