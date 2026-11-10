"""bq2.cloud against a fake S3 that serves a temporary folder, on test_sharing's hand-built day."""
import hashlib
import io
import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from bq2 import cloud
from bq2.test_sharing import day, scores

DAY = "2022-09-15"
CURATED = "s3://curated/ibm-aml/v9"
EXECUTIONS = "s3://gold/ibm-aml/v9/scoring/executions"
SHARING = "s3://gold/ibm-aml/v9/sharing"


class FakeS3:
    """get_object, download_file, upload_file and list_objects_v2 over <root>/<bucket>/<key>."""

    def __init__(self, root: Path):
        self.root = root

    def _path(self, bucket, key):
        return self.root / bucket / key

    def put(self, uri, data: bytes):
        b, k = cloud.location(uri)
        p = self._path(b, k)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self._path(Bucket, Key).read_bytes())}

    def download_file(self, bucket, key, filename):
        shutil.copyfile(self._path(bucket, key), filename)

    def upload_file(self, filename, bucket, key, ExtraArgs=None):
        self.put(f"s3://{bucket}/{key}", Path(filename).read_bytes())

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        fake = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                base = fake.root / Bucket
                keys = sorted(str(p.relative_to(base)) for p in base.rglob("*") if p.is_file())
                yield {"Contents": [{"Key": k} for k in keys if k.startswith(Prefix)]}
        return Paginator()


def parquet(frame: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    frame.to_parquet(buf, index=False)
    return buf.getvalue()


def publish_run(s3, execution_id, bank, frame, status="completed", tamper=False):
    data = parquet(frame)
    uri = f"s3://gold/ibm-aml/v9/scoring/predictions/{execution_id}/transfers.parquet"
    s3.put(uri, data)
    digest = hashlib.sha256(b"x" if tamper else data).hexdigest()
    s3.put(f"{EXECUTIONS}/{execution_id}.json", json.dumps({
        "status": status, "options": {"start": DAY, "bank": bank},
        "artifacts": {"transfers.parquet": {"uri": uri, "sha256": digest}}}).encode())


@pytest.fixture
def s3(tmp_path):
    s3 = FakeS3(tmp_path / "s3")
    transfers, labels = day()
    s3.put(f"{CURATED}/transfers/date={DAY}/part-0.parquet", parquet(transfers))
    s3.put(f"{CURATED}/transfer_labels/date={DAY}/part-0.parquet", parquet(labels))
    s3.put(f"{CURATED}/transfers/date={DAY}/_SUCCESS", b"")   # not Parquet: skipped
    publish_run(s3, cloud.shared_id(DAY), "", scores(t1=0.9, t2=0.8, t3=0.1, t4=0.7, t5=0.2))
    publish_run(s3, cloud.bank_id("A", DAY), "A", scores(t1=0.2, t3=0.9, t4=0.1))
    return s3


def go(s3, tmp_path, **kw):
    args = dict(curated_uri=CURATED, executions_uri=EXECUTIONS, sharing_uri=SHARING, dates=[DAY],
                banks=["A"], ratio=0.5, floor=1, model="gin", work_root=str(tmp_path))
    return cloud.run(s3, **{**args, **kw})


def test_publishes_the_dashboard_comparison(s3, tmp_path):
    go(s3, tmp_path)
    out = pd.read_csv(tmp_path / "s3" / "gold" / "ibm-aml/v9/sharing/comparison.csv", dtype={"bank_id": str})
    assert list(out.columns) == cloud.sharing.COMPARISON_COLUMNS
    # Bank A: cohort a1, a2, a3; positives a1, a3; K = ceil(3 * 0.5) = 2.
    # Shared view ranks a1 (0.9) and a3 (0.7) first: 2 of 2. Bank view ranks a2, a3 (0.9) first: 1 of 2.
    got = out.set_index("view").true_positives.to_dict()
    assert got == {"bank": 1, "shared": 2}
    assert (tmp_path / "s3" / "gold" / "ibm-aml/v9/sharing/comparison_by_day.csv").exists()


def test_refuses_a_run_for_the_wrong_bank(s3, tmp_path):
    publish_run(s3, cloud.bank_id("A", DAY), "B", scores(t1=0.2, t3=0.9, t4=0.1))
    with pytest.raises(ValueError, match="unexpected receipt"):
        go(s3, tmp_path)


def test_refuses_an_incomplete_run(s3, tmp_path):
    publish_run(s3, cloud.shared_id(DAY), "", scores(t1=0.9), status="running")
    with pytest.raises(ValueError, match="unexpected receipt"):
        go(s3, tmp_path)


def test_refuses_scores_that_do_not_match_the_receipt(s3, tmp_path):
    publish_run(s3, cloud.bank_id("A", DAY), "A", scores(t1=0.2, t3=0.9, t4=0.1), tamper=True)
    with pytest.raises(ValueError, match="sha256"):
        go(s3, tmp_path)


def test_refuses_a_day_with_no_curated_data(s3, tmp_path):
    with pytest.raises(ValueError, match="No curated transfers"):
        go(s3, tmp_path, dates=["2022-09-20"])


def test_nothing_is_published_when_a_run_is_missing(s3, tmp_path):
    with pytest.raises(FileNotFoundError):
        go(s3, tmp_path, banks=["A", "C"])
    assert not (tmp_path / "s3" / "gold" / "ibm-aml/v9/sharing").exists()
