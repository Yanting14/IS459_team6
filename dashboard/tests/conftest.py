"""Athena-mode test switch.

With DASH_TEST_FAKE_ATHENA=1 (and DASH_SOURCE=athena) the dashboard's Athena and S3 clients are replaced by
fakes that answer from the sample data, so the page tests run the real Athena code path with no AWS.
tests/test_athena_backend.py starts that run in a subprocess. In a normal run this file does nothing.
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

FAKE = None
FAKE_S3 = None
DUCK_CALLS: list[str] = []

if os.environ.get("DASH_TEST_FAKE_ATHENA") == "1":
    import io
    import json

    from dashlib import config, data
    from fake_athena import FakeAthena

    SAMPLE = ROOT / "sample_data"
    assert config.USES_ATHENA, "set DASH_SOURCE=athena"
    FAKE = FakeAthena(SAMPLE)

    class FakeS3:
        """get_object for the three kinds of JSON file the dashboard reads straight from S3."""
        class exceptions:
            class NoSuchKey(Exception):
                pass

        def __init__(self):
            self.keys: list[str] = []

        def get_object(self, Bucket, Key):
            uri = f"s3://{Bucket}/{Key}"
            self.keys.append(uri)
            if uri.startswith(config.QUEUE_URI) and uri.endswith("/ranking.json"):
                day = uri.split("date=")[1].split("/")[0]
                path = SAMPLE / "queue" / f"date={day}" / "ranking.json"
            elif uri.startswith(config.RULES_TUNED_URI):
                path = SAMPLE / "metrics" / "rules_tuned.json"
            elif uri.startswith(config.RULES_URI):
                path = SAMPLE / "metrics" / "rules.json"
            else:
                path = SAMPLE / "metrics" / "models.json"   # the model folder named in ranking.json
            if not path.exists():
                raise self.exceptions.NoSuchKey(uri)
            body = path.read_bytes()
            if path.name == "ranking.json":   # the sample names its model with a sample:// URI; the real one is s3://
                body = json.dumps({**json.loads(body), "model": "s3://models-bucket/bq1/sample-bq1"}).encode()
            return {"Body": io.BytesIO(body)}

    FAKE_S3 = FakeS3()
    data._athena_client = lambda: FAKE
    data._s3_client = lambda: FAKE_S3

    def _no_duckdb():
        DUCK_CALLS.append("connection")
        raise RuntimeError("DuckDB must not be used in athena mode")

    data._connection = _no_duckdb
