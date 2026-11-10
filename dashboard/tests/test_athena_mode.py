"""Checks that only make sense in athena mode (the fake Athena of conftest.py). Skipped in a normal run;
tests/test_athena_backend.py runs this file in a subprocess with DASH_SOURCE=athena."""
import os

import duckdb
import pandas as pd
import pytest

pytestmark = pytest.mark.skipif(os.environ.get("DASH_TEST_FAKE_ATHENA") != "1", reason="athena mode only")

import conftest  # noqa: E402
from dashlib import athena, config, data  # noqa: E402
from streamlit.testing.v1 import AppTest  # noqa: E402

ROOT = conftest.ROOT


def run_investigation():
    at = AppTest.from_file(str(ROOT / "views/account_investigation.py"), default_timeout=60).run()
    assert not at.exception, [e.value for e in at.exception]
    return at


def test_pages_ran_through_athena_and_never_touched_duckdb():
    run_investigation()
    assert conftest.FAKE.queries, "no query reached Athena"
    assert conftest.DUCK_CALLS == []


def test_labels_are_joined_into_the_transfer_query_not_fetched_separately():
    conftest.FAKE.queries.clear()
    data.account_transfers.clear()
    data.neighbour_transfers.clear()
    run_investigation()
    transfer_queries = [q for q in conftest.FAKE.queries if "FROM transfers t" in q]
    assert transfer_queries and all("LEFT JOIN" in q and "transfer_labels" in q for q in transfer_queries)
    assert not [q for q in conftest.FAKE.queries if "transfer_labels" in q and "FROM transfers t" not in q]


def test_partition_columns_are_filtered_on_both_tables():
    data.account_transfers.clear()
    conftest.FAKE.queries.clear()
    data.account_transfers("001", "ABC123", "2022-09-01", "2022-09-14")
    q = conftest.FAKE.queries[-1]
    assert q.count('"date" BETWEEN \'2022-09-01\' AND \'2022-09-14\'') == 2   # the labels sub-select and the transfers


def test_repeat_clicks_are_served_from_the_cache():
    data.account_transfers.clear()
    conftest.FAKE.queries.clear()
    first = data.account_transfers("001", "ABC123", "2022-09-01", "2022-09-14")
    again = data.account_transfers("001", "ABC123", "2022-09-01", "2022-09-14")
    assert len(conftest.FAKE.queries) == 1 and first.equals(again)


def test_transfers_for_a_real_queue_account_match_the_parquet_files():
    queue = data.load_queue("2022-09-15")
    bank, acct = queue.bank_id[0], queue.account_id[0]
    got = data.account_transfers(bank, acct, "2022-09-01", "2022-09-14")
    sample = (ROOT / "sample_data" / "curated").as_posix()
    want = duckdb.connect().execute(f"""
        SELECT t.transfer_id, COALESCE(l.laundering_mark, 0) AS m
        FROM read_parquet('{sample}/transfers/**/*.parquet', hive_partitioning=true) t
        LEFT JOIN read_parquet('{sample}/transfer_labels/**/*.parquet', hive_partitioning=true) l USING (transfer_id)
        WHERE t."date" BETWEEN '2022-09-01' AND '2022-09-14'
          AND ((t.sender_bank_id = ? AND t.sender_account_number = ?) OR (t.receiver_bank_id = ? AND t.receiver_account_number = ?))
        """, [bank, acct, bank, acct]).df()
    assert len(got) == len(want) > 0
    assert sorted(got.transfer_id) == sorted(want.transfer_id)
    assert int(got.laundering_mark.sum()) == int(want.m.sum())
    assert got.sender_bank_id.str.len().eq(3).all() and got.timestamp.is_monotonic_increasing


def test_neighbours_are_limited_newest_first_and_exact_on_bank_and_account():
    queue = data.load_queue("2022-09-15")
    own = data.account_transfers(queue.bank_id[0], queue.account_id[0], "2022-09-01", "2022-09-14")
    cps = pd.DataFrame({"bank": [own.receiver_bank_id.iloc[0]], "acct": [own.receiver_account_number.iloc[0]]})
    got = data.neighbour_transfers(cps, "2022-09-01", "2022-09-14", limit=5)
    assert 0 < len(got) <= 5 and got.timestamp.is_monotonic_decreasing
    b, a = cps.bank[0], cps.acct[0]
    assert (((got.sender_bank_id == b) & (got.sender_account_number == a))
            | ((got.receiver_bank_id == b) & (got.receiver_account_number == a))).all()


def test_values_that_could_alter_the_sql_are_refused():
    with pytest.raises(data.DataMissing, match="Refusing"):
        data.account_transfers("001", "x' OR '1'='1", "2022-09-01", "2022-09-14")
    with pytest.raises(data.DataMissing, match="YYYY-MM-DD"):
        data.account_transfers("001", "ABC123", "2022-09-01'; --", "2022-09-14")


def test_queue_comes_back_typed_and_exact():
    q = data.load_queue("2022-09-15")
    assert q["rank"].tolist() == sorted(q["rank"]) and q["rank"].dtype.kind == "i"
    assert q["score"].dtype == "float64" and (q.score_exact.astype(float) == q.score).all()
    assert q.bank_id.str.len().eq(3).all()
    assert data.queue_days() == ["2022-09-15", "2022-09-16"]


def test_ranking_and_metrics_are_read_from_s3():
    data.load_ranking_meta.clear()
    data.load_metrics.clear()
    meta = data.load_ranking_meta("2022-09-15")
    assert meta["date"] == "2022-09-15"
    metrics = data.load_metrics()
    assert set(metrics) == {"models", "rules", "rules_tuned"} and all(metrics.values())
    assert any(k.startswith(config.RULES_TUNED_URI) for k in conftest.FAKE_S3.keys)


def test_an_athena_failure_becomes_a_message_the_page_can_show():
    conftest.FAKE.fail_with = "ACCESS_DENIED: not authorized to perform athena:StartQueryExecution"
    try:
        with pytest.raises(data.DataMissing, match="ACCESS_DENIED"):
            data.athena_query("SELECT 1")
    finally:
        conftest.FAKE.fail_with = None


def test_a_missing_sharing_table_gives_the_placeholder_not_an_error():
    from fake_athena import FakeAthena
    real = data._athena_client
    data._athena_client = lambda: FakeAthena(ROOT / "sample_data", tables=("rings",))
    data.load_comparison.clear()
    try:
        assert data.load_comparison() is None
    finally:
        data._athena_client = real
        data.load_comparison.clear()


def test_the_sidebar_reports_what_athena_scanned():
    assert athena.STATS.queries > 0 and athena.STATS.bytes_scanned > 0
