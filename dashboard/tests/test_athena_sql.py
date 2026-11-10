"""Checks for dashboard/athena/.

Athena itself cannot be called from a test, so these check what can be checked offline:
  * the SQL splitter and the runner's handling of results and failures (with a stub Athena client);
  * that the demo queries compute the same numbers as the dashboard's own code, by running them on the sample
    data in DuckDB. DuckDB is not Athena: this proves the logic matches, not that Athena accepts every keyword.
"""
import os
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "athena"))
os.environ.setdefault("DASH_SOURCE", "sample")

import run_sql  # noqa: E402
from dashlib import analysis  # noqa: E402

SQL = ROOT / "athena"
SAMPLE = ROOT / "sample_data" / "curated"


def statements(name):
    return run_sql.split_statements((SQL / name).read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- splitter

def test_splitter_ignores_semicolons_in_strings_and_comments():
    sql = "-- a; comment\nSELECT 'a;b' AS x; /* ; */ SELECT `c;d`, \"e;f\";\n;"
    assert run_sql.split_statements(sql) == ["SELECT 'a;b' AS x", "SELECT `c;d`, \"e;f\""]


@pytest.mark.parametrize("name,count", [("01_create_tables.sql", 9), ("02_demo_queries.sql", 4), ("03_sharing_table.sql", 3), ("04_queue_table.sql", 5)])
def test_sql_files_split_into_the_expected_statements(name, count):
    assert len(statements(name)) == count


def test_table_locations_match_the_published_layout():
    ddl = (SQL / "01_create_tables.sql").read_text()
    base = "s3://is459-team6-curated-207567757101/ibm-aml/e2e-20261105-152512"
    for table in ("transfers", "transfer_labels", "rings"):
        assert f"LOCATION '{base}/{table}/'" in ddl
    assert f"{base}/transfers/date=${{date}}/" in ddl


@pytest.mark.parametrize("name", ["01_create_tables.sql", "04_queue_table.sql"])
def test_projected_locations_are_valid_and_update_existing_tables(name):
    sql = statements(name)
    for create in sql:
        if not create.startswith("CREATE EXTERNAL TABLE"):
            continue
        table = re.search(r"IF NOT EXISTS (\S+)", create).group(1)
        location = re.search(r"LOCATION '([^']+)'", create).group(1)
        assert f"ALTER TABLE {table} SET LOCATION '{location}'" in sql
        template = re.search(r"'storage.location.template'\s*=\s*'([^']+)'", create)
        if template:
            assert template.group(1) == location + "date=${date}/"
            updates = [s for s in sql if s.startswith(f"ALTER TABLE {table} SET TBLPROPERTIES")]
            assert len(updates) == 1
            assert f"'storage.location.template' = '{template.group(1)}'" in updates[0]


# --------------------------------------------------------------------------- runner

class Stub:
    """Just enough of the Athena client for run_query."""
    def __init__(self, state="SUCCEEDED", rows=None, reason=""):
        self.state, self.rows, self.reason, self.calls = state, rows or [], reason, []

    def start_query_execution(self, **kw):
        self.calls.append(kw)
        return {"QueryExecutionId": "q1"}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": self.state, "StateChangeReason": self.reason},
                                   "Statistics": {"DataScannedInBytes": 2_000_000}}}

    def get_paginator(self, _):
        rows = self.rows
        return type("P", (), {"paginate": lambda s, QueryExecutionId: [
            {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": v} for v in r]} for r in rows]}}]})()


def test_run_query_splits_header_from_rows_and_reports_bytes():
    header, rows, scanned = run_sql.run_query(Stub(rows=[["a", "b"], ["1", "2"]]), "SELECT 1", "wg", "db")
    assert header == ["a", "b"] and rows == [["1", "2"]] and scanned == 2_000_000


def test_failure_carries_athenas_reason():
    with pytest.raises(run_sql.QueryFailed, match="Access denied"):
        run_sql.run_query(Stub(state="FAILED", reason="Access denied to glue"), "SELECT 1", "wg", "db")


def test_database_context_is_set_except_for_create_database():
    s = Stub()
    run_sql.run_query(s, "SELECT 1", "wg", "db")
    run_sql.run_query(s, "  create database if not exists db", "wg", "db")
    assert s.calls[0]["QueryExecutionContext"] == {"Database": "db"}
    assert "QueryExecutionContext" not in s.calls[1]


# --------------------------------------------------------------------------- same numbers as the dashboard

@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.execute(f"CREATE VIEW transfers AS SELECT * FROM read_parquet('{SAMPLE.as_posix()}/transfers/**/*.parquet', hive_partitioning=true)")
    c.execute(f"CREATE VIEW transfer_labels AS SELECT * FROM read_parquet('{SAMPLE.as_posix()}/transfer_labels/**/*.parquet', hive_partitioning=true)")
    c.execute(f"CREATE VIEW rings AS SELECT * FROM read_parquet('{SAMPLE.as_posix()}/rings/*.parquet')")
    return c


def run(con, sql):
    return con.execute(re.sub(r"ARRAY\[", "[", sql)).df()   # Trino writes ARRAY[...], DuckDB writes [...]


def test_ring_visibility_query_matches_the_dashboards_analysis(con):
    rings = con.execute("SELECT * FROM rings").df()
    per_ring = analysis.ring_visibility(rings)
    expected = analysis.visibility_summary(rings, per_ring)
    got = run(con, statements("02_demo_queries.sql")[3]).iloc[0]
    assert int(got["rings"]) == expected["rings"]
    assert got["pct_no_single_bank_sees_all"] == pytest.approx(round(100 * expected["share_invisible"], 1), abs=0.05)
    assert got["avg_pct_seen_by_best_bank"] == pytest.approx(round(100 * expected["avg_best_share"], 0), abs=0.51)


def test_cross_bank_query_matches_pandas(con):
    t = con.execute("SELECT * FROM transfers").df()
    lab = con.execute("SELECT transfer_id, laundering_mark FROM transfer_labels").df()
    laundering = t.merge(lab[lab.laundering_mark == 1], on="transfer_id")
    expected = (laundering.sender_bank_id != laundering.receiver_bank_id).mean() * 100
    got = run(con, statements("02_demo_queries.sql")[1])
    assert len(laundering) == int(got["laundering_transfers"].sum())
    crossing = got[got["crosses_banks"] == True]  # noqa: E712
    assert crossing["pct"].iloc[0] == pytest.approx(expected, abs=0.06)


def test_transfers_per_day_query_matches_pandas(con):
    t = con.execute("SELECT CAST(\"date\" AS VARCHAR) AS d FROM transfers").df()
    got = run(con, statements("02_demo_queries.sql")[0]).assign(date=lambda d: d["date"].astype(str))
    assert dict(zip(got["date"], got["transfers"])) == t.groupby("d").size().to_dict()
