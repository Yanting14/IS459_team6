"""dashlib/athena.py against a stub boto3 client: paging, types, missing cells, failures, SQL literals."""
import pandas as pd
import pytest

from dashlib import athena


class Stub:
    def __init__(self, pages, info, states=("SUCCEEDED",), reason=""):
        self.pages, self.info, self.states, self.reason = pages, info, list(states), reason
        self.polls, self.stopped = 0, False

    def start_query_execution(self, **kw):
        self.kw = kw
        return {"QueryExecutionId": "q"}

    def get_query_execution(self, QueryExecutionId):
        state = self.states[min(self.polls, len(self.states) - 1)]
        self.polls += 1
        return {"QueryExecution": {"Status": {"State": state, "StateChangeReason": self.reason},
                                   "Statistics": {"DataScannedInBytes": 5_000_000}}}

    def stop_query_execution(self, QueryExecutionId):
        self.stopped = True

    def get_paginator(self, _):
        stub = self
        return type("P", (), {"paginate": lambda s, QueryExecutionId: [
            {"ResultSet": {"Rows": p, "ResultSetMetadata": {"ColumnInfo": stub.info}}} for p in stub.pages]})()


def row(*vals):
    return {"Data": [({} if v is None else {"VarCharValue": v}) for v in vals]}


INFO = [{"Name": "bank", "Type": "varchar"}, {"Name": "n", "Type": "bigint"}, {"Name": "x", "Type": "double"},
        {"Name": "ts", "Type": "timestamp"}]


def test_pages_are_joined_and_only_the_first_page_has_a_header_row():
    stub = Stub([[row("bank", "n", "x", "ts"), row("010", "1", "0.5", "2022-09-01 00:08:00.000")],
                 [row("002", "2", "1.5", "2022-09-02 10:00:00.000")]], INFO)
    before = athena.STATS.queries
    df = athena.run(stub, "SELECT 1", "wg", "db", poll=0)
    assert df.bank.tolist() == ["010", "002"]                       # leading zeros survive
    assert df.n.dtype == "int64" and df.x.dtype == "float64"
    assert pd.api.types.is_datetime64_any_dtype(df.ts)
    assert stub.kw["QueryExecutionContext"] == {"Database": "db"} and stub.kw["WorkGroup"] == "wg"
    assert athena.STATS.queries == before + 1


def test_missing_cells_become_nulls_and_integers_stay_integers():
    stub = Stub([[row("bank", "n", "x", "ts"), row("010", None, None, None)]], INFO)
    df = athena.run(stub, "SELECT 1", "wg", "db", poll=0)
    assert df.bank[0] == "010" and pd.isna(df.n[0]) and pd.isna(df.x[0]) and pd.isna(df.ts[0])
    assert str(df.n.dtype) == "Int64"


def test_empty_result_keeps_its_columns():
    stub = Stub([[row("bank", "n", "x", "ts")]], INFO)
    df = athena.run(stub, "SELECT 1", "wg", "db", poll=0)
    assert df.empty and list(df.columns) == ["bank", "n", "x", "ts"]


def test_waits_until_the_query_finishes():
    stub = Stub([[row("bank", "n", "x", "ts")]], INFO, states=("QUEUED", "RUNNING", "SUCCEEDED"))
    athena.run(stub, "SELECT 1", "wg", "db", poll=0)
    assert stub.polls == 3


def test_failure_carries_athenas_reason():
    stub = Stub([], INFO, states=("FAILED",), reason="HIVE_BAD_DATA: bad column")
    with pytest.raises(athena.AthenaError, match="HIVE_BAD_DATA"):
        athena.run(stub, "SELECT 1", "wg", "db", poll=0)


def test_a_query_that_never_finishes_is_cancelled(monkeypatch):
    monkeypatch.setattr(athena, "MAX_WAIT_SECONDS", -1)
    stub = Stub([], INFO, states=("RUNNING",))
    with pytest.raises(athena.AthenaError, match="cancelled"):
        athena.run(stub, "SELECT 1", "wg", "db", poll=0)
    assert stub.stopped


@pytest.mark.parametrize("bad", ["a'b", "a b", "a;b", "", "x" * 65, 'a"b', "a\\b"])
def test_literal_refuses_anything_that_could_change_the_query(bad):
    with pytest.raises(ValueError):
        athena.literal(bad)


def test_literal_and_day_accept_real_values():
    assert athena.literal("8000EBD30") == "'8000EBD30'" and athena.literal("010") == "'010'"
    assert athena.day("2022-09-14") == "'2022-09-14'"
    with pytest.raises(ValueError):
        athena.day("14/09/2022")
