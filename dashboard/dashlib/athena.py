"""A small Amazon Athena client: run one SQL statement, get a DataFrame back.

Nothing here knows about Streamlit or about the dashboard's tables, so it can be tested with a fake
``boto3`` client (see tests/fake_athena.py). ``data.py`` builds the dashboard's queries and calls ``run``.

Athena is asynchronous: ``start_query_execution`` returns an id, the query is polled until it finishes,
and the rows are then paged out with ``get_query_results``. Every query is billed by the bytes it scans,
so each run adds to ``STATS`` and the sidebar shows the running total.
"""
from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass, field

import pandas as pd

MAX_WAIT_SECONDS = 180
_INT_TYPES = {"tinyint", "smallint", "integer", "int", "bigint"}
_FLOAT_TYPES = {"real", "float", "double", "decimal"}


class AthenaError(Exception):
    """Athena refused or failed a query. The message is Athena's own reason."""


@dataclass
class Stats:
    """What this Streamlit process has asked Athena so far."""
    queries: int = 0
    bytes_scanned: int = 0
    seconds: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, scanned: int, seconds: float) -> None:
        with self._lock:
            self.queries += 1
            self.bytes_scanned += int(scanned or 0)
            self.seconds += seconds


STATS = Stats()


def _convert(values: list, athena_type: str) -> pd.Series:
    """One column of Athena's strings as a typed pandas Series. A missing cell is None."""
    kind = athena_type.lower().split("(")[0]
    s = pd.Series(values, dtype="object")
    if kind in _INT_TYPES:
        n = pd.to_numeric(s)
        return n.astype("Int64") if n.isna().any() else n.astype("int64")
    if kind in _FLOAT_TYPES:
        return pd.to_numeric(s).astype("float64")
    if kind == "boolean":
        return s.map({"true": True, "false": False}).astype("object")
    if kind in ("timestamp", "date"):
        return pd.to_datetime(s)
    return s


def run(client, sql: str, workgroup: str, database: str, poll: float = 0.25) -> pd.DataFrame:
    """Run one statement and return its rows. Raises AthenaError with Athena's reason on failure."""
    started = time.monotonic()
    qid = client.start_query_execution(
        QueryString=sql, WorkGroup=workgroup, QueryExecutionContext={"Database": database})["QueryExecutionId"]
    delay = poll
    while True:
        execution = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = execution["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.monotonic() - started > MAX_WAIT_SECONDS:
            try:
                client.stop_query_execution(QueryExecutionId=qid)
            except Exception:  # best effort: the query may have just finished
                pass
            raise AthenaError(f"Query still running after {MAX_WAIT_SECONDS} s; it was cancelled.")
        time.sleep(delay)
        delay = min(delay * 1.5, 2.0)
    if state != "SUCCEEDED":
        raise AthenaError(execution["Status"].get("StateChangeReason") or f"Query {state.lower()}.")

    names: list[str] = []
    types: list[str] = []
    columns: list[list] = []
    first = True
    for page in client.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        if first:
            info = page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
            names = [c["Name"] for c in info]
            types = [c["Type"] for c in info]
            columns = [[] for _ in names]
        rows = page["ResultSet"]["Rows"]
        if first:
            rows = rows[1:]  # on a SELECT the first row of the first page repeats the column names
            first = False
        for row in rows:
            cells = row["Data"]
            for i in range(len(names)):
                columns[i].append(cells[i].get("VarCharValue") if i < len(cells) else None)
    scanned = execution.get("Statistics", {}).get("DataScannedInBytes", 0)
    STATS.add(scanned, time.monotonic() - started)
    return pd.DataFrame({n: _convert(c, t) for n, c, t in zip(names, columns, types)})


# ---- values that go into SQL text. Athena has no bind parameters for these queries, so each one is checked.

_SAFE = re.compile(r"^[A-Za-z0-9_:.\-]{1,64}$")
_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def literal(value: str) -> str:
    """A bank id, account number or similar as a quoted SQL string. Anything unusual is refused."""
    v = str(value).strip()
    if not _SAFE.match(v):
        raise ValueError(f"Refusing to put {v!r} into a query: only letters, digits and _ : . - are allowed.")
    return f"'{v}'"


def day(value: str) -> str:
    """A YYYY-MM-DD date as a quoted SQL string."""
    v = str(value).strip()
    if not _DAY.match(v):
        raise ValueError(f"Not a YYYY-MM-DD date: {v!r}")
    return f"'{v}'"
