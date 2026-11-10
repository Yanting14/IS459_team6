"""A stand-in for the boto3 Athena client, backed by DuckDB over the sample data.

It lets the whole dashboard run in `DASH_SOURCE=athena` mode in a test, with no AWS. What it proves is the
dashboard's behaviour (that its SQL returns the right rows and that the pages work on what comes back),
not that Athena accepts every keyword: DuckDB and Athena (Trino) are different engines.

The tables mirror the Glue tables in athena/*.sql: `transfers` and `transfer_labels` have a string
partition column `date`; `queue_accounts` is the view over accounts.csv; `sharing_comparison` is the view
over comparison.csv. Results come back as text with Athena's type names, like the real service.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

_TYPE = {"BIGINT": "bigint", "INTEGER": "integer", "SMALLINT": "smallint", "TINYINT": "tinyint", "DOUBLE": "double",
         "FLOAT": "real", "VARCHAR": "varchar", "BOOLEAN": "boolean", "DATE": "date", "TIMESTAMP": "timestamp"}


class FakeAthena:
    def __init__(self, sample: Path, tables: tuple[str, ...] = ("transfers", "transfer_labels", "rings",
                                                                 "queue_accounts", "sharing_comparison")):
        s = sample.as_posix()
        self.con = duckdb.connect()
        self.queries: list[str] = []
        self.fail_with: str | None = None
        c = self.con
        if "transfers" in tables:
            c.execute(f"""CREATE VIEW transfers AS SELECT * EXCLUDE ("date"), CAST("date" AS VARCHAR) AS "date"
                          FROM read_parquet('{s}/curated/transfers/**/*.parquet', hive_partitioning=true)""")
        if "transfer_labels" in tables:
            c.execute(f"""CREATE VIEW transfer_labels AS SELECT * EXCLUDE ("date"), CAST("date" AS VARCHAR) AS "date"
                          FROM read_parquet('{s}/curated/transfer_labels/**/*.parquet', hive_partitioning=true)""")
        if "rings" in tables:
            c.execute(f"CREATE VIEW rings AS SELECT * FROM read_parquet('{s}/curated/rings/*.parquet')")
        if "queue_accounts" in tables:
            c.execute(f"""CREATE VIEW queue_accounts AS
                SELECT CAST(rank AS INTEGER) AS "rank", bank_id, account_id, CAST(score AS VARCHAR) AS score,
                       model_version, window_start, cutoff, reason_1, reason_2, reason_3,
                       regexp_extract(filename, 'date=([0-9-]+)', 1) AS "date"
                FROM read_csv('{s}/queue/date=*/accounts.csv', filename=true, all_varchar=true)""")
        if "sharing_comparison" in tables:
            c.execute(f"""CREATE VIEW sharing_comparison AS
                SELECT bank_id, "view", k, reviewed, eligible, positive_accounts, true_positives,
                       precision_at_k, recall_at_k, model
                FROM read_csv('{s}/sharing/comparison.csv', types={{'bank_id': 'VARCHAR'}})""")
        self._results: dict[str, tuple] = {}
        self._n = 0

    # ---- the slice of the boto3 Athena client the dashboard uses
    def start_query_execution(self, QueryString, WorkGroup, QueryExecutionContext):
        assert WorkGroup and QueryExecutionContext.get("Database")
        self.queries.append(QueryString)
        self._n += 1
        qid = f"q{self._n}"
        if self.fail_with:
            self._results[qid] = ("FAILED", self.fail_with, None)
            return {"QueryExecutionId": qid}
        try:
            cur = self.con.execute(QueryString)
            self._results[qid] = ("SUCCEEDED", "", (cur.description, cur.fetchall()))
        except duckdb.Error as err:
            msg = str(err)
            if "does not exist" in msg:
                msg = f"TABLE_NOT_FOUND: {msg.splitlines()[0]}"
            self._results[qid] = ("FAILED", msg, None)
        return {"QueryExecutionId": qid}

    def get_query_execution(self, QueryExecutionId):
        state, reason, _ = self._results[QueryExecutionId]
        return {"QueryExecution": {"Status": {"State": state, "StateChangeReason": reason},
                                   "Statistics": {"DataScannedInBytes": 1_000_000}}}

    def stop_query_execution(self, QueryExecutionId):
        pass

    def get_paginator(self, _name):
        fake = self

        class Pager:
            def paginate(self, QueryExecutionId):
                _, _, (desc, rows) = fake._results[QueryExecutionId]
                info = [{"Name": d[0], "Type": _TYPE.get(str(d[1]).split("(")[0].upper(), "varchar")} for d in desc]
                text = [[None if v is None else _fmt(v) for v in r] for r in rows]
                header = {"Data": [{"VarCharValue": d[0]} for d in desc]}
                pages = [text[i:i + 1000] for i in range(0, len(text), 1000)] or [[]]
                for n, chunk in enumerate(pages):
                    body = [{"Data": [({} if v is None else {"VarCharValue": v}) for v in r]} for r in chunk]
                    yield {"ResultSet": {"Rows": ([header] if n == 0 else []) + body,
                                         "ResultSetMetadata": {"ColumnInfo": info}}}
        return Pager()


def _fmt(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if hasattr(v, "strftime") and hasattr(v, "hour"):
        return v.strftime("%Y-%m-%d %H:%M:%S.") + f"{v.microsecond // 1000:03d}"
    return str(v)
