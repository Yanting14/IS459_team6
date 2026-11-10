"""Run a .sql file on Athena, one statement at a time, and print the results.

    python athena/run_sql.py athena/01_create_tables.sql
    python athena/run_sql.py athena/02_demo_queries.sql
    python athena/run_sql.py athena/02_demo_queries.sql --dry-run     # print the statements, run nothing

Uses your normal AWS credentials (AWS_PROFILE). Queries run in the workgroup `is459-team6`, which the README
creates with a results folder and a scan limit. Each statement reports how much data it scanned, because
Athena bills by data scanned.
"""
from __future__ import annotations

import argparse
import re
import sys
import time

DEFAULT_WORKGROUP = "is459-team6"
DEFAULT_DATABASE = "is459_team6"


class QueryFailed(Exception):
    pass


def split_statements(sql: str) -> list[str]:
    """Split on semicolons that are not inside quotes or comments."""
    statements, buf = [], []
    i, n, quote = 0, len(sql), None
    while i < n:
        ch, nxt = sql[i], sql[i + 1] if i + 1 < n else ""
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
        elif ch == "-" and nxt == "-":                    # line comment: dropped
            while i < n and sql[i] != "\n":
                i += 1
            continue
        elif ch == "/" and nxt == "*":                    # block comment: dropped
            end = sql.find("*/", i + 2)
            i = n if end == -1 else end + 2
            continue
        elif ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
        elif ch == ";":
            statements.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    statements.append("".join(buf))
    return [s.strip() for s in statements if s.strip()]


def run_query(client, sql: str, workgroup: str, database: str | None, poll: float = 1.0):
    """Returns (header, rows, bytes_scanned). Raises QueryFailed with Athena's reason."""
    kwargs = {"QueryString": sql, "WorkGroup": workgroup}
    if database and not re.match(r"(?is)^\s*create\s+database", sql):
        kwargs["QueryExecutionContext"] = {"Database": database}
    qid = client.start_query_execution(**kwargs)["QueryExecutionId"]
    while True:
        execution = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = execution["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(poll)
    if state != "SUCCEEDED":
        raise QueryFailed(execution["Status"].get("StateChangeReason", state))
    scanned = execution.get("Statistics", {}).get("DataScannedInBytes", 0)
    header, rows = [], []
    for page in client.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for row in page["ResultSet"]["Rows"]:
            rows.append([c.get("VarCharValue", "") for c in row["Data"]])
    if rows and not header:
        header, rows = rows[0], rows[1:]        # Athena returns the column names as the first row of a SELECT
    return header, rows, scanned


def show(header: list[str], rows: list[list[str]], limit: int = 40) -> None:
    if not header:
        return
    shown = rows[:limit]
    widths = [max(len(str(x)) for x in col) for col in zip(header, *shown)] if shown else [len(h) for h in header]
    print("  " + "  ".join(h.ljust(w) for h, w in zip(header, widths)))
    for row in shown:
        print("  " + "  ".join(str(v).ljust(w) for v, w in zip(row, widths)))
    if len(rows) > limit:
        print(f"  ... {len(rows) - limit} more rows")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sqlfile")
    ap.add_argument("--workgroup", default=DEFAULT_WORKGROUP)
    ap.add_argument("--database", default=DEFAULT_DATABASE)
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--dry-run", action="store_true", help="print the statements and run nothing")
    args = ap.parse_args(argv)

    with open(args.sqlfile, encoding="utf-8") as f:
        statements = split_statements(f.read())
    if args.dry_run:
        for i, s in enumerate(statements, 1):
            print(f"--- statement {i} of {len(statements)}\n{s}\n")
        return
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        sys.exit("boto3 is not installed:  pip install boto3 \"botocore[crt]\"")
    client = boto3.client("athena", region_name=args.region)
    total = 0
    for i, sql in enumerate(statements, 1):
        first = " ".join(sql.split())[:90]
        print(f"[{i}/{len(statements)}] {first}{'...' if len(first) == 90 else ''}")
        try:
            header, rows, scanned = run_query(client, sql, args.workgroup, args.database)
        except QueryFailed as err:
            sys.exit(f"  FAILED: {err}")
        except (BotoCoreError, ClientError) as err:
            sys.exit(f"  AWS error: {err}\n  Check you are logged in (aws sts get-caller-identity) and that the "
                     f"workgroup '{args.workgroup}' exists (see athena/README.md).")
        show(header, rows)
        total += scanned
        print(f"  scanned {scanned / 1e6:,.1f} MB\n")
    print(f"Done. Total scanned {total / 1e9:.3f} GB.")


if __name__ == "__main__":
    main()
