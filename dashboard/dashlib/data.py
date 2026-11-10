"""All reads go through this module, so the pages never touch a file or a database directly.

Two back ends, chosen by ``DASH_SOURCE``:

  athena   every table is read with SQL on Amazon Athena (curated transfers, labels and rings, the
           BQ1 queue, the BQ2 comparison). Only two small JSON files (ranking.json and the three
           metrics.json) are fetched from S3 directly, because they are not tables.
  sample / local / s3   the offline path: files read through DuckDB. DuckDB is imported only here
           and only on that path, so athena mode never loads it.

Every function returns a plain DataFrame or dict. Reads are cached for the session.
Account and bank IDs are always strings: bank IDs such as ``010`` lose their leading
zero if they are ever parsed as numbers.
"""
from __future__ import annotations

import json
import re

import pandas as pd
import streamlit as st

from . import athena, config

_TRANSFER_FIELDS = (
    "transfer_id", "timestamp", "date", "sender_bank_id", "sender_account_number",
    "receiver_bank_id", "receiver_account_number", "amount_paid", "payment_currency",
    "amount_received", "receiving_currency", "payment_format",
)
TRANSFER_COLS = ", ".join(f't."{c}"' for c in _TRANSFER_FIELDS)


class DataMissing(Exception):
    """A file the page needs is not there. The message says which one."""


# --------------------------------------------------------------------------- athena plumbing

@st.cache_resource(show_spinner=False)
def _athena_client():
    import boto3
    return boto3.client("athena", region_name=config.ATHENA_REGION)


@st.cache_resource(show_spinner=False)
def _s3_client():
    import boto3
    return boto3.client("s3", region_name=config.ATHENA_REGION)


def athena_query(sql: str) -> pd.DataFrame:
    """Run SQL on Athena. Any failure becomes DataMissing, whose message the page shows."""
    try:
        return athena.run(_athena_client(), sql, config.ATHENA_WORKGROUP, config.ATHENA_DATABASE)
    except athena.AthenaError as err:
        hint = ""
        low = str(err).lower()
        if "does not exist" in low or "not found" in low:
            hint = " Run the SQL files in `athena/` first (see athena/README.md)."
        raise DataMissing(f"Athena could not run the query: {err}{hint}") from err
    except ValueError as err:  # athena.literal refused a value
        raise DataMissing(str(err)) from err
    except Exception as err:   # no credentials, wrong profile, no network
        raise DataMissing(
            f"Could not reach Athena ({type(err).__name__}: {err}). Check `aws sts get-caller-identity` and AWS_PROFILE.") from err


@st.cache_resource(show_spinner=False)
def _glue_client():
    import boto3
    return boto3.client("glue", region_name=config.ATHENA_REGION)


@st.cache_data(show_spinner=False)
def queue_uri() -> str:
    """The S3 folder of the queue run that the Athena table ``queue_files`` reads.

    Each pipeline run re-points that table at its own queue, so by default the folder is read from
    the table itself, and ranking.json always comes from the same run as the rows. DASH_QUEUE_URI
    overrides it."""
    if config.QUEUE_URI:
        return config.QUEUE_URI
    try:
        table = _glue_client().get_table(DatabaseName=config.ATHENA_DATABASE, Name="queue_files")["Table"]
    except Exception as err:
        raise DataMissing(f"Could not read the location of the Athena table `queue_files` "
                          f"({type(err).__name__}: {err}). Set DASH_QUEUE_URI to the queue folder.") from err
    return table["StorageDescriptor"]["Location"].rstrip("/")


def _s3_json(uri: str) -> dict | None:
    """A small JSON file from S3, or None if it is not there."""
    m = re.fullmatch(r"s3://([^/]+)/(.+)", uri)
    if not m:
        raise DataMissing(f"Not an s3:// URI: {uri}")
    s3 = _s3_client()
    try:
        return json.loads(s3.get_object(Bucket=m.group(1), Key=m.group(2))["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as err:
        raise DataMissing(f"Could not read `{uri}` ({type(err).__name__}: {err}).") from err


# --------------------------------------------------------------------------- offline (DuckDB) plumbing

@st.cache_resource(show_spinner=False)
def _connection() -> "duckdb.DuckDBPyConnection":
    import duckdb  # only the offline path needs it
    con = duckdb.connect()
    if config.USES_S3:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        try:
            con.execute("INSTALL aws; LOAD aws;")
            con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1')")
        except Exception as exc:  # fall back to the plain httpfs credentials from env vars
            st.session_state["_s3_note"] = f"AWS credential chain not loaded: {exc}"
    return con


def query(sql: str, params: list | tuple = (), frames: dict[str, pd.DataFrame] | None = None) -> pd.DataFrame:
    cur = _connection().cursor()  # a cursor per call: Streamlit runs pages on several threads
    try:
        for name, frame in (frames or {}).items():
            cur.register(name, frame)
        return cur.execute(sql, list(params)).df()
    finally:
        cur.close()


def exists(pattern: str) -> bool:
    try:
        return int(query("SELECT count(*) AS n FROM glob(?)", [pattern]).n[0]) > 0
    except Exception:
        return False


def _require(pattern: str, what: str) -> None:
    if not exists(pattern):
        raise DataMissing(f"{what} not found at `{pattern}`.")


def _transfers() -> str:
    return f"read_parquet('{config.CURATED_DIR}/transfers/**/*.parquet', hive_partitioning=true)"


# --------------------------------------------------------------------------- queue

@st.cache_data(show_spinner=False)
def queue_days() -> list[str]:
    """The days with a published queue: one ``date=<day>`` folder each, oldest first."""
    if config.USES_ATHENA:
        days = athena_query('SELECT DISTINCT "date" AS day FROM queue_accounts ORDER BY 1')["day"].astype(str).tolist()
        if not days:
            raise DataMissing("The Athena table `queue_accounts` has no rows. Run `athena/04_queue_table.sql` "
                              "and check its LOCATION points at the queue run.")
        return days
    pattern = f"{config.QUEUE_DIR}/date=*/accounts.csv"
    _require(pattern, "A BQ1 queue (date=<day>/accounts.csv)")
    files = query("SELECT file FROM glob(?)", [pattern]).file
    return sorted({m.group(1) for f in files if (m := re.search(r"date=(\d{4}-\d{2}-\d{2})", f))})


@st.cache_data(show_spinner="Loading the review queue...")
def load_queue(day: str) -> pd.DataFrame:
    if config.USES_ATHENA:
        df = athena_query(
            'SELECT "rank", bank_id, account_id, score, model_version, window_start, cutoff, '
            f'reason_1, reason_2, reason_3 FROM queue_accounts WHERE "date" = {_lit_day(day)} ORDER BY "rank"')
        if df.empty:
            raise DataMissing(f"The Athena table `queue_accounts` has no accounts for {day}.")
        for col in ("bank_id", "account_id"):
            df[col] = df[col].str.strip()
        df["score"] = df["score"].astype(float)
        df["score_exact"] = df["score"].map(lambda v: repr(float(v)))
        return df.sort_values("rank").reset_index(drop=True)
    path = f"{config.QUEUE_DIR}/date={day}/accounts.csv"
    _require(path, f"The ranked accounts file for {day} (accounts.csv)")
    df = query(
        f"SELECT * FROM read_csv('{path}', types={{'bank_id': 'VARCHAR', 'account_id': 'VARCHAR', "
        f"'reason_1': 'VARCHAR', 'reason_2': 'VARCHAR', 'reason_3': 'VARCHAR'}})"
    )
    df["bank_id"] = df["bank_id"].str.strip()
    df["account_id"] = df["account_id"].str.strip()
    # the score at full precision: neighbouring scores can differ only in late digits
    df["score_exact"] = df["score"].astype(float).map(lambda v: repr(float(v)))
    return df.sort_values("rank").reset_index(drop=True)


def _read_text(path: str) -> str | None:
    if not exists(path):
        return None
    return str(query("SELECT content FROM read_text(?)", [path]).content[0])


def _read_json(path: str) -> dict | None:
    text = _read_text(path)
    return json.loads(text) if text else None


@st.cache_data(show_spinner=False)
def load_ranking_meta(day: str) -> dict:
    if config.USES_ATHENA:
        return _s3_json(f"{queue_uri()}/date={day}/ranking.json") or {}
    return _read_json(f"{config.QUEUE_DIR}/date={day}/ranking.json") or {}


METRICS_FILES = {"models": "models.json", "rules": "rules.json", "rules_tuned": "rules_tuned.json"}


@st.cache_data(show_spinner=False)
def load_metrics() -> dict[str, dict | None]:
    """The BQ1 evaluation files (the models, the label-free rules, the best-case rules).

    They come from the training and baseline jobs, which read labels; the score job that
    writes the queue does not, so the queue folder has no evaluation of its own."""
    if config.USES_ATHENA:
        model = (load_ranking_meta(queue_days()[-1]).get("model") or "").rstrip("/")
        uris = {"models": f"{model}/metrics.json" if model else None,
                "rules": f"{config.RULES_URI}/metrics.json",
                "rules_tuned": f"{config.RULES_TUNED_URI}/metrics.json"}
        return {name: (_s3_json(uri) if uri else None) for name, uri in uris.items()}
    root = config.METRICS_DIR
    return {name: (_read_json(f"{root}/{file}") if root else None) for name, file in METRICS_FILES.items()}


# --------------------------------------------------------------------------- rings

@st.cache_data(show_spinner="Loading laundering rings...")
def load_rings() -> pd.DataFrame:
    if config.USES_ATHENA:
        return athena_query(
            "SELECT ring_id, pattern_type, sender_bank_id, sender_account_number, "
            "receiver_bank_id, receiver_account_number FROM rings")
    pattern = f"{config.CURATED_DIR}/rings/*.parquet"
    _require(pattern, "The rings table")
    return query(
        f"SELECT ring_id, pattern_type, sender_bank_id, sender_account_number, "
        f"receiver_bank_id, receiver_account_number FROM read_parquet('{pattern}')"
    )


@st.cache_data(show_spinner=False)
def ring_membership() -> pd.DataFrame:
    """One row per account that appears in a ring, with the pattern types it appears in."""
    rings = load_rings()
    sides = pd.concat([
        rings[["ring_id", "pattern_type", "sender_bank_id", "sender_account_number"]]
        .set_axis(["ring_id", "pattern_type", "bank_id", "account_id"], axis=1),
        rings[["ring_id", "pattern_type", "receiver_bank_id", "receiver_account_number"]]
        .set_axis(["ring_id", "pattern_type", "bank_id", "account_id"], axis=1),
    ]).drop_duplicates()
    return (
        sides.groupby(["bank_id", "account_id"])["pattern_type"]
        .agg(lambda s: ", ".join(sorted(set(s))))
        .rename("ring_pattern")
        .reset_index()
    )


# --------------------------------------------------------------------------- transfers

def _lit(value: str) -> str:
    try:
        return athena.literal(value)
    except ValueError as err:
        raise DataMissing(str(err)) from err


def _lit_day(value: str) -> str:
    try:
        return athena.day(value)
    except ValueError as err:
        raise DataMissing(str(err)) from err


def _transfer_sql(date_from: str, date_to: str, condition: str, tail: str) -> str:
    """One query for both back ends: transfers in the date range that match ``condition``, each with its
    ground-truth ``laundering_mark`` (0 where there is no label row). The labels are joined here, not
    fetched afterwards, so Athena scans the transfers once and the page makes no second round trip.
    The label table is evaluation-only (ADR-0002): the mark is shown on the page and never scored."""
    if config.USES_ATHENA:
        transfers, labels = "transfers", "transfer_labels"
    else:
        transfers = f"read_parquet('{config.CURATED_DIR}/transfers/**/*.parquet', hive_partitioning=true)"
        label_pattern = f"{config.CURATED_DIR}/transfer_labels/**/*.parquet"
        labels = (f"read_parquet('{label_pattern}', hive_partitioning=true)" if exists(label_pattern)
                  else "(SELECT CAST(NULL AS VARCHAR) AS transfer_id, 0 AS laundering_mark, "
                       "CAST(NULL AS VARCHAR) AS \"date\" WHERE false)")
    a, b = _lit_day(date_from), _lit_day(date_to)
    return (
        f"SELECT {TRANSFER_COLS}, COALESCE(l.laundering_mark, 0) AS laundering_mark\n"
        f"FROM {transfers} t\n"
        f'LEFT JOIN (SELECT transfer_id, laundering_mark FROM {labels} WHERE "date" BETWEEN {a} AND {b}) l\n'
        f"  ON l.transfer_id = t.transfer_id\n"
        f'WHERE t."date" BETWEEN {a} AND {b} AND ({condition})\n'
        f"{tail}")


def _run_transfers(sql: str) -> pd.DataFrame:
    if config.USES_ATHENA:
        return athena_query(sql)
    _require(f"{config.CURATED_DIR}/transfers/**/*.parquet", "The curated transfers")
    return query(sql)


@st.cache_data(show_spinner="Asking Athena for this account's transfers...")
def account_transfers(bank: str, account: str, date_from: str, date_to: str) -> pd.DataFrame:
    """Every transfer the account sent or received, oldest first, with its ground-truth mark."""
    b, a = _lit(bank), _lit(account)
    cond = (f"(t.sender_bank_id = {b} AND t.sender_account_number = {a}) "
            f"OR (t.receiver_bank_id = {b} AND t.receiver_account_number = {a})")
    return _run_transfers(_transfer_sql(date_from, date_to, cond, 'ORDER BY t."timestamp", t.transfer_id'))


@st.cache_data(show_spinner="Asking Athena for the counterparties' transfers...")
def neighbour_transfers(counterparties: pd.DataFrame, date_from: str, date_to: str, limit: int) -> pd.DataFrame:
    """The most recent transfers sent or received by the given counterparties (columns bank, acct)."""
    clauses = []
    for bank, acct in counterparties[["bank", "acct"]].itertuples(index=False):
        b, a = _lit(bank), _lit(acct)
        clauses.append(f"(t.sender_bank_id = {b} AND t.sender_account_number = {a})")
        clauses.append(f"(t.receiver_bank_id = {b} AND t.receiver_account_number = {a})")
    cond = " OR ".join(clauses) or "1 = 0"
    return _run_transfers(_transfer_sql(
        date_from, date_to, cond, f'ORDER BY t."timestamp" DESC, t.transfer_id LIMIT {int(limit)}'))


# --------------------------------------------------------------------------- sharing

@st.cache_data(show_spinner=False)
def load_comparison() -> pd.DataFrame | None:
    """Bank-view vs shared-view results from bq2/. None if comparison.csv is not in the sharing folder."""
    if config.USES_ATHENA:
        try:
            return athena_query(
                'SELECT bank_id, "view", k, reviewed, eligible, positive_accounts, true_positives, '
                "precision_at_k, recall_at_k, model FROM sharing_comparison")
        except DataMissing as err:
            if "sharing_comparison" in str(err) and "does not exist" in str(err).lower():
                return None   # athena/03_sharing_table.sql has not been run: the page shows its placeholder
            raise
    path = f"{config.SHARING_DIR}/comparison.csv" if config.SHARING_DIR else ""
    if not path or not exists(path):
        return None
    df = query(f"SELECT * FROM read_csv('{path}', types={{'bank_id': 'VARCHAR', 'view': 'VARCHAR'}})")
    return df
