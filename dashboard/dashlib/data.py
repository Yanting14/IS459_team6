"""All reads go through this module, so the pages never touch a file directly.

Every function returns a plain DataFrame or dict. Reads are cached for the session.
Account and bank IDs are always strings: bank IDs such as ``010`` lose their leading
zero if they are ever parsed as numbers.
"""
from __future__ import annotations

import json
import re

import duckdb
import pandas as pd
import streamlit as st

from . import config

TRANSFER_COLS = (
    "transfer_id, timestamp, date, sender_bank_id, sender_account_number, "
    "receiver_bank_id, receiver_account_number, amount_paid, payment_currency, "
    "amount_received, receiving_currency, payment_format"
)


class DataMissing(Exception):
    """A file the page needs is not there. The message says which one."""


@st.cache_resource(show_spinner=False)
def _connection() -> duckdb.DuckDBPyConnection:
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
    pattern = f"{config.QUEUE_DIR}/date=*/accounts.csv"
    _require(pattern, "A BQ1 queue (date=<day>/accounts.csv)")
    files = query("SELECT file FROM glob(?)", [pattern]).file
    return sorted({m.group(1) for f in files if (m := re.search(r"date=(\d{4}-\d{2}-\d{2})", f))})


@st.cache_data(show_spinner="Loading the review queue...")
def load_queue(day: str) -> pd.DataFrame:
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
    return _read_json(f"{config.QUEUE_DIR}/date={day}/ranking.json") or {}


METRICS_FILES = {"models": "models.json", "rules": "rules.json", "rules_tuned": "rules_tuned.json"}


@st.cache_data(show_spinner=False)
def load_metrics() -> dict[str, dict | None]:
    """The BQ1 evaluation files (the models, the label-free rules, the best-case rules).

    They come from the training and baseline jobs, which read labels; the score job that
    writes the queue does not, so the queue folder has no evaluation of its own."""
    root = config.METRICS_DIR
    return {name: (_read_json(f"{root}/{file}") if root else None) for name, file in METRICS_FILES.items()}


# --------------------------------------------------------------------------- rings

@st.cache_data(show_spinner="Loading laundering rings...")
def load_rings() -> pd.DataFrame:
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

def account_transfers(bank: str, account: str, date_from: str, date_to: str) -> pd.DataFrame:
    """Every transfer the account sent or received, oldest first."""
    pattern = f"{config.CURATED_DIR}/transfers/**/*.parquet"
    _require(pattern, "The curated transfers")
    return query(
        f"""SELECT {TRANSFER_COLS} FROM {_transfers()}
            WHERE date BETWEEN ? AND ?
              AND ((sender_bank_id = ? AND sender_account_number = ?)
                OR (receiver_bank_id = ? AND receiver_account_number = ?))
            ORDER BY timestamp""",
        [date_from, date_to, bank, account, bank, account],
    )


def neighbour_transfers(counterparties: pd.DataFrame, date_from: str, date_to: str, limit: int) -> pd.DataFrame:
    """Transfers sent or received by the given counterparties (columns bank, acct)."""
    return query(
        f"""SELECT {TRANSFER_COLS} FROM {_transfers()}
            WHERE date BETWEEN ? AND ?
              AND ((sender_bank_id, sender_account_number) IN (SELECT bank, acct FROM cps)
                OR (receiver_bank_id, receiver_account_number) IN (SELECT bank, acct FROM cps))
            ORDER BY timestamp DESC LIMIT {int(limit)}""",
        [date_from, date_to],
        frames={"cps": counterparties},
    )


def transfer_labels(ids: list[str], date_from: str, date_to: str) -> pd.DataFrame:
    """Ground truth for the given transfers. Evaluation only -- never a model input."""
    pattern = f"{config.CURATED_DIR}/transfer_labels/**/*.parquet"
    if not ids or not exists(pattern):
        return pd.DataFrame({"transfer_id": [], "laundering_mark": []})
    return query(
        f"""SELECT l.transfer_id, l.laundering_mark
            FROM read_parquet('{pattern}', hive_partitioning=true) l
            JOIN wanted w USING (transfer_id)
            WHERE l.date BETWEEN ? AND ?""",
        [date_from, date_to],
        frames={"wanted": pd.DataFrame({"transfer_id": ids})},
    )


# --------------------------------------------------------------------------- sharing

@st.cache_data(show_spinner=False)
def load_comparison() -> pd.DataFrame | None:
    """Bank-view vs shared-view results from bq2/. None if comparison.csv is not in the sharing folder."""
    path = f"{config.SHARING_DIR}/comparison.csv" if config.SHARING_DIR else ""
    if not path or not exists(path):
        return None
    df = query(f"SELECT * FROM read_csv('{path}', types={{'bank_id': 'VARCHAR', 'view': 'VARCHAR'}})")
    return df
