from __future__ import annotations

import streamlit as st

from . import athena, config


def banner() -> None:
    if config.IS_SAMPLE:
        st.warning(
            "**Synthetic sample data.** Every number on this page comes from a generated dataset "
            "(`scripts/make_sample_data.py`), not from the real pipeline. Set `DASH_SOURCE=local` or `s3` "
            "to read real outputs (or `athena` to query Amazon Athena) - see the README.", icon=":material/science:")
    if config.USES_ATHENA:
        st.caption(f"Live from Amazon Athena - workgroup `{config.ATHENA_WORKGROUP}`, database `{config.ATHENA_DATABASE}`.")
    note = st.session_state.get("_s3_note")
    if note:
        st.caption(note)


def footer() -> None:
    """How much Athena has been asked this session. Athena bills by data scanned, so show it."""
    if config.USES_ATHENA and athena.STATS.queries:
        st.sidebar.caption(
            f"Athena this session: {athena.STATS.queries} queries, {athena.STATS.bytes_scanned / 1e9:,.2f} GB scanned "
            f"(about ${athena.STATS.bytes_scanned / 1e12 * 5:,.4f}). Repeat clicks are served from the cache.")


def require_config() -> None:
    if config.CONFIGURED:
        return
    st.error(
        "No data location is set. Either read Athena (`DASH_SOURCE=athena`), run with the sample data "
        "(`DASH_SOURCE=sample`) or point `DASH_ROOT` at a folder containing `queue/`, `curated/` and `sharing/` (see the README).")
    st.stop()


def stop_missing(err: Exception) -> None:
    st.error(f"{err}")
    st.caption("Check the folder names in the README, or run with `DASH_SOURCE=sample` to see the layout expected.")
    st.stop()


def pct(x: float, digits: int = 0) -> str:
    return "n/a" if x != x else f"{x * 100:.{digits}f}%"
