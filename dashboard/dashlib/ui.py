from __future__ import annotations

import streamlit as st

from . import config


def banner() -> None:
    if config.IS_SAMPLE:
        st.warning(
            "**Synthetic sample data.** Every number on this page comes from a generated dataset "
            "(`scripts/make_sample_data.py`), not from the real pipeline. Set `DASH_SOURCE=local` or `s3` "
            "to read real outputs - see the README.", icon=":material/science:")
    note = st.session_state.get("_s3_note")
    if note:
        st.caption(note)


def require_config() -> None:
    if config.CONFIGURED:
        return
    st.error(
        "No data location is set. Either run with the sample data (`DASH_SOURCE=sample`) or point "
        "`DASH_ROOT` at a folder containing `queue/`, `curated/` and `sharing/` (see the README).")
    st.stop()


def stop_missing(err: Exception) -> None:
    st.error(f"{err}")
    st.caption("Check the folder names in the README, or run with `DASH_SOURCE=sample` to see the layout expected.")
    st.stop()


def pct(x: float, digits: int = 0) -> str:
    return "n/a" if x != x else f"{x * 100:.{digits}f}%"
