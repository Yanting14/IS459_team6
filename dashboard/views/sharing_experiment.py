"""Business question 2: how much laundering is invisible to one bank, and what would sharing add?"""
import streamlit as st

from dashlib import analysis, charts, config, data, ui

ui.banner()
ui.require_config()
st.title("What does a single bank miss, and what would sharing add?")
st.markdown(
    "A **bank view** holds only the transfers where one bank is the sender's or the receiver's bank. "
    "The **shared view** holds every bank's transfers. The two sections below separate what can be "
    "measured from the data alone from what needs the scoring experiment.")

# =============================================================== 1. visibility
st.header("1. How much laundering can a single bank see?")
try:
    rings = data.load_rings()
except data.DataMissing as err:
    ui.stop_missing(err)
per_ring = analysis.ring_visibility(rings)
summary = analysis.visibility_summary(rings, per_ring)
by_pattern = analysis.by_pattern(per_ring)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Laundering rings", f"{summary['rings']:,}", help=f"{summary['transfers']:,} transfers in total.")
c2.metric("Rings no single bank sees in full", ui.pct(summary["share_invisible"], 1),
          help="No one bank is the sender's or the receiver's bank on every transfer of the ring.")
c3.metric("Best-placed bank sees, on average", ui.pct(summary["avg_best_share"], 0),
          help="For each ring, the share of its transfers touched by the bank that sees the most of it.")
c4.metric("Cross-bank laundering transfers", ui.pct(summary["share_cross_bank_transfers"], 0),
          help="Sender and receiver are at different banks, so neither bank sees the other side's account.")
st.caption(f"Median ring involves {summary['median_banks_per_ring']:.0f} banks.")

st.plotly_chart(charts.pattern_bars(by_pattern), width="stretch")
with st.expander("Table view"):
    shown = by_pattern.assign(
        not_fully_visible=(by_pattern.not_fully_visible * 100).round(1),
        avg_best_share=(by_pattern.avg_best_share * 100).round(1),
    ).rename(columns={"pattern_type": "Pattern", "rings": "Rings",
                      "not_fully_visible": "No single bank sees all (%)", "avg_best_share": "Best bank sees (%, average)"})
    st.dataframe(shown, hide_index=True, width="stretch")
st.info(
    "This is a **ceiling on visibility, not a detection result.** A bank that is party to a transfer sees "
    "its own side only; it still cannot see what the other account does at other banks. Whether sharing "
    "actually *catches* more laundering is measured in section 2.", icon=":material/info:")

# =============================================================== 2. detection
st.header("2. How much more would banks detect by sharing data?")
comparison = data.load_comparison()
if comparison is None:
    st.warning(
        "The bank-view vs shared-view results are not available yet, so this section has nothing to plot. "
        f"It reads `{config.SHARING_DIR or 'sharing'}/comparison.csv`, one row per bank and view:",
        icon=":material/hourglass_top:")
    st.code("bank_id,view,k,reviewed,eligible,positive_accounts,true_positives,precision_at_k,recall_at_k\n"
            "010,bank,30,30,4000,25,6,0.20,0.24\n010,shared,30,30,4000,25,14,0.47,0.56", language="csv")
    st.caption("Each row is the `evaluation.json` from `scoring.pipeline evaluate` for one run: `view` is `bank` "
               "for a run with `--bank`, `shared` for the matching run without it. Both runs must use the same "
               "window, budget and eligible accounts (ADR-0004).")
    st.stop()

try:
    uplift = analysis.uplift_table(comparison)
except ValueError as err:
    st.error(str(err))
    st.stop()

tp_bank, tp_shared = uplift.true_positives_bank.sum(), uplift.true_positives_shared.sum()
pos = uplift.positive_accounts_shared.sum()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Banks compared", f"{len(uplift):,}")
c2.metric("Caught, one bank alone", f"{int(tp_bank):,}")
c3.metric("Caught, shared data", f"{int(tp_shared):,}",
          delta=f"+{int(tp_shared - tp_bank):,}" if tp_shared >= tp_bank else f"{int(tp_shared - tp_bank):,}")
c4.metric("Recall, bank view to shared", f"{ui.pct(tp_bank / pos)} to {ui.pct(tp_shared / pos)}",
          help="Summed over the compared banks' own accounts, at the same review budget in both views.")

metric = st.radio("Measure", ["recall_at_k", "precision_at_k"], horizontal=True,
                  format_func=lambda m: "Recall (how much laundering is caught)" if m == "recall_at_k" else "Precision (how many reviews pay off)")
st.plotly_chart(charts.comparison_bars(uplift, metric), width="stretch")
with st.expander("Table view"):
    st.dataframe(uplift.rename(columns={
        "bank_id": "Bank", "true_positives_bank": "Caught (bank view)", "true_positives_shared": "Caught (shared view)",
        "extra_true_positives": "Extra caught by sharing", "recall_uplift": "Recall uplift"}),
        hide_index=True, width="stretch")
st.caption("Retrospective results on simulated data. Same accounts, window, review budget and model weights in "
           "both views; only the transfers each view can see differ.")
