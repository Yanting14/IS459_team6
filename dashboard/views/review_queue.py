"""Business question 1: which accounts should investigators review first?"""
import streamlit as st

from dashlib import analysis, charts, data, ui

ui.banner()
ui.require_config()
st.title("Which accounts should investigators review first?")

try:
    days = data.queue_days()
except data.DataMissing as err:
    ui.stop_missing(err)

# ---- filters (the day is shared with the investigation page)
with st.sidebar:
    st.header("Filters")
    saved = st.session_state.get("day")
    day = st.selectbox("Queue day", days, index=days.index(saved) if saved in days else 0,
                       help="Each day has its own queue, scored from that day's account features.")
    st.session_state["day"] = day

try:
    queue = data.load_queue(day)
    meta = data.load_ranking_meta(day)
    comparison = analysis.model_comparison(data.load_metrics(), day)
except data.DataMissing as err:
    ui.stop_missing(err)
gbt = comparison[comparison.approach == "Gradient-boosted trees"]

# ---- headline numbers
k = int(meta.get("k", len(queue)))
c1, c2, c3, c4 = st.columns(4)
c1.metric("Accounts to review", f"{len(queue):,}", help="The daily review budget (K) the scoring run was given.")
c2.metric("Scoring day", day)
if len(gbt):
    c3.metric(f"Precision @ {k:,}", ui.pct(gbt.precision_at_k.iloc[0], 1),
              help="Of the accounts in the queue, the share that really took part in a laundering transfer that day.")
    c4.metric(f"Recall @ {k:,}", ui.pct(gbt.recall_at_k.iloc[0], 1),
              help="Of all accounts that took part in laundering that day, the share the queue catches.")
else:
    c3.metric("Precision", "not published")
    c4.metric("Recall", "not published")
elig = meta.get("eligible")
st.caption(
    (f"The review budget covers {k:,} of {elig:,} eligible accounts that day. " if isinstance(elig, int) else "")
    + "The model is gradient-boosted trees trained on EMR, using only what the account's own bank can see. "
    "Scores rank accounts; they are not probabilities. These are retrospective results on simulated data, "
    "not a claim about real-world accuracy.")

with st.sidebar:
    banks = sorted(queue["bank_id"].unique())
    chosen = st.multiselect("Bank", banks, placeholder="All banks",
                            help="One bank's slice of the pooled queue: the queue ranks every bank's accounts "
                                 "together, and this keeps only the chosen banks' rows.")
    top_n = st.slider("Show top", min_value=10, max_value=len(queue), value=min(100, len(queue)), step=10 if len(queue) > 50 else 1)
    show_rings = st.toggle("Show known ring pattern (ground truth)", value=True,
                           help="From the dataset's patterns file. Evaluation only: the model never sees it.")

view = queue.head(top_n)
if chosen:
    view = view[view["bank_id"].isin(chosen)]

if show_rings:
    try:
        view = view.merge(data.ring_membership(), on=["bank_id", "account_id"], how="left")
        view["ring_pattern"] = view["ring_pattern"].fillna("-")
    except data.DataMissing:
        show_rings = False
        st.sidebar.caption("Rings table not found; pattern column hidden.")

# ---- the queue
st.subheader("Ranked queue")
if chosen:
    st.caption(f"Bank {', '.join(chosen)}: one bank's slice of the pooled queue. Ranks are positions in the "
               "queue across all banks.")
cols = ["rank", "bank_id", "account_id", "score_exact", "reason_1", "reason_2", "reason_3"] + (["ring_pattern"] if show_rings else [])
table = view[cols].reset_index(drop=True)
reason_help = "Descriptive flags, not model attributions: the account's most extreme features that day."
event = st.dataframe(
    table, hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row", key="queue_table",
    column_config={
        "rank": st.column_config.NumberColumn("Rank", width="small"),
        "bank_id": st.column_config.TextColumn("Bank", width="small"),
        "account_id": st.column_config.TextColumn("Account"),
        "score_exact": st.column_config.TextColumn(
            "Risk score (exact)", width="medium",
            help="The model's score at full precision, as written in accounts.csv. Rank order is the ranking; "
                 "equal scores are ordered by bank, then account."),
        "reason_1": st.column_config.TextColumn("Reason 1", help=reason_help),
        "reason_2": st.column_config.TextColumn("Reason 2", help=reason_help),
        "reason_3": st.column_config.TextColumn("Reason 3", help=reason_help),
        "ring_pattern": st.column_config.TextColumn("Known ring pattern (ground truth)"),
    })
selected = event.selection.rows
if selected:
    row = table.iloc[selected[0]]
    st.session_state["focus"] = (str(row["bank_id"]), str(row["account_id"]))
    left, right = st.columns([3, 1])
    left.write(f"Selected: rank **{int(row['rank'])}**, bank **{row['bank_id']}**, account **{row['account_id']}**")
    if right.button("Investigate this account", type="primary", icon=":material/hub:", width="stretch"):
        st.switch_page("views/account_investigation.py")
else:
    st.caption("Select a row to investigate that account's transfers.")

# ---- how the queue compares
st.subheader(f"How the queue compares, {day}")
if comparison.empty:
    st.caption("No metrics files found (`metrics/models.json`, `rules.json`, `rules_tuned.json`); see the README.")
else:
    st.dataframe(
        comparison[["approach", "precision_at_k", "recall_at_k", "true_positives", "positives"]],
        hide_index=True, width="stretch",
        column_config={
            "approach": st.column_config.TextColumn("Approach"),
            "precision_at_k": st.column_config.NumberColumn(f"Precision @ {k:,}", format="percent"),
            "recall_at_k": st.column_config.NumberColumn(f"Recall @ {k:,}", format="percent"),
            "true_positives": st.column_config.NumberColumn("Accounts with laundering found"),
            "positives": st.column_config.NumberColumn("Accounts with laundering that day"),
        })
    st.caption(
        f"Every approach reviews the same {k:,} accounts a day from the same eligible accounts. The rules use fixed "
        "thresholds from the training days; the best-case rules weigh the same rules using the validation days' "
        "labels, so they are an upper bound for hand-tuned rules. The test days were never used to choose anything.")

# ---- workload
st.subheader("Where the review work sits")
st.plotly_chart(charts.queue_by_bank(view if len(view) else queue), width="stretch")
with st.expander("Table view"):
    st.dataframe(view.groupby("bank_id").size().rename("accounts in queue").sort_values(ascending=False).reset_index(),
                 hide_index=True, width="stretch")

with st.expander("How this queue was built"):
    lines = [
        f"- **Model:** {queue['model_version'].iloc[0]} (gradient-boosted trees trained on EMR, using only what "
        "the account's own bank can see)",
        f"- **Window:** {day}, midnight to midnight; cutoff {str(queue['cutoff'].iloc[0])[:10]}",
        f"- **Order:** {meta.get('order', 'score descending, then bank_id, then account_number')}",
        "- **Eligibility:** an account with a non-self transfer that day",
        f"- **Reasons:** {meta.get('reasons', 'descriptive flags, not model attributions')}",
        f"- **Score:** {meta.get('score', 'a ranking value, not a probability')}",
    ]
    if isinstance(elig, int):
        lines.append(f"- **Eligible accounts:** {elig:,}")
    st.write("\n".join(lines))
ui.footer()
