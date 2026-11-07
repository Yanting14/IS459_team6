"""Business question 1: which accounts should investigators review first?"""
import pandas as pd
import streamlit as st

from dashlib import charts, config, data, ui

# Published in docs/scoring-validation.md (retrospective, simulated data). Used only when the
# scoring run's own evaluation.json is not available.
DOCUMENTED = {
    "2022-09-14": dict(true_positives=822, positive_accounts=3524, eligible=900182),
    "2022-09-15": dict(true_positives=807, positive_accounts=3632, eligible=899565),
    "2022-09-16": dict(true_positives=723, positive_accounts=3757, eligible=1421148),
}

ui.banner()
ui.require_config()
st.title("Which accounts should investigators review first?")

try:
    queue = data.load_queue()
except data.DataMissing as err:
    ui.stop_missing(err)
meta = data.load_ranking_meta()
evaluation = data.load_evaluation()

# ---- headline numbers
window = str(queue["window_start"].iloc[0])[:10]
k = int(meta.get("requested_k", len(queue)))
source_note = None
if evaluation is None and window in DOCUMENTED and not config.IS_SAMPLE:
    d = DOCUMENTED[window]
    evaluation = dict(d, k=k, reviewed=k, precision_at_k=d["true_positives"] / k,
                      recall_at_k=d["true_positives"] / d["positive_accounts"])
    source_note = "Precision and recall are the figures recorded in `docs/scoring-validation.md` for this day."

c1, c2, c3, c4 = st.columns(4)
c1.metric("Accounts to review", f"{len(queue):,}", help="The daily review budget (K) the scoring run was given.")
c2.metric("Scoring day", window)
if evaluation:
    c3.metric(f"Precision @ {k:,}", ui.pct(evaluation["precision_at_k"], 1),
              help="Of the accounts in the queue, the share that really took part in a laundering transfer that day.")
    c4.metric(f"Recall @ {k:,}", ui.pct(evaluation["recall_at_k"], 1),
              help="Of all accounts that took part in laundering that day, the share the queue catches.")
else:
    c3.metric("Precision", "not published")
    c4.metric("Recall", "not published")
if evaluation:
    elig = evaluation.get("eligible")
    st.caption(
        f"The review budget covers {k:,} of {elig:,} eligible accounts that day. " if elig else ""
        "Scores rank accounts; they are not calibrated probabilities. These are retrospective results on "
        "simulated data, not a claim about real-world accuracy." + (f" {source_note}" if source_note else ""))
else:
    st.caption("No evaluation file for this run. Produce one with `python -m scoring.pipeline evaluate` "
               "and place `evaluation.json` next to `accounts.csv`.")

# ---- filters
with st.sidebar:
    st.header("Filters")
    banks = sorted(queue["bank_id"].unique())
    chosen = st.multiselect("Bank", banks, placeholder="All banks")
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
cols = ["rank", "bank_id", "account_id", "score"] + (["ring_pattern"] if show_rings else []) + ["supporting_transfer_id"]
table = view[cols].reset_index(drop=True)
event = st.dataframe(
    table, hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row", key="queue_table",
    column_config={
        "rank": st.column_config.NumberColumn("Rank", width="small"),
        "bank_id": st.column_config.TextColumn("Bank", width="small"),
        "account_id": st.column_config.TextColumn("Account"),
        "score": st.column_config.ProgressColumn("Risk score", format="%.3f",
                                                 min_value=float(queue["score"].min()), max_value=float(queue["score"].max())),
        "ring_pattern": st.column_config.TextColumn("Known ring pattern (ground truth)"),
        "supporting_transfer_id": st.column_config.TextColumn("Supporting transfer", help="The highest-scoring transfer behind the account's score."),
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

# ---- workload
st.subheader("Where the review work sits")
st.plotly_chart(charts.queue_by_bank(view if len(view) else queue), width="stretch")
with st.expander("Table view"):
    st.dataframe(view.groupby("bank_id").size().rename("accounts in queue").sort_values(ascending=False).reset_index(),
                 hide_index=True, width="stretch")

with st.expander("How this queue was built"):
    eligible_accounts = meta.get("eligible_accounts")
    lines = [
        f"- **Model:** {queue['model_version'].iloc[0]} (frozen graph neural network, not retrained)",
        f"- **Window:** {window}, midnight to midnight; cutoff {str(queue['cutoff'].iloc[0])[:10]}",
        f"- **Account score:** {meta.get('aggregation', 'maximum incident transfer score')}",
        f"- **Eligibility:** {meta.get('eligibility', 'a non-self transfer that day')}",
    ]
    if isinstance(eligible_accounts, int):
        lines.append(f"- **Eligible accounts:** {eligible_accounts:,}")
    st.write("\n".join(lines))
