"""Business question 1, part two: which transaction patterns justify the priority?"""
import pandas as pd
import streamlit as st

from dashlib import analysis, charts, data, ui

@st.cache_data(show_spinner="Laying out the graph...")
def network(edges, focus, show_labels):
    return charts.network_figure(edges, focus, None, show_labels)


ui.banner()
ui.require_config()
st.title("Why is this account on the list?")

try:
    days = data.queue_days()
except data.DataMissing as err:
    ui.stop_missing(err)

# ---- choose the day and account (a click on the queue page lands here already selected)
with st.sidebar:
    st.header("Account")
    saved = st.session_state.get("day")
    day = st.selectbox("Queue day", days, index=days.index(saved) if saved in days else 0)
    st.session_state["day"] = day
try:
    queue = data.load_queue(day)
except data.DataMissing as err:
    ui.stop_missing(err)
focus = st.session_state.get("focus")
labels = [f"#{r.rank} · bank {r.bank_id} · {r.account_id}" for r in queue.itertuples()]
index = 0
if focus:
    hit = queue.index[(queue.bank_id == focus[0]) & (queue.account_id == focus[1])]
    index = int(hit[0]) if len(hit) else 0

with st.sidebar:
    pick = st.selectbox("Queue position", range(len(queue)), index=index, format_func=lambda i: labels[i])
    st.header("What to show")
    visibility = st.radio(
        "Who is looking?", ["Shared view", "Bank view"],
        help="Shared view: every transfer from every bank. Bank view: only the transfers where the account's "
             "own bank is the sender's or the receiver's bank.")
    second_hop = st.toggle("Include counterparties' other transfers", value=True)
    n_expand = st.slider("Counterparties to expand", 3, 15, 6, disabled=not second_hop)
    per_cp = st.slider("Transfers shown per counterparty", 3, 30, 8, disabled=not second_hop,
                       help="Most recent first. Keeps the graph readable when a counterparty is very busy.")
    show_labels = st.toggle("Show known laundering labels", value=True,
                            help="Ground truth from the dataset. Evaluation only: the model never sees it.")

row = queue.iloc[pick]
bank, account = str(row.bank_id), str(row.account_id)
st.session_state["focus"] = (bank, account)
date_to = (pd.Timestamp(str(row.cutoff)) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
date_from = "2022-09-01"

# ---- the account's own transfers
try:
    own = data.account_transfers(bank, account, date_from, date_to)
except data.DataMissing as err:
    ui.stop_missing(err)
if own.empty:
    st.warning(
        f"No transfers found for bank {bank}, account {account}. If this is real data, check that the "
        "queue's `account_id` matches the curated `*_account_number` columns (see the README).")
    st.stop()

# ---- neighbours (second hop), and what the bank view hides
second = pd.DataFrame(columns=own.columns)
hidden = 0
if second_hop:
    is_out = (own.sender_bank_id == bank) & (own.sender_account_number == account)
    cp = pd.DataFrame({
        "bank": own.receiver_bank_id.where(is_out, own.sender_bank_id),
        "acct": own.receiver_account_number.where(is_out, own.sender_account_number)})
    cp = cp[~((cp.bank == bank) & (cp.acct == account))]
    top_cp = cp.value_counts().head(n_expand).reset_index()[["bank", "acct"]]
    if len(top_cp):
        second = data.neighbour_transfers(top_cp, date_from, date_to, limit=3000)
        second = second[~second.transfer_id.isin(own.transfer_id)]
        keep: set[str] = set()  # the most recent few transfers of each expanded counterparty
        for b, a in top_cp.itertuples(index=False):
            touches = second[((second.sender_bank_id == b) & (second.sender_account_number == a))
                             | ((second.receiver_bank_id == b) & (second.receiver_account_number == a))]
            keep.update(touches.sort_values("timestamp", ascending=False).head(per_cp).transfer_id)
        second = second[second.transfer_id.isin(keep)]
        visible = analysis.apply_bank_view(second, bank) if visibility == "Bank view" else second
        hidden = len(second) - len(visible)
        second = visible
edges_raw = pd.concat([own, second], ignore_index=True)

ids = edges_raw.transfer_id.tolist()
marks = data.transfer_labels(ids, date_from, date_to)
edges_raw = edges_raw.merge(marks, on="transfer_id", how="left")
edges_raw["laundering"] = edges_raw["laundering_mark"].fillna(0).astype(int) == 1

# ---- aggregate to one arrow per pair of accounts
agg = (edges_raw.groupby(["sender_bank_id", "sender_account_number", "receiver_bank_id", "receiver_account_number"])
       .agg(n=("transfer_id", "size"), first=("timestamp", "min"), last=("timestamp", "max"),
            laundering=("laundering", "max")).reset_index()
       .rename(columns={"sender_bank_id": "src_bank", "sender_account_number": "src_acct",
                        "receiver_bank_id": "dst_bank", "receiver_account_number": "dst_acct"}))

# ---- summary
left, right = st.columns([3, 2], gap="large")
with right:
    st.subheader("Summary")
    c1, c2 = st.columns(2)
    c1.metric("Queue rank", f"#{int(row['rank'])}")
    c2.metric("Risk score", f"{row.score:.3f}", help="A ranking value, not a probability.")
    non_self = own[~((own.sender_bank_id == own.receiver_bank_id) & (own.sender_account_number == own.receiver_account_number))]
    other_side = pd.concat([
        non_self.loc[non_self.sender_account_number == account, ["receiver_bank_id", "receiver_account_number"]].set_axis(["b", "a"], axis=1),
        non_self.loc[non_self.receiver_account_number == account, ["sender_bank_id", "sender_account_number"]].set_axis(["b", "a"], axis=1)])
    cross = float((non_self.sender_bank_id != non_self.receiver_bank_id).mean()) if len(non_self) else float("nan")
    c3, c4 = st.columns(2)
    c3.metric("Transfers in history", f"{len(own):,}", help=f"1 Sep to {date_to}, sent or received.")
    c4.metric("Distinct counterparties", f"{len(other_side.drop_duplicates()):,}")
    c5, c6 = st.columns(2)
    c5.metric("Cross-bank share", ui.pct(cross), help="Share of its transfers whose other side is at a different bank.")
    try:
        ring = data.ring_membership()
        match = ring[(ring.bank_id == bank) & (ring.account_id == account)]
        c6.metric("Known ring pattern", match.ring_pattern.iloc[0] if len(match) else "none",
                  help="Ground truth from the dataset's patterns file. Evaluation only.")
    except data.DataMissing:
        c6.metric("Known ring pattern", "n/a")
    if visibility == "Bank view":
        st.info(f"Bank view: bank {bank} sees its own account's transfers, but only {len(second):,} of "
                f"{len(second) + hidden:,} second-hop transfers shown in the shared view. {hidden:,} are hidden because they happen "
                "between other banks.", icon=":material/visibility_off:")
    elif second_hop:
        st.caption(f"Shared view: {len(second):,} second-hop transfers from the {n_expand} busiest counterparties "
                   f"(up to {per_cp} each).")
    st.markdown("**Why it was flagged**")
    st.markdown("\n".join(f"- {r}" for r in (row.reason_1, row.reason_2, row.reason_3) if isinstance(r, str) and r))
    st.caption("Descriptive flags, not model attributions: the account's three most extreme features on "
               f"{day} among that day's eligible accounts, from what its own bank can see.")

with left:
    st.subheader("Transaction graph")
    st.plotly_chart(network(agg, (bank, account), show_labels), width="stretch")

# ---- timeline
st.subheader("Timeline of this account's transfers")
own = own.merge(marks, on="transfer_id", how="left")
own["laundering"] = own["laundering_mark"].fillna(0).astype(int) == 1
outgoing = (own.sender_bank_id == bank) & (own.sender_account_number == account)
own["direction"] = outgoing.map({True: "Outgoing", False: "Incoming"})
own["counterparty"] = (own.receiver_bank_id + ":" + own.receiver_account_number).where(
    outgoing, own.sender_bank_id + ":" + own.sender_account_number)
st.plotly_chart(charts.timeline_figure(own, (bank, account), None, show_labels), width="stretch")
st.caption("The model scores the account, not single transfers, so no transfer carries a score of its own.")

with st.expander("Table view: this account's transfers"):
    shown = own[["timestamp", "direction", "counterparty", "amount_paid", "payment_currency", "payment_format"]
                + (["laundering"] if show_labels else [])]
    st.dataframe(shown.sort_values("timestamp", ascending=False), hide_index=True, width="stretch")
