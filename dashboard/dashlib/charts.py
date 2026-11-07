"""Plotly figures. Colours are the validated reference palette: slot 1 blue, slot 2 orange,
and a neutral for everything that is not being called out. Series are never told apart by
colour alone: every multi-series chart has a legend, and every chart has a table view on
its page."""
from __future__ import annotations

import math

import networkx as nx
import numpy as np
import pandas as pd
import plotly.graph_objects as go

from .config import BLUE, NEUTRAL, ORANGE

GRID = "rgba(128,128,128,0.22)"


def _base(fig: go.Figure, height: int, legend: bool = True) -> go.Figure:
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=36, b=8),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        showlegend=legend, legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        hoverlabel=dict(align="left"),
    )
    return fig


def _bars(fig: go.Figure) -> go.Figure:
    fig.update_traces(marker_cornerradius=4)
    fig.update_layout(bargap=0.4, bargroupgap=0.08)
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor=GRID, zeroline=False)
    return fig


def queue_by_bank(queue: pd.DataFrame, top: int = 15) -> go.Figure:
    counts = queue.groupby("bank_id").size().sort_values(ascending=False).head(top)
    fig = go.Figure(go.Bar(
        x=counts.index.tolist(), y=counts.values, marker_color=BLUE, name="Accounts in queue",
        hovertemplate="Bank %{x}<br>%{y} accounts in the queue<extra></extra>"))
    fig.update_xaxes(type="category", title="Bank")
    fig.update_yaxes(title="Accounts in the queue")
    return _bars(_base(fig, 300, legend=False))


def pattern_bars(by_pattern: pd.DataFrame) -> go.Figure:
    d = by_pattern.sort_values("not_fully_visible", ascending=False)
    fig = go.Figure(go.Bar(
        x=d["pattern_type"], y=d["not_fully_visible"] * 100, marker_color=BLUE, name="Rings",
        text=[f"{v:.0f}%" for v in d["not_fully_visible"] * 100], textposition="outside", cliponaxis=False,
        customdata=np.stack([d["rings"], d["avg_best_share"] * 100], axis=1),
        hovertemplate=("%{x}<br>%{y:.0f}% of rings have no single bank seeing all of them"
                       "<br>%{customdata[0]} rings; best bank sees %{customdata[1]:.0f}% of a ring on average"
                       "<extra></extra>")))
    fig.update_yaxes(title="Rings no single bank can see in full (%)", range=[0, 112])
    fig.update_xaxes(type="category", title="Ring pattern")
    return _bars(_base(fig, 340, legend=False))


def comparison_bars(uplift: pd.DataFrame, metric: str) -> go.Figure:
    label = "Recall" if metric == "recall_at_k" else "Precision"
    fig = go.Figure()
    for view, colour, name in (("bank", BLUE, "Bank view (one bank alone)"), ("shared", ORANGE, "Shared view (banks pool data)")):
        fig.add_trace(go.Bar(
            x=uplift["bank_id"], y=uplift[f"{metric}_{view}"] * 100, name=name, marker_color=colour,
            hovertemplate="Bank %{x}<br>" + name + ": %{y:.1f}%<extra></extra>"))
    fig.update_layout(barmode="group")
    fig.update_xaxes(type="category", title="Bank")
    fig.update_yaxes(title=f"{label} at the review budget (%)")
    return _bars(_base(fig, 360))


# --------------------------------------------------------------------------- investigation

def _short(bank: str, acct: str) -> str:
    return f"{bank}:{acct}"


def network_figure(edges: pd.DataFrame, focus: tuple[str, str], support: dict | None, show_labels: bool) -> go.Figure:
    """edges: src_bank, src_acct, dst_bank, dst_acct, n, first, last, laundering (bool)."""
    fid = _short(*focus)
    g = nx.DiGraph()
    g.add_node(fid)
    for r in edges.itertuples():
        g.add_edge(_short(r.src_bank, r.src_acct), _short(r.dst_bank, r.dst_acct))
    n = max(g.number_of_nodes(), 1)
    pos = nx.spring_layout(g.to_undirected(), k=1.4 / math.sqrt(n), seed=7, iterations=120,
                           pos={fid: (0.0, 0.0)}, fixed=[fid])

    fig = go.Figure()
    # One trace per edge class, with None breaks between edges: hundreds of edges stay fast
    # (a plotly annotation per edge takes a minute to build at this size).
    classes = [("Transfer", NEUTRAL, 1.1, 0.7, False)]
    if show_labels:
        classes.append(("Known laundering transfer (ground truth)", ORANGE, 2.6, 1.0, True))
    mid_x, mid_y, mid_text = [], [], []
    for name, colour, width, opacity, hot_class in classes:
        xs, ys, hx, hy, angle = [], [], [], [], []
        for r in edges.itertuples():
            a, b = _short(r.src_bank, r.src_acct), _short(r.dst_bank, r.dst_acct)
            if a == b or (bool(r.laundering) and show_labels) != hot_class:
                continue
            (x0, y0), (x1, y1) = pos[a], pos[b]
            xs += [x0, x1, None]
            ys += [y0, y1, None]
            hx.append(x0 + 0.88 * (x1 - x0))
            hy.append(y0 + 0.88 * (y1 - y0))
            angle.append(math.degrees(math.atan2(x1 - x0, y1 - y0)))  # clockwise from "up"
            when = f"{r.first:%d %b %H:%M}" + ("" if r.n == 1 else f" to {r.last:%d %b %H:%M}")
            mid_x.append((x0 + x1) / 2)
            mid_y.append((y0 + y1) / 2)
            mid_text.append(f"{a} → {b}<br>{r.n} transfer{'s' if r.n != 1 else ''}, {when}"
                            + ("<br>known laundering transfer" if hot_class else ""))
        fig.add_trace(go.Scatter(x=xs, y=ys, mode="lines", name=name, opacity=opacity,
                                 line=dict(color=colour, width=width), hoverinfo="skip"))
        fig.add_trace(go.Scatter(
            x=hx, y=hy, mode="markers", showlegend=False, hoverinfo="skip",
            marker=dict(symbol="arrow", size=9 if not hot_class else 12, color=colour, angle=angle, angleref="up")))
    fig.add_trace(go.Scatter(x=mid_x, y=mid_y, mode="markers", marker=dict(size=16, opacity=0),
                             hovertext=mid_text, hoverinfo="text", showlegend=False))

    focus_bank = focus[0]
    groups = {"focus": [], "same": [], "other": []}
    for node in g.nodes:
        bank = node.split(":", 1)[0]
        groups["focus" if node == fid else "same" if bank == focus_bank else "other"].append(node)
    spec = {
        "focus": ("Account under review", BLUE, 26, 3),
        "same": ("Same bank as the account", BLUE, 12, 0),
        "other": ("Other bank", NEUTRAL, 12, 0),
    }
    for key, (name, colour, size, outline) in spec.items():
        nodes = groups[key]
        if not nodes:
            continue
        fig.add_trace(go.Scatter(
            x=[pos[v][0] for v in nodes], y=[pos[v][1] for v in nodes], mode="markers",
                        marker=dict(size=size, color=colour, line=dict(width=outline + 1.5, color="rgba(255,255,255,0.9)")),
            hovertext=[f"Bank {v.split(':')[0]}<br>Account {v.split(':')[1]}" for v in nodes], hoverinfo="text", name=name))
    if support:
        a, b = _short(support["src_bank"], support["src_acct"]), _short(support["dst_bank"], support["dst_acct"])
        if a in pos and b in pos and a != b:
            fig.add_trace(go.Scatter(
                x=[(pos[a][0] + pos[b][0]) / 2], y=[(pos[a][1] + pos[b][1]) / 2], mode="markers",
                marker=dict(symbol="star", size=18, color="#0b0b0b", line=dict(width=2, color="#ffffff")),
                hovertext=["Supporting transfer: the highest-scoring transfer behind this account's rank"],
                hoverinfo="text", name="Supporting transfer"))
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False, scaleanchor="x")
    return _base(fig, 560)


def timeline_figure(t: pd.DataFrame, focus: tuple[str, str], support_id: str | None, show_labels: bool) -> go.Figure:
    """t: one row per transfer of the account, with direction, counterparty, amount, label, score."""
    fig = go.Figure()
    amt = t["amount_paid"].clip(lower=1)
    t = t.assign(size=np.clip(5 + 3 * np.log10(amt), 8, 22))

    def hover(d: pd.DataFrame) -> list[str]:
        out = []
        for r in d.itertuples():
            score = "" if pd.isna(r.risk_score) else f"<br>risk score {r.risk_score:.3f}"
            out.append(f"{r.timestamp:%d %b %Y %H:%M}<br>{r.direction} · {r.counterparty}"
                       f"<br>{r.amount_paid:,.2f} {r.payment_currency} via {r.payment_format}{score}")
        return out

    hot = t["laundering"] & show_labels
    for mask, name, colour in ((~hot, "Transfer", BLUE), (hot, "Known laundering transfer (ground truth)", ORANGE)):
        d = t[mask]
        if d.empty:
            continue
        fig.add_trace(go.Scatter(
            x=d["timestamp"], y=d["direction"], mode="markers", name=name, hovertext=hover(d), hoverinfo="text",
            marker=dict(size=d["size"], color=colour, opacity=0.85, line=dict(width=1.5, color="rgba(255,255,255,0.9)"))))
    if support_id:
        d = t[t["transfer_id"] == support_id]
        if not d.empty:
            fig.add_trace(go.Scatter(
                x=d["timestamp"], y=d["direction"], mode="markers", name="Supporting transfer", hovertext=hover(d),
                hoverinfo="text", marker=dict(symbol="star", size=20, color="#0b0b0b", line=dict(width=2, color="#ffffff"))))
    fig.update_yaxes(categoryorder="array", categoryarray=["Outgoing", "Incoming"], gridcolor=GRID, title=None)
    fig.update_xaxes(showgrid=False, title="Time (marker size ~ amount)")
    return _base(fig, 300)
