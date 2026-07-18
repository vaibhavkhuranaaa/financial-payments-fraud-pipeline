"""Fraud-ops dashboard — Plotly Dash app (ticket 08).

Audience: a recruiter watching a 3-minute screen share, so clarity beats
density. Every panel degrades to a "waiting for data..." state instead of a
traceback when the bank DB is empty/unreachable or the API's /metrics is
unavailable — see src/dashboard/data.py for the soft-fail data layer this
app is built on.

Palette/marks/accessibility follow the `dataviz` skill (see
src/dashboard/assets/style.css for the CSS custom properties and
`_SERIES` below for the chart hex values — both are the same validated
default palette, referenced by role rather than raw hex wherever the two
overlap).
"""

from __future__ import annotations

import logging
import os
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from dash import ALL, Dash, Input, Output, callback_context, dcc, html
from dotenv import load_dotenv

from src.bank.db import get_engine
from src.dashboard.data import (
    ModelCard,
    apply_alert_action,
    fetch_alert_mix,
    fetch_cold_card_share,
    fetch_live_feed,
    fetch_open_alerts,
    fetch_score_distribution,
    fetch_stats,
    fetch_throughput,
    fetch_latency_quantiles,
    load_model_card,
    mask_card_token,
)

load_dotenv()

logger = logging.getLogger(__name__)

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# --- Env-driven configuration (defaults mirror .env.example) ---------------

API_METRICS_URL = os.environ.get("API_METRICS_URL", "http://api:8000/metrics")
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "8050"))
MODEL_METRICS_PATH = os.environ.get("MODEL_METRICS_PATH", os.path.join(_REPO_ROOT, "models", "metrics.json"))
REFRESH_INTERVAL_MS = int(os.environ.get("DASHBOARD_REFRESH_MS", "2000"))

# --- Chart palette (dataviz skill: validated default, see references/palette.md) ---
# Categorical slots 1-4 are the ones that clear the CVD/normal-vision floors
# under the "all pairs" pairlist (bar/mix charts here compare >2 categories
# at once) in BOTH light and dark; slots past 4 fold into "other" rather than
# spawning a 5th hue.
_SERIES_BLUE = "#2a78d6"
_SERIES_GREEN = "#008300"
_SERIES_MAGENTA = "#e87ba4"
_SERIES_YELLOW = "#eda100"
_CATEGORICAL = [_SERIES_BLUE, _SERIES_GREEN, _SERIES_MAGENTA, _SERIES_YELLOW]

_STATUS_GOOD = "#0ca30c"
_STATUS_CRITICAL = "#d03b3b"
_STATUS_WARNING = "#fab219"

_INK_SECONDARY = "#52514e"
_GRIDLINE = "#e1e0d9"

_BASE_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="system-ui, -apple-system, 'Segoe UI', sans-serif", color=_INK_SECONDARY, size=12),
    margin=dict(l=48, r=16, t=16, b=36),
    height=260,
)


def _empty_figure(message: str = "waiting for data...") -> go.Figure:
    fig = go.Figure()
    fig.update_layout(**_BASE_LAYOUT)
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    fig.add_annotation(
        text=message,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font=dict(size=13, color=_INK_SECONDARY),
    )
    return fig


def _styled_axes(fig: go.Figure) -> go.Figure:
    fig.update_xaxes(showgrid=False, showline=True, linecolor=_GRIDLINE, ticks="outside", tickcolor=_GRIDLINE)
    fig.update_yaxes(showgrid=True, gridcolor=_GRIDLINE, zeroline=False)
    return fig


# --- Figure builders ---------------------------------------------------------


def build_score_distribution_figure(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return _empty_figure("no scored transactions yet")
    fig = go.Figure(
        data=[
            go.Histogram(
                x=df["fraud_probability"],
                marker=dict(color=_SERIES_BLUE),
                nbinsx=30,
            )
        ]
    )
    fig.update_layout(**_BASE_LAYOUT)
    fig.update_yaxes(type="log", title="count (log)")
    fig.update_xaxes(title="fraud probability")
    return _styled_axes(fig)


def build_throughput_figure(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return _empty_figure("no throughput in the last 30 minutes")
    fig = go.Figure(
        data=[
            go.Scatter(
                x=df["minute_bucket"],
                y=df["txn_count"],
                mode="lines",
                line=dict(color=_SERIES_BLUE, width=2, shape="spline"),
                fill="tozeroy",
                fillcolor="rgba(42,120,214,0.12)",
            )
        ]
    )
    fig.update_layout(**_BASE_LAYOUT)
    fig.update_yaxes(title="txns / min")
    fig.update_xaxes(title="time")
    return _styled_axes(fig)


def _grouped_bar(df: pd.DataFrame, group_col: str, empty_message: str) -> go.Figure:
    if df.empty or group_col not in df.columns:
        return _empty_figure(empty_message)
    counts = df[group_col].fillna("unknown").value_counts()
    fig = go.Figure(
        data=[
            go.Bar(
                x=counts.index.tolist(),
                y=counts.values.tolist(),
                marker=dict(color=_CATEGORICAL[: len(counts)] if len(counts) <= 4 else _SERIES_BLUE),
            )
        ]
    )
    fig.update_layout(**_BASE_LAYOUT)
    fig.update_yaxes(title="open alerts")
    return _styled_axes(fig)


def build_alert_mix_channel_figure(df: pd.DataFrame) -> go.Figure:
    return _grouped_bar(df, "channel", "no open alerts yet")


def build_alert_mix_mcc_figure(df: pd.DataFrame) -> go.Figure:
    return _grouped_bar(df, "mcc_group", "no open alerts yet")


def build_cold_card_figure(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return _empty_figure("no scoring activity in the last 5 minutes")
    fig = go.Figure(
        data=[
            go.Scatter(
                x=df["minute_bucket"],
                y=df["cold_share"].fillna(0.0) * 100.0,
                mode="lines+markers",
                line=dict(color=_STATUS_WARNING, width=2),
                marker=dict(size=8),
            )
        ]
    )
    fig.update_layout(**_BASE_LAYOUT)
    fig.update_yaxes(title="cold-card share (%)", range=[0, 100])
    fig.update_xaxes(title="time")
    return _styled_axes(fig)


# --- Small HTML component helpers -------------------------------------------


def _stat_tile(label: str, value: str, sublabel: str | None = None, tile_id: str | None = None) -> html.Div:
    children = [html.Div(label, className="stat-label"), html.Div(value, className="stat-value")]
    if sublabel:
        children.append(html.Div(sublabel, className="stat-sublabel"))
    return html.Div(children, className="stat-tile", id=tile_id) if tile_id else html.Div(children, className="stat-tile")


def _fmt_num(value: Any, suffix: str = "") -> str:
    if value is None:
        return "—"
    return f"{value:,}{suffix}" if isinstance(value, int) else f"{value:.3f}{suffix}"


def _fmt_ms(value: float | None) -> str:
    return "—" if value is None else f"{value * 1000:.1f} ms"


def render_stat_row(stats: dict, latency: Any, model_card: ModelCard) -> html.Div:
    return html.Div(
        [
            _stat_tile(
                "Transactions scored",
                f"{stats['total_scored']:,}",
                f"{stats['last_60s']:,} in last 60s",
            ),
            _stat_tile("Open alerts", f"{stats['open_alerts']:,}"),
            _stat_tile(
                "Scoring latency",
                _fmt_ms(latency.p50),
                f"p95 {_fmt_ms(latency.p95)} · p99 {_fmt_ms(latency.p99)}",
            ),
            _stat_tile(
                "Model (offline eval)",
                f"PR-AUC {model_card.pr_auc:.4f}" if model_card.pr_auc is not None else "—",
                f"ROC-AUC {model_card.roc_auc:.4f}" if model_card.roc_auc is not None else "model card unavailable",
            ),
        ],
        className="stat-row",
    )


def render_live_feed(df: pd.DataFrame) -> html.Div:
    if df.empty:
        return html.Div("waiting for scored transactions...", className="empty-state")
    header = html.Tr(
        [html.Th(h) for h in ["Time", "Card", "Merchant", "Amount", "Score", "Decision"]]
    )
    rows = []
    for _, r in df.iterrows():
        decision = str(r.get("decision", ""))
        decision_class = "pill-critical" if decision == "review" else "pill-good"
        rows.append(
            html.Tr(
                [
                    html.Td(str(r.get("event_time", ""))),
                    html.Td(mask_card_token(r.get("card_token"))),
                    html.Td(str(r.get("merchant_name", "") or "—")),
                    html.Td(f"${float(r.get('amount', 0.0)):,.2f}"),
                    html.Td(f"{float(r.get('fraud_probability', 0.0)):.4f}"),
                    html.Td(decision, className=decision_class),
                ]
            )
        )
    return html.Table([html.Thead(header), html.Tbody(rows)], className="data-table")


def render_alerts_queue(df: pd.DataFrame) -> html.Div:
    if df.empty:
        return html.Div("no open alerts — queue is clear", className="empty-state")
    cards = []
    for _, r in df.iterrows():
        alert_id = int(r["alert_id"])
        cards.append(
            html.Div(
                [
                    html.Div(
                        [
                            html.Span(f"${float(r.get('amount', 0.0)):,.2f}", className="alert-amount"),
                            html.Span(f"score {float(r.get('fraud_probability', 0.0)):.4f}", className="alert-score"),
                        ],
                        className="alert-row-top",
                    ),
                    html.Div(
                        f"{r.get('customer_name') or 'unknown customer'} · "
                        f"{r.get('risk_tier') or 'n/a'} risk · "
                        f"limit ${float(r['credit_limit']):,.0f}"
                        if pd.notna(r.get("credit_limit"))
                        else f"{r.get('customer_name') or 'unknown customer'} · {r.get('risk_tier') or 'n/a'} risk",
                        className="alert-meta",
                    ),
                    html.Div(
                        f"{r.get('merchant_name') or 'unknown merchant'} · {mask_card_token(r.get('card_token'))} · "
                        f"{r.get('created_at')}",
                        className="alert-meta",
                    ),
                    html.Div(
                        [
                            html.Button(
                                "Confirm fraud",
                                id={"type": "alert-action", "index": alert_id, "action": "confirm"},
                                className="btn btn-critical",
                                n_clicks=0,
                            ),
                            html.Button(
                                "Dismiss",
                                id={"type": "alert-action", "index": alert_id, "action": "dismiss"},
                                className="btn btn-muted",
                                n_clicks=0,
                            ),
                        ],
                        className="alert-actions",
                    ),
                ],
                className="alert-card",
            )
        )
    return html.Div(cards, className="alert-list")


# --- App factory --------------------------------------------------------------

_UNSET: Any = object()


def create_app(
    engine: Any = _UNSET,
    metrics_url: str | None = None,
    model_metrics_path: str | None = None,
) -> Dash:
    """Dash application factory.

    All three data-source dependencies are optional overrides (mirroring
    `src.app.create_app`'s sentinel pattern) so tests can inject a mocked
    SQLAlchemy engine / fake metrics URL / scratch model-card path without
    touching a real DB, network, or model directory. Production
    (`python -m src.dashboard.app`) calls `create_app()` with no arguments.
    """
    resolved_engine = get_engine() if engine is _UNSET else engine
    resolved_metrics_url = metrics_url if metrics_url is not None else API_METRICS_URL
    resolved_model_metrics_path = model_metrics_path if model_metrics_path is not None else MODEL_METRICS_PATH

    app = Dash(__name__, title="Fraud Ops Dashboard", update_title=None)
    app.layout = html.Div(
        [
            html.Div(
                [
                    html.H1("Fraud Ops Dashboard", className="page-title"),
                    html.Div("live view of scoring throughput, alerts, and model health", className="page-subtitle"),
                ],
                className="page-header",
            ),
            html.Div(id="stat-row-container"),
            html.Div(
                [
                    html.Div(
                        [html.H2("Live scored transactions", className="panel-title"), html.Div(id="live-feed-container")],
                        className="panel panel-live-feed",
                    ),
                    html.Div(
                        [html.H2("Open fraud alerts", className="panel-title"), html.Div(id="alerts-container")],
                        className="panel panel-alerts",
                    ),
                ],
                className="two-col",
            ),
            html.Div(
                [
                    html.Div(
                        [html.H2("Score distribution (last 60m)", className="panel-title"), dcc.Graph(id="score-dist-graph", config={"displayModeBar": False})],
                        className="panel",
                    ),
                    html.Div(
                        [html.H2("Throughput (last 30m)", className="panel-title"), dcc.Graph(id="throughput-graph", config={"displayModeBar": False})],
                        className="panel",
                    ),
                ],
                className="two-col",
            ),
            html.Div(
                [
                    html.Div(
                        [html.H2("Open alerts by channel", className="panel-title"), dcc.Graph(id="alert-mix-channel-graph", config={"displayModeBar": False})],
                        className="panel",
                    ),
                    html.Div(
                        [html.H2("Open alerts by MCC group", className="panel-title"), dcc.Graph(id="alert-mix-mcc-graph", config={"displayModeBar": False})],
                        className="panel",
                    ),
                    html.Div(
                        [
                            html.H2("Cold-card share (last 5m)", className="panel-title"),
                            dcc.Graph(id="cold-card-graph", config={"displayModeBar": False}),
                        ],
                        className="panel",
                    ),
                ],
                className="three-col",
            ),
            dcc.Interval(id="interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),
        ],
        className="app-root",
    )

    @app.callback(Output("stat-row-container", "children"), Input("interval", "n_intervals"))
    def _refresh_stats(_n: int) -> html.Div:
        stats = fetch_stats(resolved_engine)
        latency = fetch_latency_quantiles(resolved_metrics_url)
        model_card = load_model_card(resolved_model_metrics_path)
        return render_stat_row(stats, latency, model_card)

    @app.callback(Output("live-feed-container", "children"), Input("interval", "n_intervals"))
    def _refresh_live_feed(_n: int) -> html.Div:
        return render_live_feed(fetch_live_feed(resolved_engine))

    @app.callback(Output("score-dist-graph", "figure"), Input("interval", "n_intervals"))
    def _refresh_score_dist(_n: int):
        return build_score_distribution_figure(fetch_score_distribution(resolved_engine))

    @app.callback(Output("throughput-graph", "figure"), Input("interval", "n_intervals"))
    def _refresh_throughput(_n: int):
        return build_throughput_figure(fetch_throughput(resolved_engine))

    @app.callback(
        Output("alert-mix-channel-graph", "figure"),
        Output("alert-mix-mcc-graph", "figure"),
        Input("interval", "n_intervals"),
    )
    def _refresh_alert_mix(_n: int):
        df = fetch_alert_mix(resolved_engine)
        return build_alert_mix_channel_figure(df), build_alert_mix_mcc_figure(df)

    @app.callback(Output("cold-card-graph", "figure"), Input("interval", "n_intervals"))
    def _refresh_cold_card(_n: int):
        return build_cold_card_figure(fetch_cold_card_share(resolved_engine))

    @app.callback(
        Output("alerts-container", "children"),
        Input("interval", "n_intervals"),
        Input({"type": "alert-action", "index": ALL, "action": ALL}, "n_clicks"),
        prevent_initial_call=False,
    )
    def _refresh_alerts(_n: int, _clicks: list[int]) -> html.Div:
        triggered = callback_context.triggered_id
        if isinstance(triggered, dict) and triggered.get("type") == "alert-action":
            alert_id = triggered["index"]
            action = triggered["action"]
            apply_alert_action(resolved_engine, alert_id, action)
        return render_alerts_queue(fetch_open_alerts(resolved_engine))

    return app


app = create_app()
server = app.server


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=DASHBOARD_PORT, debug=False)
