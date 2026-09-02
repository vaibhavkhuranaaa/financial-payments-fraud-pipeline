"""Local fraud decision console.

THESIS: Put the policy equation before the dashboard and refuse the equal-card
operations template. OWN-WORLD: warm ledger surfaces, ruled sections, muted
blue controls, and outcome colors used only with text. STORY: choose threshold
and capacity, read the consequences, inspect the bounded queue. FIRST VIEWPORT:
policy controls occupy the left rail while captured, missed, and wasted review
work resolve on the right. FORM: an operational review ledger extending the
incumbent restrained dashboard shell.
"""

from __future__ import annotations

import math
import os
import re
import threading
import time
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, ctx, dash_table, dcc, html
from flask import jsonify, request

from src.dashboard.data import (
    ArtifactLoadError,
    ArtifactStore,
    format_count,
    format_ratio,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ARTIFACT_ROOT = REPO_ROOT / "artifacts"
THRESHOLD_STEP = 0.000001


class _TokenBucket:
    def __init__(self, rate: float, capacity: int) -> None:
        self.rate = rate
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.updated_at = time.monotonic()
        self.lock = threading.Lock()

    def allow(self) -> bool:
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.updated_at
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.updated_at = now
            if self.tokens < 1:
                return False
            self.tokens -= 1
            return True


def _empty_figure(message: str) -> go.Figure:
    figure = go.Figure()
    figure.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=24, r=12, t=20, b=32),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        annotations=[
            dict(
                text=message, x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False
            )
        ],
    )
    return figure


def build_score_figure(frame: pd.DataFrame, threshold: float) -> go.Figure:
    if frame.empty:
        return _empty_figure("No scored holdout is available")
    figure = go.Figure()
    labels = {0: ("Not fraud", "#728078"), 1: ("Observed fraud", "#a53f3f")}
    for observed, (name, color) in labels.items():
        subset = frame.loc[frame["Class"].eq(observed), "fraud_probability"]
        figure.add_trace(
            go.Histogram(
                x=subset,
                name=name,
                marker_color=color,
                opacity=0.78,
                nbinsx=55,
                hovertemplate="Score %{x:.3f}<br>Transactions %{y}<extra></extra>",
            )
        )
    figure.add_vline(x=threshold, line_width=2, line_dash="dash", line_color="#255f73")
    figure.update_layout(
        barmode="overlay",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Aptos, system-ui, sans-serif", color="#5d625a", size=12),
        legend=dict(orientation="h", y=1.12, x=0),
        margin=dict(l=52, r=16, t=36, b=44),
        xaxis=dict(
            title="Calibrated fraud probability", gridcolor="#e3e1da", zeroline=False
        ),
        yaxis=dict(
            title="Transactions, log scale",
            type="log",
            dtick=1,
            gridcolor="#e3e1da",
            tickfont=dict(size=11),
            zeroline=False,
        ),
        hoverlabel=dict(bgcolor="#171915", font_color="#fffdf8"),
    )
    return figure


def build_frontier_figure(
    frontier: pd.DataFrame, summary: dict[str, Any] | None = None
) -> go.Figure:
    if frontier.empty:
        return _empty_figure("The capacity frontier is unavailable")
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=frontier["reviews_per_1000"],
            y=frontier["recall"],
            mode="lines+markers",
            name="Fraud recall",
            line=dict(color="#174553", width=3),
            marker=dict(color="#fffdf8", line=dict(color="#174553", width=2), size=8),
            customdata=frontier[["review_count", "precision", "false_positive"]],
            hovertemplate=(
                "%{x:.2f} reviews / 1,000<br>"
                "%{customdata[0]:,.0f} reviews<br>"
                "Recall %{y:.1%}<br>"
                "Precision %{customdata[1]:.1%}<br>"
                "Non-fraud reviews %{customdata[2]:,.0f}<extra></extra>"
            ),
        )
    )
    if summary:
        figure.add_trace(
            go.Scatter(
                x=[summary["reviews_per_1000"]],
                y=[summary["recall"]],
                mode="markers",
                name="Current policy",
                marker=dict(
                    color="#c26b27",
                    size=13,
                    symbol="diamond",
                    line=dict(color="#fffdf8", width=2),
                ),
                hovertemplate="Current policy<br>%{x:.2f} reviews / 1,000<br>Recall %{y:.1%}<extra></extra>",
            )
        )
    figure.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Aptos, system-ui, sans-serif", color="#5d625a", size=12),
        legend=dict(orientation="h", y=1.12, x=0),
        margin=dict(l=58, r=22, t=42, b=52),
        xaxis=dict(
            title="Review capacity per 1,000 transactions",
            gridcolor="#e3e1da",
            zeroline=False,
        ),
        yaxis=dict(
            title="Fraud recall",
            tickformat=".0%",
            range=[0, 1],
            gridcolor="#e3e1da",
            zeroline=False,
        ),
        hoverlabel=dict(bgcolor="#171915", font_color="#fffdf8"),
    )
    return figure


def _metric(
    label: str, value_id: str, definition: str, tone: str = "neutral"
) -> html.Div:
    return html.Div(
        [
            html.Div(label, className="metric-label"),
            html.Div("Unavailable", id=value_id, className="metric-value"),
            html.Div(definition, className="metric-definition"),
        ],
        className=f"metric metric-{tone}",
    )


def _evidence_fact(label: str, value: str, detail: str) -> html.Div:
    return html.Div(
        [
            html.Div(label, className="evidence-label"),
            html.Strong(value),
            html.Span(detail),
        ],
        className="evidence-fact",
    )


def _strategy_context(
    store: ArtifactStore,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not store.ready:
        return None, None
    strategy = store.strategy_summary()
    targets = {
        round(float(row["target_recall"]), 2): row for row in strategy["recall_targets"]
    }
    return strategy, targets.get(0.80)


def _brief_copy(summary: dict[str, Any] | None) -> tuple[str, str]:
    if not summary:
        return (
            "Strategy evidence is unavailable.",
            "Build a complete model run to compare workload and fraud capture.",
        )
    captured = int(summary["true_positive"])
    missed = int(summary["false_negative"])
    total_fraud = captured + missed
    binding = str(summary["binding_control"]).capitalize()
    precision = summary["precision"]
    precision_copy = (
        f" at {format_ratio(precision)} precision"
        if precision is not None
        else "; precision is unavailable because the queue is empty"
    )
    return (
        f"{binding}: {captured} of {total_fraud} observed frauds captured.",
        f"The active {summary['reviews_per_1000']:.2f}-per-1,000 queue reviews "
        f"{format_count(summary['review_count'])} rows and delivers "
        f"{format_ratio(summary['recall'])} recall{precision_copy}. "
        f"{format_count(summary['false_positive'])} reviews are non-fraud; "
        f"{format_count(missed)} observed frauds remain outside the queue.",
    )


def _target_records(store: ArtifactStore) -> list[dict[str, Any]]:
    if not store.ready:
        return []
    return [
        {
            "target_recall": f"{row.target_recall:.0%}",
            "review_count": f"{int(row.review_count):,}",
            "reviews_per_1000": f"{row.reviews_per_1000:.2f}",
            "threshold": f"{row.threshold:.2%}",
            "precision": f"{row.precision:.1%}",
            "false_positive": f"{int(row.false_positive):,}",
        }
        for row in store.recall_targets.itertuples(index=False)
    ]


def _layout(store: ArtifactStore) -> html.Div:
    disabled = not store.ready
    error = store.error or ""
    strategy, _ = _strategy_context(store)
    current = strategy["current_policy"] if strategy else None
    model = strategy["model"] if strategy else None
    brief_title, brief_detail = _brief_copy(current)
    target_rows = _target_records(store)
    return html.Div(
        [
            html.Header(
                [
                    html.Div(
                        [
                            html.Div(
                                "Financial payments · retrospective holdout",
                                className="product-kicker",
                            ),
                            html.H1(
                                "Fraud strategy control room", className="page-title"
                            ),
                        ]
                    ),
                    html.Div(
                        [
                            html.Span("Run", className="run-label"),
                            html.Code(
                                store.manifest["run_id"]
                                if store.ready
                                else "not loaded",
                                id="run-id",
                            ),
                            html.Span(
                                "Ready" if store.ready else "Artifact error",
                                className="status status-ready"
                                if store.ready
                                else "status status-error",
                            ),
                        ],
                        className="run-context",
                    ),
                ],
                className="context-bar",
            ),
            html.Main(
                [
                    html.Section(
                        [
                            html.Div(
                                [
                                    html.Div("Director brief", className="brief-label"),
                                    html.H2(brief_title, id="brief-title"),
                                    html.P(brief_detail, id="brief-detail"),
                                ],
                                className="brief-copy",
                            ),
                            html.Div(
                                [
                                    _evidence_fact(
                                        "Selected model",
                                        str(model["selected"]).replace("_", " ").title()
                                        if model
                                        else "Unavailable",
                                        "Chosen on calibration evidence",
                                    ),
                                    _evidence_fact(
                                        "Held-out PR-AUC",
                                        f"{model['pr_auc']:.3f}"
                                        if model
                                        else "Unavailable",
                                        (
                                            f"95% bootstrap {model['pr_auc_ci_95']['low']:.3f}-"
                                            f"{model['pr_auc_ci_95']['high']:.3f}"
                                            if model
                                            else "No model evidence"
                                        ),
                                    ),
                                    _evidence_fact(
                                        "Holdout base rate",
                                        f"{model['test_fraud_rows'] / model['test_rows']:.3%}"
                                        if model
                                        else "Unavailable",
                                        f"{model['test_fraud_rows']:,} frauds in {model['test_rows']:,} rows"
                                        if model
                                        else "No holdout",
                                    ),
                                ],
                                className="brief-evidence",
                            ),
                        ],
                        className="strategy-brief",
                    ),
                    html.Section(
                        [
                            html.Div(
                                [
                                    html.H2("Set the review policy"),
                                    html.P(
                                        "Threshold controls eligibility. Capacity controls workload. The stricter control wins.",
                                        className="section-intro",
                                    ),
                                    html.Div(
                                        "Apply retrospective scenario",
                                        className="control-label scenario-label",
                                    ),
                                    html.Div(
                                        [
                                            html.Button(
                                                "Current",
                                                id="preset-current",
                                                disabled=disabled,
                                            ),
                                            html.Button(
                                                "80% recall",
                                                id="preset-80",
                                                disabled=disabled,
                                            ),
                                            html.Button(
                                                "85% recall",
                                                id="preset-85",
                                                disabled=disabled,
                                            ),
                                            html.Button(
                                                "90% recall",
                                                id="preset-90",
                                                disabled=disabled,
                                            ),
                                        ],
                                        className="scenario-buttons",
                                    ),
                                    html.P(
                                        "Scenario buttons use observed holdout outcomes and are diagnostic only.",
                                        className="control-help scenario-help",
                                    ),
                                    html.Label(
                                        "Minimum fraud probability",
                                        htmlFor="threshold-slider",
                                        className="control-label",
                                    ),
                                    html.Div(
                                        id="threshold-readout",
                                        className="control-readout",
                                    ),
                                    dcc.Slider(
                                        id="threshold-slider",
                                        min=0,
                                        max=1,
                                        step=THRESHOLD_STEP,
                                        value=store.default_threshold,
                                        marks={
                                            0: "0",
                                            0.25: ".25",
                                            0.5: ".50",
                                            0.75: ".75",
                                            1: "1",
                                        },
                                        disabled=disabled,
                                        tooltip={
                                            "placement": "bottom",
                                            "always_visible": False,
                                        },
                                    ),
                                    html.Label(
                                        "Reviews per 1,000 transactions",
                                        htmlFor="capacity-input",
                                        className="control-label",
                                    ),
                                    html.Div(
                                        id="capacity-readout",
                                        className="control-readout",
                                    ),
                                    dcc.Input(
                                        id="capacity-input",
                                        type="number",
                                        min=0,
                                        max=1000,
                                        step=0.01,
                                        value=store.default_capacity,
                                        disabled=disabled,
                                        inputMode="decimal",
                                    ),
                                    html.P(
                                        "Enter any capacity up to the full holdout. The frontier below shows where additional review work stops paying back.",
                                        className="control-help",
                                    ),
                                    html.Div(
                                        [
                                            html.Span("Binding control"),
                                            html.Strong(
                                                "Unavailable", id="binding-control"
                                            ),
                                        ],
                                        className="binding-note",
                                    ),
                                    html.Div(
                                        "Policy unavailable",
                                        id="policy-status",
                                        className="policy-status",
                                        role="status",
                                        **{"aria-live": "polite"},
                                    ),
                                ],
                                className="policy-controls",
                            ),
                            html.Div(
                                [
                                    html.Div(
                                        "Policy consequences",
                                        className="consequence-title",
                                    ),
                                    html.Div(
                                        [
                                            html.Section(
                                                [
                                                    html.H3("Detection coverage"),
                                                    html.Div(
                                                        [
                                                            _metric(
                                                                "Recall",
                                                                "metric-recall",
                                                                "Share of observed fraud captured.",
                                                                "good",
                                                            ),
                                                            _metric(
                                                                "Captured",
                                                                "metric-captured",
                                                                "Observed fraud inside the queue.",
                                                            ),
                                                            _metric(
                                                                "Missed",
                                                                "metric-missed",
                                                                "Observed fraud outside the queue.",
                                                                "critical",
                                                            ),
                                                        ],
                                                        className="metric-row",
                                                    ),
                                                ],
                                                className="metric-group",
                                            ),
                                            html.Section(
                                                [
                                                    html.H3("Review economics"),
                                                    html.Div(
                                                        [
                                                            _metric(
                                                                "Reviewed",
                                                                "metric-reviewed",
                                                                "Queue used versus capacity.",
                                                            ),
                                                            _metric(
                                                                "Precision",
                                                                "metric-precision",
                                                                "Fraud share of reviewed rows.",
                                                            ),
                                                            _metric(
                                                                "Non-fraud reviews",
                                                                "metric-false-positive",
                                                                "Capacity consumed without capture.",
                                                                "warning",
                                                            ),
                                                        ],
                                                        className="metric-row",
                                                    ),
                                                ],
                                                className="metric-group",
                                            ),
                                            html.Section(
                                                [
                                                    html.H3("Observed source amount"),
                                                    html.Div(
                                                        [
                                                            _metric(
                                                                "Amount recall",
                                                                "metric-amount-recall",
                                                                "Share of observed fraud amount captured.",
                                                            ),
                                                            _metric(
                                                                "Captured amount",
                                                                "metric-captured-amount",
                                                                "Source amount on captured fraud.",
                                                            ),
                                                            _metric(
                                                                "Missed amount",
                                                                "metric-missed-amount",
                                                                "Source amount on missed fraud.",
                                                                "critical",
                                                            ),
                                                        ],
                                                        className="metric-row",
                                                    ),
                                                ],
                                                className="metric-group",
                                            ),
                                        ],
                                        className="metric-groups",
                                    ),
                                ],
                                className="consequence-panel",
                            ),
                        ],
                        className="policy-stage",
                    ),
                    html.Div(
                        [
                            html.Strong("Artifacts could not be loaded."),
                            html.Span(error, id="artifact-error-detail"),
                            html.Code("make train", className="recovery-command"),
                        ],
                        id="artifact-error",
                        className="system-state system-state-error"
                        if disabled
                        else "system-state is-hidden",
                        role="alert",
                    ),
                    html.Section(
                        [
                            html.Section(
                                [
                                    html.Div(
                                        [
                                            html.H2("What additional capacity buys"),
                                            html.P(
                                                "Each point ranks the same scored holdout with no score floor. The diamond is the active policy."
                                            ),
                                        ],
                                        className="section-heading",
                                    ),
                                    html.Div(
                                        dcc.Graph(
                                            id="capacity-frontier",
                                            figure=(
                                                build_frontier_figure(
                                                    store.frontier, current
                                                )
                                                if store.ready
                                                else _empty_figure(error)
                                            ),
                                            config={
                                                "displayModeBar": False,
                                                "responsive": True,
                                            },
                                            className="graph-shell frontier-graph",
                                        ),
                                        id="capacity-frontier-region",
                                        role="img",
                                        **{
                                            "aria-label": (
                                                "Line chart of retrospective fraud recall against review capacity per 1,000 transactions. "
                                                "The current policy appears as a diamond; exact current values are stated above."
                                            )
                                        },
                                    ),
                                ],
                                className="frontier-panel",
                            ),
                            html.Section(
                                [
                                    html.Div(
                                        [
                                            html.H2(
                                                "Workload required by recall target"
                                            ),
                                            html.P(
                                                "Post-hoc holdout scenarios, not deployment recommendations."
                                            ),
                                        ],
                                        className="section-heading",
                                    ),
                                    dash_table.DataTable(
                                        id="target-table",
                                        columns=[
                                            {"name": "Recall", "id": "target_recall"},
                                            {
                                                "name": "Reviews",
                                                "id": "review_count",
                                            },
                                            {
                                                "name": "/ 1,000",
                                                "id": "reviews_per_1000",
                                            },
                                            {"name": "Score floor", "id": "threshold"},
                                            {"name": "Precision", "id": "precision"},
                                            {
                                                "name": "Non-fraud",
                                                "id": "false_positive",
                                            },
                                        ],
                                        data=target_rows,
                                        style_as_list_view=True,
                                        style_table={"overflowX": "auto"},
                                        style_cell={
                                            "minWidth": "62px",
                                            "width": "74px",
                                            "maxWidth": "96px",
                                        },
                                    ),
                                    html.P(
                                        "The gap from 85% to 90% is the saturation zone: a small capture gain requires a disproportionate queue expansion.",
                                        className="strategy-warning",
                                    ),
                                ],
                                className="target-panel",
                            ),
                        ],
                        className="strategy-grid",
                    ),
                    html.Details(
                        [
                            html.Summary("Model diagnostics · score distribution"),
                            html.Div(
                                [
                                    html.P(
                                        "Observed outcomes appear only because this is a retrospective holdout."
                                    ),
                                    html.Div(
                                        dcc.Graph(
                                            id="score-distribution",
                                            figure=(
                                                build_score_figure(
                                                    store.scored,
                                                    store.default_threshold,
                                                )
                                                if store.ready
                                                else _empty_figure(error)
                                            ),
                                            config={
                                                "displayModeBar": False,
                                                "responsive": True,
                                            },
                                            className="graph-shell diagnostic-graph",
                                        ),
                                        id="score-distribution-region",
                                        role="img",
                                        **{
                                            "aria-label": (
                                                "Overlaid logarithmic histograms compare calibrated scores for observed fraud and non-fraud transactions. "
                                                "A dashed line marks the selected threshold; exact policy consequences are stated above."
                                            )
                                        },
                                    ),
                                ],
                                className="diagnostic-content",
                            ),
                        ],
                        className="diagnostic-panel",
                        open=True,
                    ),
                    html.Section(
                        [
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.H2("Bounded review queue"),
                                            html.P(
                                                "Highest scores first. Ties resolve by source row ID."
                                            ),
                                        ],
                                        className="section-heading",
                                    ),
                                    html.Div(
                                        [
                                            html.Div(
                                                [
                                                    html.Div(
                                                        "Outcome",
                                                        className="queue-filter-name",
                                                    ),
                                                    dcc.RadioItems(
                                                        id="outcome-filter",
                                                        options=[
                                                            {
                                                                "label": "All",
                                                                "value": "all",
                                                                "disabled": disabled,
                                                            },
                                                            {
                                                                "label": "Captured",
                                                                "value": "captured_fraud",
                                                                "disabled": disabled,
                                                            },
                                                            {
                                                                "label": "False positive",
                                                                "value": "false_positive",
                                                                "disabled": disabled,
                                                            },
                                                        ],
                                                        value="all",
                                                        className="outcome-filter",
                                                        inputClassName="outcome-filter-input",
                                                        labelClassName="outcome-filter-label",
                                                    ),
                                                ],
                                                className="queue-filter-field",
                                            ),
                                            html.Div(
                                                [
                                                    html.Label(
                                                        "Minimum amount",
                                                        htmlFor="amount-min",
                                                        className="queue-filter-name",
                                                    ),
                                                    dcc.Input(
                                                        id="amount-min",
                                                        type="number",
                                                        min=0,
                                                        value=0,
                                                        disabled=disabled,
                                                    ),
                                                ],
                                                className="queue-filter-field",
                                            ),
                                            html.Div(
                                                [
                                                    html.Label(
                                                        "Maximum amount",
                                                        htmlFor="amount-max",
                                                        className="queue-filter-name",
                                                    ),
                                                    dcc.Input(
                                                        id="amount-max",
                                                        type="number",
                                                        min=0,
                                                        disabled=disabled,
                                                    ),
                                                ],
                                                className="queue-filter-field",
                                            ),
                                        ],
                                        className="queue-filters",
                                    ),
                                ],
                                className="queue-toolbar",
                            ),
                            html.Div(
                                id="queue-state",
                                className="system-state is-hidden",
                                role="status",
                            ),
                            dash_table.DataTable(
                                id="queue-table",
                                columns=[
                                    {"name": "Rank", "id": "rank", "type": "numeric"},
                                    {"name": "Score", "id": "score", "type": "numeric"},
                                    {"name": "Outcome", "id": "outcome"},
                                    {
                                        "name": "Amount",
                                        "id": "amount",
                                        "type": "numeric",
                                    },
                                    {
                                        "name": "Row ID",
                                        "id": "source_row_id",
                                        "type": "numeric",
                                    },
                                    {
                                        "name": "Elapsed time",
                                        "id": "elapsed",
                                        "type": "numeric",
                                    },
                                ],
                                data=[],
                                cell_selectable=True,
                                active_cell=None,
                                page_action="none",
                                sort_action="native",
                                export_format="none",
                                export_headers="display",
                                css=[
                                    {"selector": ".show-hide", "rule": "display: none"}
                                ],
                                style_as_list_view=True,
                                fixed_rows={"headers": True},
                                style_table={
                                    "overflowX": "auto",
                                    "overflowY": "auto",
                                    "maxHeight": "520px",
                                },
                                style_cell={
                                    "fontFamily": "Aptos, system-ui, sans-serif"
                                },
                            ),
                            html.Aside(
                                [
                                    html.Div(
                                        "Select a queue row to inspect its anonymized record and strongest model signals.",
                                        className="detail-empty",
                                    )
                                ],
                                id="record-detail",
                                className="record-detail",
                                **{"aria-live": "polite"},
                            ),
                        ],
                        className="queue-panel",
                    ),
                    html.Footer(
                        "Retrospective benchmark only. Source amount is not loss avoided. Scores support review analysis and do not authorize a payment action.",
                        className="product-footer",
                    ),
                ],
                className="app-main",
            ),
        ],
        className="app-root",
    )


def _queue_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    return [
        {
            "rank": int(row.rank),
            "source_row_id": int(row.source_row_id),
            "score": round(float(row.fraud_probability), 5),
            "amount": round(float(row.Amount), 2),
            "elapsed": int(row.Time),
            "outcome": str(row.outcome).replace("_", " "),
        }
        for row in frame.itertuples(index=False)
    ]


def _detail_component(record: dict[str, Any] | None) -> html.Div:
    if record is None:
        return html.Div(
            "The selected transaction is unavailable.", className="detail-empty"
        )
    signal_rows = [
        html.Li(
            [html.Code(signal["feature"]), html.Span(f"{signal['contribution']:+.3f}")],
            className="signal-row",
        )
        for signal in record["signals"]
    ]
    return html.Div(
        [
            html.Div(
                [
                    html.Div("Transaction detail", className="detail-kicker"),
                    html.H3(f"Source row {record['source_row_id']:,}"),
                ],
                className="detail-heading",
            ),
            html.Dl(
                [
                    html.Div(
                        [
                            html.Dt("Calibrated score"),
                            html.Dd(f"{record['fraud_probability']:.3%}"),
                        ]
                    ),
                    html.Div(
                        [
                            html.Dt("Score percentile"),
                            html.Dd(f"{record['score_percentile']:.2%}"),
                        ]
                    ),
                    html.Div(
                        [
                            html.Dt("Amount (source units)"),
                            html.Dd(f"{record['amount']:,.2f}"),
                        ]
                    ),
                    html.Div(
                        [
                            html.Dt("Elapsed seconds"),
                            html.Dd(f"{record['elapsed_seconds']:,.0f}"),
                        ]
                    ),
                    html.Div(
                        [
                            html.Dt("Observed class"),
                            html.Dd(
                                "Fraud" if record["observed_class"] else "Not fraud"
                            ),
                        ]
                    ),
                ],
                className="detail-facts",
            ),
            html.Div("Strongest model signals", className="detail-subtitle"),
            html.Ul(signal_rows, className="signal-list"),
            html.P(
                "Signal contributions describe this model's score. They are not causal explanations.",
                className="detail-caveat",
            ),
        ]
    )


def create_app(artifact_root: str | Path | None = None) -> Dash:
    root = Path(
        artifact_root or os.environ.get("FRAUD_ARTIFACT_ROOT", DEFAULT_ARTIFACT_ROOT)
    )
    store = ArtifactStore(root)
    app = Dash(
        __name__, title="Fraud Decision Workbench", suppress_callback_exceptions=True
    )
    app.index_string = app.index_string.replace("<html>", '<html lang="en">')
    app.layout = _layout(store)
    app.server.config["ARTIFACT_STORE"] = store
    source_sha = os.environ.get("SOURCE_SHA", "")
    release_ready = re.fullmatch(r"[0-9a-f]{40}", source_sha) is not None

    rate = float(os.environ.get("PUBLIC_RATE_LIMIT_RPS", "0"))
    burst = int(os.environ.get("PUBLIC_RATE_LIMIT_BURST", "40"))
    limiter = _TokenBucket(rate, burst) if rate > 0 and burst > 0 else None

    @app.server.before_request
    def enforce_public_rate_limit() -> Any:
        if limiter is None or limiter.allow():
            return None
        response = jsonify(
            {
                "error": "public request limit reached",
                "retry_after_seconds": 1,
            }
        )
        response.status_code = 429
        response.headers["Retry-After"] = "1"
        return response

    @app.server.get("/health")
    def health() -> Any:
        status = 200 if store.ready else 503
        return jsonify(
            {"status": "ready" if store.ready else "error", "detail": store.error}
        ), status

    @app.server.get("/api/release")
    def release() -> Any:
        status = 200 if release_ready else 503
        return jsonify(
            {
                "source_sha": source_sha if release_ready else None,
                "status": "ready" if release_ready else "error",
            }
        ), status

    @app.server.get("/api/metrics")
    def metrics() -> Any:
        if not store.ready:
            return jsonify({"error": store.error}), 503
        return jsonify(store.evaluation)

    @app.server.get("/api/summary")
    def summary_api() -> Any:
        try:
            threshold_arg = request.args.get("threshold")
            capacity_arg = request.args.get("reviews_per_1000")
            if threshold_arg is None and capacity_arg is None:
                if not store.ready:
                    raise ArtifactLoadError(
                        store.error or "model artifacts are unavailable"
                    )
                return jsonify(store.default_summary)
            threshold = float(threshold_arg or store.default_threshold)
            capacity = float(capacity_arg or store.default_capacity)
            return jsonify(store.decision_view(threshold, capacity).summary)
        except (ArtifactLoadError, ValueError) as exc:
            return jsonify({"error": str(exc)}), 400 if store.ready else 503

    @app.server.get("/api/strategy")
    def strategy_api() -> Any:
        try:
            return jsonify(store.strategy_summary())
        except ArtifactLoadError as exc:
            return jsonify({"error": str(exc)}), 503

    @app.server.get("/api/queue")
    def queue_api() -> Any:
        try:
            threshold = float(request.args.get("threshold", store.default_threshold))
            capacity = float(
                request.args.get("reviews_per_1000", store.default_capacity)
            )
            limit = max(0, min(int(request.args.get("limit", 100)), 1000))
            view = store.decision_view(threshold, capacity)
            return jsonify(
                {
                    "summary": view.summary,
                    "transactions": _queue_records(view.queue.head(limit)),
                }
            )
        except (ArtifactLoadError, ValueError) as exc:
            return jsonify({"error": str(exc)}), 400 if store.ready else 503

    @app.server.get("/api/transactions/<int:source_row_id>")
    def record_api(source_row_id: int) -> Any:
        try:
            record = store.record(source_row_id)
            return (
                (jsonify(record), 200)
                if record
                else (jsonify({"error": "transaction not found"}), 404)
            )
        except ArtifactLoadError as exc:
            return jsonify({"error": str(exc)}), 503

    @app.server.post("/api/score")
    def score_api() -> Any:
        try:
            payload = request.get_json(silent=True)
            if not isinstance(payload, dict):
                raise ValueError("request body must be a JSON object")
            return jsonify(store.score(payload))
        except (ArtifactLoadError, ValueError) as exc:
            return jsonify({"error": str(exc)}), 400 if store.ready else 503

    @app.callback(
        Output("threshold-slider", "value"),
        Output("capacity-input", "value"),
        Input("preset-current", "n_clicks"),
        Input("preset-80", "n_clicks"),
        Input("preset-85", "n_clicks"),
        Input("preset-90", "n_clicks"),
        prevent_initial_call=True,
    )
    def apply_scenario(*_: int | None) -> tuple[float, float]:
        if ctx.triggered_id == "preset-current":
            return store.default_threshold, store.default_capacity
        target_by_button = {"preset-80": 0.80, "preset-85": 0.85, "preset-90": 0.90}
        target = target_by_button.get(str(ctx.triggered_id))
        if target is None or store.recall_targets.empty:
            return store.default_threshold, store.default_capacity
        position = (store.recall_targets["target_recall"] - target).abs().idxmin()
        row = store.recall_targets.loc[position]
        capacity = math.ceil(float(row["reviews_per_1000"]) * 100) / 100
        threshold = max(0.0, float(row["threshold"]) - 1e-8)
        return threshold, capacity

    @app.callback(
        Output("brief-title", "children"),
        Output("brief-detail", "children"),
        Output("threshold-readout", "children"),
        Output("capacity-readout", "children"),
        Output("binding-control", "children"),
        Output("policy-status", "children"),
        Output("metric-reviewed", "children"),
        Output("metric-precision", "children"),
        Output("metric-captured", "children"),
        Output("metric-missed", "children"),
        Output("metric-false-positive", "children"),
        Output("metric-recall", "children"),
        Output("metric-amount-recall", "children"),
        Output("metric-captured-amount", "children"),
        Output("metric-missed-amount", "children"),
        Output("score-distribution", "figure"),
        Output("capacity-frontier", "figure"),
        Output("queue-table", "data"),
        Output("queue-table", "active_cell"),
        Output("queue-table", "export_format"),
        Output("queue-state", "children"),
        Output("queue-state", "className"),
        Input("threshold-slider", "value"),
        Input("capacity-input", "value"),
        Input("outcome-filter", "value"),
        Input("amount-min", "value"),
        Input("amount-max", "value"),
    )
    def update_policy(
        threshold: float | None,
        capacity: float | None,
        outcome: str | None,
        amount_min: float | None,
        amount_max: float | None,
    ) -> tuple[Any, ...]:
        threshold = store.default_threshold if threshold is None else float(threshold)
        capacity = store.default_capacity if capacity is None else float(capacity)
        if not store.ready:
            unavailable = "Unavailable"
            brief_title, brief_detail = _brief_copy(None)
            return (
                brief_title,
                brief_detail,
                f"{threshold:.1%}",
                f"{capacity:g}",
                unavailable,
                unavailable,
                unavailable,
                unavailable,
                unavailable,
                unavailable,
                unavailable,
                unavailable,
                unavailable,
                unavailable,
                unavailable,
                _empty_figure(store.error or "Artifacts unavailable"),
                _empty_figure(store.error or "Artifacts unavailable"),
                [],
                None,
                "none",
                "Build a complete model run with make train.",
                "system-state system-state-error",
            )
        view = store.decision_view(
            threshold,
            capacity,
            amount_min=0 if amount_min is None else amount_min,
            amount_max=amount_max,
            outcome=outcome or "all",
        )
        summary = view.summary
        empty_message = ""
        empty_class = "system-state is-hidden"
        if view.queue.empty:
            empty_message = (
                "Review capacity is zero. Increase it to create a queue."
                if capacity == 0
                else "No transactions match the current threshold and filters. Relax a control to continue."
            )
            empty_class = "system-state system-state-empty"
        elif len(view.queue) > 1000:
            empty_message = (
                f"Showing the highest-ranked 1,000 of {format_count(len(view.queue))} reviewed transactions. "
                "Policy metrics use the full queue."
            )
            empty_class = "system-state system-state-info"
        policy_status = (
            f"{str(summary['binding_control']).capitalize()} bound · "
            f"{format_count(summary['review_count'])} of {format_count(summary['capacity_limit'])} queue slots · "
            f"capacity ceiling {format_ratio(summary['capacity_recall'])} recall"
        )
        captured_amount = summary["captured_amount"]
        missed_amount = summary["missed_amount"]
        brief_title, brief_detail = _brief_copy(summary)
        return (
            brief_title,
            brief_detail,
            f"{threshold:.1%}",
            f"{capacity:g} / 1,000",
            str(summary["binding_control"]).capitalize(),
            policy_status,
            f"{format_count(summary['review_count'])} / {format_count(summary['capacity_limit'])}",
            format_ratio(summary["precision"]),
            format_count(summary["true_positive"]),
            format_count(summary["false_negative"]),
            format_count(summary["false_positive"]),
            format_ratio(summary["recall"]),
            format_ratio(summary["amount_recall"]),
            "Unavailable" if captured_amount is None else f"{captured_amount:,.2f}",
            "Unavailable" if missed_amount is None else f"{missed_amount:,.2f}",
            build_score_figure(view.frame, threshold),
            build_frontier_figure(store.frontier, summary),
            _queue_records(view.queue.head(1000)),
            None,
            "csv" if not view.queue.empty else "none",
            empty_message,
            empty_class,
        )

    @app.callback(
        Output("record-detail", "children"),
        Input("queue-table", "active_cell"),
        Input("queue-table", "data"),
    )
    def update_detail(
        active_cell: dict[str, Any] | None, rows: list[dict[str, Any]] | None
    ) -> html.Div:
        if not active_cell or not rows:
            return html.Div(
                "Select a reviewed transaction to inspect its anonymized record and strongest model signals. When the queue is empty, no record is available.",
                className="detail-empty",
            )
        position = int(active_cell["row"])
        if position >= len(rows):
            return _detail_component(None)
        return _detail_component(store.record(int(rows[position]["source_row_id"])))

    return app


app = create_app()
server = app.server


if __name__ == "__main__":
    app.run_server(
        host="127.0.0.1",
        port=int(os.environ.get("DASHBOARD_PORT", "8050")),
        debug=False,
    )
