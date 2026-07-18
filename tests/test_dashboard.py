"""Unit tests for src.dashboard: Prometheus-text parsing, SQL query builders,
the alert-action callback's SQL, and the app factory.

Hermetic — no live bank DB, no live API. `create_app()` is exercised with a
mocked SQLAlchemy engine injected via the factory's override args (mirroring
`src.app.create_app`'s pattern), so importing/constructing the app never
touches a socket.
"""

from __future__ import annotations

import json
import os
import tempfile
from unittest.mock import MagicMock

import pandas as pd

from src.dashboard.data import (
    ALERT_MIX_QUERY,
    COLD_CARD_SHARE_QUERY,
    LIVE_FEED_QUERY,
    OPEN_ALERTS_QUERY,
    SCORE_DISTRIBUTION_QUERY,
    STATS_QUERY,
    THROUGHPUT_QUERY,
    apply_alert_action,
    estimate_quantile,
    fetch_stats,
    load_model_card,
    mask_card_token,
    parse_histogram_buckets,
    parse_latency_quantiles,
)

# --- Prometheus text parsing -------------------------------------------------

_PROM_TEXT = """
# HELP score_latency_seconds Server-side scoring path latency
# TYPE score_latency_seconds histogram
score_latency_seconds_bucket{le="0.001"} 5
score_latency_seconds_bucket{le="0.0025"} 20
score_latency_seconds_bucket{le="0.005"} 50
score_latency_seconds_bucket{le="0.01"} 90
score_latency_seconds_bucket{le="0.02"} 98
score_latency_seconds_bucket{le="0.03"} 99
score_latency_seconds_bucket{le="+Inf"} 100
score_latency_seconds_sum 0.8
score_latency_seconds_count 100
# HELP http_requests_total Total HTTP requests
# TYPE http_requests_total counter
http_requests_total{endpoint="score",status="200"} 95
http_requests_total{endpoint="score",status="400"} 5
"""


def test_parse_histogram_buckets_returns_sorted_cumulative_pairs() -> None:
    buckets = parse_histogram_buckets(_PROM_TEXT, "score_latency_seconds")
    assert buckets[0] == (0.001, 5.0)
    assert buckets[-1] == (float("inf"), 100.0)
    les = [le for le, _ in buckets]
    assert les == sorted(les)


def test_parse_histogram_buckets_ignores_other_metrics() -> None:
    buckets = parse_histogram_buckets(_PROM_TEXT, "some_other_metric")
    assert buckets == []


def test_estimate_quantile_p50_falls_within_bucket_range() -> None:
    buckets = parse_histogram_buckets(_PROM_TEXT, "score_latency_seconds")
    p50 = estimate_quantile(buckets, 0.50)
    # target = 50; cumulative hits exactly 50 at le=0.005 -> falls in [0.0025, 0.005]
    assert p50 is not None
    assert 0.0025 <= p50 <= 0.005


def test_estimate_quantile_p99_within_finite_range() -> None:
    buckets = parse_histogram_buckets(_PROM_TEXT, "score_latency_seconds")
    p99 = estimate_quantile(buckets, 0.99)
    assert p99 is not None
    assert p99 <= 0.03


def test_estimate_quantile_empty_buckets_returns_none() -> None:
    assert estimate_quantile([], 0.5) is None


def test_parse_latency_quantiles_end_to_end() -> None:
    result = parse_latency_quantiles(_PROM_TEXT)
    assert result.p50 is not None
    assert result.p95 is not None
    assert result.p99 is not None
    assert result.p50 <= result.p95 <= result.p99
    assert result.request_total == 100


def test_parse_latency_quantiles_malformed_text_degrades_gracefully() -> None:
    result = parse_latency_quantiles("not prometheus text at all\n???")
    assert result.p50 is None
    assert result.p95 is None
    assert result.p99 is None


# --- SQL query builders: shape assertions (no live DB) -----------------------


def test_live_feed_query_selects_expected_columns() -> None:
    for col in ("event_time", "card_token", "merchant_name", "amount", "fraud_probability", "decision"):
        assert col in LIVE_FEED_QUERY
    assert "bank.scored_transactions" in LIVE_FEED_QUERY
    assert "TOP" in LIVE_FEED_QUERY.upper()


def test_open_alerts_query_joins_customer_dims_and_filters_open() -> None:
    for col in ("alert_id", "customer_name", "risk_tier", "credit_limit", "card_token"):
        assert col in OPEN_ALERTS_QUERY
    assert "bank.fraud_alerts" in OPEN_ALERTS_QUERY
    assert "bank.cards" in OPEN_ALERTS_QUERY
    assert "bank.accounts" in OPEN_ALERTS_QUERY
    assert "bank.customers" in OPEN_ALERTS_QUERY
    assert "status = 'open'" in OPEN_ALERTS_QUERY


def test_score_distribution_query_targets_scored_transactions() -> None:
    assert "fraud_probability" in SCORE_DISTRIBUTION_QUERY
    assert "bank.scored_transactions" in SCORE_DISTRIBUTION_QUERY


def test_throughput_query_buckets_by_minute() -> None:
    assert "minute_bucket" in THROUGHPUT_QUERY
    assert "txn_count" in THROUGHPUT_QUERY
    assert "GROUP BY" in THROUGHPUT_QUERY.upper()


def test_alert_mix_query_exposes_channel_and_mcc_group() -> None:
    assert "channel" in ALERT_MIX_QUERY
    assert "mcc_group" in ALERT_MIX_QUERY
    # spot check a couple of MCC codes mirrored from dbt/macros/mcc_group.sql
    assert "4111" in ALERT_MIX_QUERY
    assert "'travel'" in ALERT_MIX_QUERY


def test_cold_card_share_query_windows_last_five_minutes() -> None:
    assert "cold_share" in COLD_CARD_SHARE_QUERY
    assert "cold_card" in COLD_CARD_SHARE_QUERY
    assert "-5" in COLD_CARD_SHARE_QUERY


def test_stats_query_exposes_total_last60s_and_open_alerts() -> None:
    for col in ("total_scored", "last_60s", "open_alerts"):
        assert col in STATS_QUERY


# --- fetch_* graceful degradation (mocked engine raising) -------------------


def test_fetch_stats_degrades_to_zeros_when_db_unreachable() -> None:
    engine = MagicMock()
    engine.connect.side_effect = Exception("simulated connection failure")
    stats = fetch_stats(engine)
    assert stats == {"total_scored": 0, "last_60s": 0, "open_alerts": 0}


# --- alert-action callback SQL (mocked engine) -------------------------------


def test_apply_alert_action_confirm_issues_expected_update() -> None:
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False

    ok = apply_alert_action(engine, alert_id=42, action="confirm")

    assert ok is True
    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    sql_text = str(call_args.args[0])
    params = call_args.args[1]
    assert "UPDATE bank.fraud_alerts" in sql_text
    assert "SET status" in sql_text
    assert "reviewed_at" in sql_text
    assert params == {"status": "confirmed_fraud", "alert_id": 42}


def test_apply_alert_action_dismiss_sets_dismissed_status() -> None:
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False

    apply_alert_action(engine, alert_id=7, action="dismiss")

    params = conn.execute.call_args.args[1]
    assert params == {"status": "dismissed", "alert_id": 7}


def test_apply_alert_action_unknown_action_is_a_no_op() -> None:
    engine = MagicMock()
    ok = apply_alert_action(engine, alert_id=1, action="explode")
    assert ok is False
    engine.begin.assert_not_called()


def test_apply_alert_action_swallows_db_errors() -> None:
    engine = MagicMock()
    engine.begin.side_effect = Exception("simulated write failure")
    ok = apply_alert_action(engine, alert_id=1, action="confirm")
    assert ok is False


# --- masking helper -----------------------------------------------------------


def test_mask_card_token_shows_only_last_six() -> None:
    token = "a" * 58 + "123456"
    assert mask_card_token(token) == "…123456"


def test_mask_card_token_handles_missing_token() -> None:
    assert mask_card_token(None) == "…unknown"
    assert mask_card_token("") == "…unknown"


# --- model card loader ---------------------------------------------------------


def test_load_model_card_reads_pr_auc_and_roc_auc() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "metrics.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"pr_auc": 0.0227, "roc_auc": 0.768}, f)
        card = load_model_card(path)
    assert card.loaded is True
    assert card.pr_auc == 0.0227
    assert card.roc_auc == 0.768


def test_load_model_card_missing_file_degrades_gracefully() -> None:
    card = load_model_card("/nonexistent/path/metrics.json")
    assert card.loaded is False
    assert card.pr_auc is None
    assert card.roc_auc is None


# --- app factory smoke test ---------------------------------------------------


def test_create_app_imports_and_builds_clean_with_mocked_engine() -> None:
    """Importing/building the Dash app must never touch a real DB or the
    network — a mocked engine that returns empty results on every query
    stands in for "tables not seeded yet"."""
    from src.dashboard.app import create_app

    engine = MagicMock()
    engine.connect.side_effect = Exception("no live DB in tests")

    app = create_app(engine=engine, metrics_url="http://unreachable.invalid:9/metrics")

    assert app is not None
    assert app.layout is not None


def test_render_live_feed_handles_empty_dataframe_without_raising() -> None:
    from src.dashboard.app import render_live_feed

    component = render_live_feed(pd.DataFrame())
    assert component is not None


def test_render_alerts_queue_handles_empty_dataframe_without_raising() -> None:
    from src.dashboard.app import render_alerts_queue

    component = render_alerts_queue(pd.DataFrame())
    assert component is not None


def test_build_score_distribution_figure_handles_empty_dataframe() -> None:
    from src.dashboard.app import build_score_distribution_figure

    fig = build_score_distribution_figure(pd.DataFrame())
    assert fig is not None
