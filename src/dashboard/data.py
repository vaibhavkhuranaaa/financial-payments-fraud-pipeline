"""Data access layer for the fraud-ops dashboard.

Three independent, individually-degradable data sources, each wrapped so a
failure never raises past this module (the Dash callbacks that call these
functions always get *something* — an empty DataFrame / `None` / a
zero-valued dict — never an exception):

1. **Bank DB** (`bank.scored_transactions`, `bank.fraud_alerts`, ... via
   `src.bank.db.get_engine()`) — SQL query builders live here as module-level
   string constants so tests can assert their shape without a live database;
   `fetch_*` functions run them and swallow any DB error into an empty frame.
2. **API `/metrics`** (`API_METRICS_URL`, Prometheus text format) — a small
   hand-rolled parser (no `prometheus_client` parser dependency needed for
   read-side) that turns `..._bucket{le="..."}` lines into a histogram and
   estimates p50/p95/p99 via linear interpolation within the containing
   bucket (the standard `histogram_quantile` approximation — coarse at the
   tails when buckets are wide, which is why buckets in `src.app` are dense
   below 100ms).
3. **Model card** (`models/metrics.json`) — read once per process into a
   small dict; missing file degrades to `None` values, never a crash.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass

import pandas as pd
import requests
from sqlalchemy import Engine, text

logger = logging.getLogger(__name__)

# --- SQL query builders (plain constants: testable without a live DB) ------
#
# Written for T-SQL (Azure SQL Edge / SQL Server) per src/bank/schema.sql:
# TOP instead of LIMIT, SYSUTCDATETIME()/DATEADD for time windows.

LIVE_FEED_QUERY = """
SELECT TOP 20
    event_time,
    card_token,
    merchant_name,
    amount,
    fraud_probability,
    decision
FROM bank.scored_transactions
ORDER BY scored_at DESC
"""

OPEN_ALERTS_QUERY = """
SELECT TOP 100
    a.alert_id,
    a.event_id,
    a.card_token,
    a.fraud_probability,
    a.amount,
    a.merchant_name,
    a.created_at,
    a.status,
    c.name AS customer_name,
    c.risk_tier,
    acc.credit_limit
FROM bank.fraud_alerts a
LEFT JOIN bank.cards ca ON ca.card_token = a.card_token
LEFT JOIN bank.accounts acc ON acc.account_id = ca.account_id
LEFT JOIN bank.customers c ON c.customer_id = acc.customer_id
WHERE a.status = 'open'
ORDER BY a.created_at DESC
"""

SCORE_DISTRIBUTION_QUERY = """
SELECT fraud_probability
FROM bank.scored_transactions
WHERE scored_at >= DATEADD(MINUTE, -60, SYSUTCDATETIME())
"""

THROUGHPUT_QUERY = """
SELECT
    DATEADD(MINUTE, DATEDIFF(MINUTE, 0, scored_at), 0) AS minute_bucket,
    COUNT(*) AS txn_count
FROM bank.scored_transactions
WHERE scored_at >= DATEADD(MINUTE, -30, SYSUTCDATETIME())
GROUP BY DATEADD(MINUTE, DATEDIFF(MINUTE, 0, scored_at), 0)
ORDER BY minute_bucket
"""

# `mcc_group` CASE mirrors dbt/macros/mcc_group.sql (itself mirroring
# src/pipeline/features.py::_mcc_group). Kept in sync BY HAND across three
# places (Python feature builder, dbt macro, this dashboard query) — there is
# no single source of truth at runtime, so if `_mcc_group` ever changes this
# CASE will silently drift from the model's own grouping. See
# docs/governance/lineage.md.
ALERT_MIX_QUERY = """
SELECT
    st.channel,
    CASE
        WHEN st.mcc IN (4111, 4112, 4121, 4131, 4411, 4511) THEN 'travel'
        WHEN st.mcc IN (5411, 5422, 5451, 5499) THEN 'grocery'
        WHEN st.mcc IN (6010, 6011, 6012, 4829) THEN 'cash'
        WHEN st.mcc IN (5310, 5311, 5300, 5964, 5965, 5966, 5967, 5968, 5969) THEN 'online_retail'
        WHEN st.mcc BETWEEN 3000 AND 3999 THEN 'travel'
        ELSE 'other'
    END AS mcc_group
FROM bank.fraud_alerts a
JOIN bank.scored_transactions st ON st.event_id = a.event_id
"""

COLD_CARD_SHARE_QUERY = """
SELECT
    DATEADD(MINUTE, DATEDIFF(MINUTE, 0, scored_at), 0) AS minute_bucket,
    CAST(SUM(CASE WHEN cold_card = 1 THEN 1 ELSE 0 END) AS FLOAT) / NULLIF(COUNT(*), 0) AS cold_share,
    COUNT(*) AS n
FROM bank.scored_transactions
WHERE scored_at >= DATEADD(MINUTE, -5, SYSUTCDATETIME())
GROUP BY DATEADD(MINUTE, DATEDIFF(MINUTE, 0, scored_at), 0)
ORDER BY minute_bucket
"""

STATS_QUERY = """
SELECT
    (SELECT COUNT(*) FROM bank.scored_transactions) AS total_scored,
    (SELECT COUNT(*) FROM bank.scored_transactions
        WHERE scored_at >= DATEADD(SECOND, -60, SYSUTCDATETIME())) AS last_60s,
    (SELECT COUNT(*) FROM bank.fraud_alerts WHERE status = 'open') AS open_alerts
"""

ALERT_ACTION_SQL = text(
    """
UPDATE bank.fraud_alerts
SET status = :status, reviewed_at = SYSUTCDATETIME()
WHERE alert_id = :alert_id AND status = 'open'
"""
)

_EMPTY_STATS = {"total_scored": 0, "last_60s": 0, "open_alerts": 0}


def _safe_read_sql(engine: Engine, query: str) -> pd.DataFrame:
    """Run `query` and return a DataFrame, or an empty one on ANY failure
    (DB down, table missing, network hiccup) — callers must never see an
    exception here; that's the whole point of a "graceful degradation"
    dashboard."""
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)
    except Exception:  # noqa: BLE001 - intentionally broad: any DB failure degrades, never crashes
        logger.warning("bank-db query failed; degrading to empty result", exc_info=True)
        return pd.DataFrame()


def fetch_live_feed(engine: Engine) -> pd.DataFrame:
    return _safe_read_sql(engine, LIVE_FEED_QUERY)


def fetch_open_alerts(engine: Engine) -> pd.DataFrame:
    return _safe_read_sql(engine, OPEN_ALERTS_QUERY)


def fetch_score_distribution(engine: Engine) -> pd.DataFrame:
    return _safe_read_sql(engine, SCORE_DISTRIBUTION_QUERY)


def fetch_throughput(engine: Engine) -> pd.DataFrame:
    return _safe_read_sql(engine, THROUGHPUT_QUERY)


def fetch_alert_mix(engine: Engine) -> pd.DataFrame:
    return _safe_read_sql(engine, ALERT_MIX_QUERY)


def fetch_cold_card_share(engine: Engine) -> pd.DataFrame:
    return _safe_read_sql(engine, COLD_CARD_SHARE_QUERY)


def fetch_stats(engine: Engine) -> dict:
    df = _safe_read_sql(engine, STATS_QUERY)
    if df.empty:
        return dict(_EMPTY_STATS)
    row = df.iloc[0]
    return {
        "total_scored": int(row["total_scored"]),
        "last_60s": int(row["last_60s"]),
        "open_alerts": int(row["open_alerts"]),
    }


def apply_alert_action(engine: Engine, alert_id: int, action: str) -> bool:
    """Update `bank.fraud_alerts.status` for `alert_id` per the dashboard's
    Confirm-fraud / Dismiss buttons. Returns True on success, False on any
    failure (never raises — callback degrades to "nothing happened, try
    again on next refresh")."""
    status = {"confirm": "confirmed_fraud", "dismiss": "dismissed"}.get(action)
    if status is None:
        logger.warning("unknown alert action %r for alert_id=%r", action, alert_id)
        return False
    try:
        with engine.begin() as conn:
            conn.execute(ALERT_ACTION_SQL, {"status": status, "alert_id": alert_id})
        return True
    except Exception:  # noqa: BLE001 - never let a write failure crash the dashboard
        logger.warning("failed to apply alert action alert_id=%r action=%r", alert_id, action, exc_info=True)
        return False


def mask_card_token(card_token: str | None) -> str:
    """`…{last 6 of token}` masking for the live feed / alerts table."""
    if not card_token:
        return "…unknown"
    return f"…{card_token[-6:]}"


# --- Prometheus text parsing (API_METRICS_URL) ------------------------------


@dataclass
class LatencyQuantiles:
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None
    request_total: int | None = None


_BUCKET_RE = re.compile(r'^(?P<metric>\w+)_bucket\{(?P<labels>[^}]*)\}\s+(?P<value>[0-9.e+-]+)\s*$')
_LE_RE = re.compile(r'le="([^"]+)"')
_COUNTER_RE = re.compile(r'^http_requests_total\{[^}]*\}\s+([0-9.e+-]+)\s*$')


def parse_histogram_buckets(prom_text: str, metric_name: str) -> list[tuple[float, float]]:
    """Parse `<metric_name>_bucket{le="..."} <count>` lines into a sorted
    list of `(le, cumulative_count)` pairs (Prometheus histogram buckets are
    always cumulative). `+Inf` sorts last as `float("inf")`."""
    buckets: list[tuple[float, float]] = []
    for line in prom_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = _BUCKET_RE.match(line)
        if not match or match.group("metric") != metric_name:
            continue
        le_match = _LE_RE.search(match.group("labels"))
        if not le_match:
            continue
        le_raw = le_match.group(1)
        le = float("inf") if le_raw == "+Inf" else float(le_raw)
        buckets.append((le, float(match.group("value"))))
    buckets.sort(key=lambda pair: pair[0])
    return buckets


def estimate_quantile(buckets: list[tuple[float, float]], quantile: float) -> float | None:
    """Linear-interpolation estimate of a quantile from cumulative histogram
    buckets (the standard `histogram_quantile` approximation): find the
    bucket whose cumulative count first reaches `quantile * total`, then
    interpolate linearly between the previous bucket boundary and this one.
    Coarse when buckets are wide relative to the true distribution — accepted
    here since `score_latency_seconds` buckets are dense below 100ms (see
    src/app.py SCORE_LATENCY histogram buckets)."""
    if not buckets:
        return None
    total = buckets[-1][1]
    if total <= 0:
        return None
    target = quantile * total
    prev_le, prev_count = 0.0, 0.0
    for le, count in buckets:
        if count >= target:
            if le == float("inf"):
                # target falls in the overflow bucket: no upper boundary to
                # interpolate against — report the last finite boundary.
                return prev_le
            bucket_span = count - prev_count
            if bucket_span <= 0:
                return le
            fraction = (target - prev_count) / bucket_span
            return prev_le + fraction * (le - prev_le)
        prev_le, prev_count = le, count
    return buckets[-1][0] if buckets[-1][0] != float("inf") else prev_le


def parse_latency_quantiles(prom_text: str) -> LatencyQuantiles:
    buckets = parse_histogram_buckets(prom_text, "score_latency_seconds")
    total_requests = None
    for line in prom_text.splitlines():
        m = _COUNTER_RE.match(line.strip())
        if m:
            total_requests = int(total_requests or 0) + int(float(m.group(1)))
    return LatencyQuantiles(
        p50=estimate_quantile(buckets, 0.50),
        p95=estimate_quantile(buckets, 0.95),
        p99=estimate_quantile(buckets, 0.99),
        request_total=total_requests,
    )


def fetch_latency_quantiles(metrics_url: str, timeout: float = 1.5) -> LatencyQuantiles:
    """Fetch and parse `API_METRICS_URL`. Any failure (API down, unreachable,
    timeout, malformed text) degrades to an all-`None` result — never
    raises."""
    try:
        response = requests.get(metrics_url, timeout=timeout)
        response.raise_for_status()
        return parse_latency_quantiles(response.text)
    except Exception:  # noqa: BLE001 - API is a soft dependency for the dashboard
        logger.info("could not fetch/parse %s; latency tile will show waiting state", metrics_url)
        return LatencyQuantiles()


# --- Model card (models/metrics.json) --------------------------------------


@dataclass
class ModelCard:
    pr_auc: float | None = None
    roc_auc: float | None = None
    loaded: bool = False


def load_model_card(path: str) -> ModelCard:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return ModelCard(pr_auc=data.get("pr_auc"), roc_auc=data.get("roc_auc"), loaded=True)
    except (OSError, json.JSONDecodeError):
        logger.warning("could not load model card at %s; model tile will show waiting state", path)
        return ModelCard()
