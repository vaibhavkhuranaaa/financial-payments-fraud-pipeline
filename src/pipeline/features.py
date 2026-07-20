"""Shared feature definitions + Spark Structured Streaming windowed-feature job.

Data flow
---------
This module is the **single source of truth for feature definitions**, imported
by both the streaming job (below) and the offline training builder
(``src/pipeline/train.py``). Sharing one module is the train/serve-skew
prevention mechanism called out in ``docs/adr/0001-stack-and-architecture.md``:
whatever a card's features look like online (Redis, updated by the streaming
job) must be bit-for-bit reproducible offline (pandas, at training time).

Two layers of features:

1. **Event-level enrichment** (``enrich``) — pure, stateless derivations from a
   single contract-v1 event: card-not-present/chip/swipe flags, cross-border
   flag, MCC grouping + raw MCC, current-event auth-error flag, log-amount,
   hour-of-day, day-of-week. No history required — all available at
   authorization time.
2. **Windowed per-card aggregates** (``CARD_WINDOWS``) — count, amount
   sum/mean, distinct merchant-city count, and decline-ish rate over the
   trailing 1 hour / 1 day / 7 days / 30 days of a card's activity, computed
   **strictly before** the event being scored (point-in-time correctness /
   no label leakage). TabFormer cards transact roughly daily, so windows
   under an hour are almost always empty; 1h/1d/7d/30d actually capture
   density variation, unlike the original 1m/10m/1h set.
3. **Point-in-time history features** computed alongside the windows:
   ``time_since_last_txn_s`` (seconds since the card's previous event, capped
   at the 30d window) and ``is_new_city_30d`` (merchant_city not seen for
   this card in the trailing 30 days), plus ``amount_over_mean_30d`` (current
   amount relative to the card's 30d average spend).

Streaming job (``run_stream``)
-------------------------------
Reads ``KAFKA_TOPIC_TRANSACTIONS`` via Spark Structured Streaming
(``spark-sql-kafka-0-10``), parses the JSON payload against an explicit schema
mirroring ``contracts/transaction.schema.json``, and re-validates required
fields even though the producer already validated once (defense in depth —
the stream must not trust the wire). **Quarantine strategy: invalid records
are written to a Delta table at ``{DELTA_ROOT}/_quarantine`` rather than
re-published to a Kafka DLQ topic** — this keeps the streaming job's only
external dependencies as Kafka (read) + Delta + Redis (write), with no second
Kafka producer needed inside `foreachBatch`, and gives the quarantine table
the same time-travel/audit properties as the rest of the lake.

Valid events are appended to Delta ``{DELTA_ROOT}/events``, and then — per
microbatch, in `foreachBatch` — each affected card's trailing-30d Delta
history is replayed through the SAME vectorized feature code the offline
trainer uses (``latest_card_feature_mappings`` →
``compute_card_features_vectorized``, the skew rule), producing that card's
current online feature hash, which is:
  * upserted into the Redis hash ``features:{card_token}`` (online store), and
  * appended to Delta ``{DELTA_ROOT}/card_features`` for offline reuse.

This is deliberately NOT a Spark sliding-window aggregation: v2 windows go
up to 30 days, and ``F.window(duration, slide=30s)`` semantics would create
tens of thousands of window instances per event. A microbatch is bounded and
TabFormer has O(thousands) of cards, so the bounded driver-side pandas pass
is the pragmatic `foreachBatch` escape hatch. ``time_since_last_txn_s`` and
``amount_over_mean_30d`` are never stored — ``build_feature_row`` derives
them at serving time from ``last_event_ts`` / ``amount_mean_30d`` in the
hash. ``is_new_city_30d`` defaults to "new" online when absent (documented
approximation; the offline trainer computes it exactly).
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from dotenv import load_dotenv

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd

load_dotenv()

# --- Env-driven configuration (defaults mirror .env.example) ---------------

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.environ.get("KAFKA_SASL_MECHANISM", "")
KAFKA_SASL_USERNAME = os.environ.get("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.environ.get("KAFKA_SASL_PASSWORD", "")
KAFKA_TOPIC_TRANSACTIONS = os.environ.get("KAFKA_TOPIC_TRANSACTIONS", "transactions")

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

DELTA_ROOT = os.environ.get("DELTA_ROOT", "data/delta")

# --- Shared feature spec (train/serve skew-prevention contract) ------------

# Trailing windows evaluated per card_token, in seconds. TabFormer cards
# transact roughly daily, so sub-hour windows are almost always empty; 1h is
# kept for burst detection, 1d/7d/30d capture the density signal that
# actually varies card-to-card.
CARD_WINDOWS: dict[str, int] = {"1h": 3600, "1d": 86400, "7d": 604800, "30d": 2592000}

# time_since_last_txn_s is capped at this many seconds (= the 30d window) so
# a dormant/new card doesn't produce an unbounded feature value.
_TIME_SINCE_LAST_TXN_CAP_SECONDS = float(CARD_WINDOWS["30d"])

# The per-window aggregate metrics computed for each window in CARD_WINDOWS.
_WINDOW_METRICS = ("txn_count", "amount_sum", "amount_mean", "distinct_merchant_city", "decline_rate")

# MCC -> spend-category grouping, encoded ordinally for numeric model input.
MCC_GROUP_IDS: dict[str, int] = {
    "travel": 0,
    "grocery": 1,
    "cash": 2,
    "online_retail": 3,
    "other": 4,
}

# Exact-code overrides layered on top of the range rules in `_mcc_group`.
_MCC_EXACT_GROUPS: dict[int, str] = {
    4111: "travel",  # local/suburban transportation
    4112: "travel",  # passenger railways
    4121: "travel",  # taxicabs/limousines
    4131: "travel",  # bus lines
    4411: "travel",  # cruise lines
    4511: "travel",  # airlines
    5411: "grocery",
    5422: "grocery",  # meat/seafood markets
    5451: "grocery",  # dairy stores
    5499: "grocery",  # misc food stores
    6010: "cash",  # manual cash disbursement
    6011: "cash",  # ATM cash disbursement
    6012: "cash",  # financial institutions, merchandise/services
    4829: "cash",  # wire transfer/money order
    5310: "online_retail",  # discount stores (mail/phone/online)
    5311: "online_retail",  # department stores
    5300: "online_retail",  # wholesale clubs
    5964: "online_retail",
    5965: "online_retail",
    5966: "online_retail",
    5967: "online_retail",
    5968: "online_retail",
    5969: "online_retail",
}


def _mcc_group(mcc: int) -> str:
    """Map a merchant category code to a coarse spend-category group."""
    if mcc in _MCC_EXACT_GROUPS:
        return _MCC_EXACT_GROUPS[mcc]
    if 3000 <= mcc <= 3999:
        # Airlines / car rental / lodging ranges.
        return "travel"
    return "other"


def _parse_event_time(event_time: str) -> datetime:
    return datetime.fromisoformat(event_time.replace("Z", "+00:00"))


def enrich(event: dict[str, Any]) -> dict[str, Any]:
    """Pure, stateless per-event derived features (no history required).

    Returns a dict with keys: is_cnp, is_chip, is_swipe, is_cross_border,
    mcc_group, mcc_group_id, mcc, has_error, amount_log, hour_of_day,
    day_of_week. Every one of these is available at authorization time (the
    current event's own fields), so it's safe to use both offline and online
    with no history lookup.
    """
    channel = event["channel"]
    merchant_country = event["merchant_country"]
    amount = float(event["amount"])
    mcc = int(event["mcc"])
    event_dt = _parse_event_time(event["event_time"])

    mcc_group = _mcc_group(mcc)

    return {
        "is_cnp": channel == "online",
        "is_chip": channel == "chip",
        "is_swipe": channel == "swipe",
        "is_cross_border": merchant_country not in ("US", "XX"),
        "mcc_group": mcc_group,
        "mcc_group_id": MCC_GROUP_IDS[mcc_group],
        "mcc": mcc,
        "has_error": bool(event.get("errors")),
        "amount_log": math.log1p(abs(amount)),
        "hour_of_day": event_dt.hour,
        "day_of_week": event_dt.weekday(),
    }


def window_feature_names(window_key: str) -> list[str]:
    """Column names for one window's aggregates, e.g. 'txn_count_1m'."""
    return [f"{metric}_{window_key}" for metric in _WINDOW_METRICS]


def _empty_window_features(window_key: str) -> dict[str, float]:
    """Zero-valued aggregates for a card with no prior history in this window."""
    names = window_feature_names(window_key)
    return dict.fromkeys(names, 0.0)


# The ordered list of columns the model trains/serves on. Event-level
# enrichments first, then point-in-time history features, then each window's
# aggregates in CARD_WINDOWS order.
FEATURE_COLUMNS: list[str] = [
    "amount",
    "amount_log",
    "is_cnp",
    "is_chip",
    "is_swipe",
    "is_cross_border",
    "mcc_group_id",
    "mcc",
    "has_error",
    "hour_of_day",
    "day_of_week",
    "time_since_last_txn_s",
    "is_new_city_30d",
    "amount_over_mean_30d",
    *[name for window_key in CARD_WINDOWS for name in window_feature_names(window_key)],
]


def compute_offline_card_features(events: list[dict[str, Any]]) -> list[dict[str, float]]:
    """Point-in-time-correct windowed aggregates for one card's event history.

    `events` must be every event for a single card_token, in ANY order; this
    function sorts by event_time and, for each event at position i, computes
    CARD_WINDOWS aggregates using only events strictly before it in time
    (leakage-safe: the event being featurized never contributes to its own
    features). Returns a list aligned index-for-index with the time-sorted
    input.

    Each result dict also carries ``last_event_ts`` (epoch seconds of the
    card's immediately-preceding event, or None for the card's first event)
    and ``is_new_city_30d`` (bool: merchant_city not seen in the trailing 30d
    window). ``last_event_ts`` is deliberately NOT the final
    ``time_since_last_txn_s`` feature — ``build_feature_row`` derives that,
    which is the one place (shared by the offline and online/Redis paths)
    that turns "timestamp of the last event" into "seconds since the last
    event", so a live-serving caller only needs to supply the same
    `last_event_ts` value (as stored in the Redis feature hash) with no
    special-case logic of its own.

    Implemented with a per-window sliding deque so each window is O(n) in the
    number of events for the card, not O(n^2).
    """
    order = sorted(range(len(events)), key=lambda i: events[i]["event_time"])
    sorted_events = [events[i] for i in order]
    n = len(sorted_events)
    time_secs = [_parse_event_time(e["event_time"]).timestamp() for e in sorted_events]

    results: list[dict[str, float]] = [dict() for _ in range(n)]

    for i in range(n):
        results[i]["last_event_ts"] = time_secs[i - 1] if i > 0 else None

    for window_key, window_seconds in CARD_WINDOWS.items():
        window_delta = window_seconds  # seconds
        buf: deque[int] = deque()  # indices of events currently in window, oldest first
        city_counts: Counter[str] = Counter()
        error_count = 0
        amount_sum = 0.0

        for right in range(n):
            # Advance the window to include all prior events within window_seconds
            # of the CURRENT event's time, then evict anything now stale, BEFORE
            # folding in features for `right` itself (features must exclude it).
            cutoff = time_secs[right] - window_delta
            while buf and time_secs[buf[0]] < cutoff:
                evicted = buf.popleft()
                amount_sum -= sorted_events[evicted]["amount"]
                city_counts[sorted_events[evicted]["merchant_city"]] -= 1
                if city_counts[sorted_events[evicted]["merchant_city"]] <= 0:
                    del city_counts[sorted_events[evicted]["merchant_city"]]
                if sorted_events[evicted].get("errors"):
                    error_count -= 1
            # (buf/amount_sum/city_counts/error_count now hold only events
            # strictly before `right`'s time — the invariant this function
            # relies on for leakage safety.)

            count = len(buf)
            results[right][f"txn_count_{window_key}"] = float(count)
            results[right][f"amount_sum_{window_key}"] = amount_sum
            results[right][f"amount_mean_{window_key}"] = amount_sum / count if count else 0.0
            results[right][f"distinct_merchant_city_{window_key}"] = float(len(city_counts))
            results[right][f"decline_rate_{window_key}"] = error_count / count if count else 0.0

            if window_key == "30d":
                current_city = sorted_events[right]["merchant_city"]
                results[right]["is_new_city_30d"] = float(city_counts.get(current_city, 0) == 0)

            # Now that features for `right` are recorded, add it to the window
            # so it becomes available to future events.
            buf.append(right)
            amount_sum += sorted_events[right]["amount"]
            city_counts[sorted_events[right]["merchant_city"]] += 1
            if sorted_events[right].get("errors"):
                error_count += 1

    # Re-order results back to the caller's original event ordering.
    output = [None] * n
    for sorted_pos, original_idx in enumerate(order):
        output[original_idx] = results[sorted_pos]
    return output  # type: ignore[return-value]


def build_feature_row(event: dict[str, Any], window_features: dict[str, Any]) -> dict[str, Any]:
    """Combine an event's enrichment + precomputed window features into one
    row keyed exactly by FEATURE_COLUMNS (plus card_token/label passthrough).

    `window_features` is intentionally loosely typed: offline it's the dict
    `compute_offline_card_features` produced for this event (native floats
    and a `last_event_ts`/`is_new_city_30d` pair); online it can be a Redis
    hash (string values) for the same card, keyed the same way — this
    function is the ONE place that derives `time_since_last_txn_s` from
    `last_event_ts`, so a serving caller (e.g. src/app.py) needs no special
    logic beyond fetching the hash and passing it straight through.
    """
    enriched = enrich(event)
    row: dict[str, Any] = {"card_token": event["card_token"], "amount": float(event["amount"])}
    row["amount_log"] = enriched["amount_log"]
    row["is_cnp"] = int(enriched["is_cnp"])
    row["is_chip"] = int(enriched["is_chip"])
    row["is_swipe"] = int(enriched["is_swipe"])
    row["is_cross_border"] = int(enriched["is_cross_border"])
    row["mcc_group_id"] = enriched["mcc_group_id"]
    row["mcc"] = enriched["mcc"]
    row["has_error"] = int(enriched["has_error"])
    row["hour_of_day"] = enriched["hour_of_day"]
    row["day_of_week"] = enriched["day_of_week"]

    for key, value in window_features.items():
        if key in ("last_event_ts", "is_new_city_30d"):
            continue
        row[key] = value

    is_new_city_raw = window_features.get("is_new_city_30d", 1)
    row["is_new_city_30d"] = int(bool(float(is_new_city_raw)))

    last_event_ts = window_features.get("last_event_ts")
    if last_event_ts is None or last_event_ts == "":
        row["time_since_last_txn_s"] = _TIME_SINCE_LAST_TXN_CAP_SECONDS
    else:
        current_ts = _parse_event_time(event["event_time"]).timestamp()
        delta = current_ts - float(last_event_ts)
        row["time_since_last_txn_s"] = max(0.0, min(delta, _TIME_SINCE_LAST_TXN_CAP_SECONDS))

    mean_30d = float(window_features.get("amount_mean_30d", 0.0) or 0.0)
    row["amount_over_mean_30d"] = row["amount"] / (mean_30d or 1.0)

    return row


def enrich_vectorized(
    channel: "pd.Series",
    merchant_country: "pd.Series",
    amount: "pd.Series",
    mcc: "pd.Series",
    event_time: "pd.Series",
    has_error: "pd.Series",
) -> "pd.DataFrame":
    """Vectorized equivalent of `enrich()` applied to whole columns at once.

    Same semantics as the per-event function (is_cnp, is_chip, is_swipe,
    is_cross_border, mcc_group_id, mcc, has_error, amount_log, hour_of_day,
    day_of_week) — used by the fast/vectorized training path so 24M rows
    don't pay per-row Python function-call overhead.
    """
    import numpy as np
    import pandas as pd

    is_cnp = (channel == "online").astype("int64")
    is_chip = (channel == "chip").astype("int64")
    is_swipe = (channel == "swipe").astype("int64")
    is_cross_border = (~merchant_country.isin(["US", "XX"])).astype("int64")

    exact_group = mcc.map(_MCC_EXACT_GROUPS)
    is_travel_range = mcc.between(3000, 3999)
    mcc_group = exact_group.where(exact_group.notna(), np.where(is_travel_range, "travel", "other"))
    mcc_group_id = mcc_group.map(MCC_GROUP_IDS).astype("int64")

    amount_log = np.log1p(amount.abs())

    event_dt = pd.to_datetime(event_time, utc=True)
    hour_of_day = event_dt.dt.hour.astype("int64")
    day_of_week = event_dt.dt.dayofweek.astype("int64")

    return pd.DataFrame(
        {
            "is_cnp": is_cnp,
            "is_chip": is_chip,
            "is_swipe": is_swipe,
            "is_cross_border": is_cross_border,
            "mcc_group_id": mcc_group_id,
            "mcc": mcc.astype("int64"),
            "has_error": has_error.astype("int64"),
            "amount_log": amount_log,
            "hour_of_day": hour_of_day,
            "day_of_week": day_of_week,
        },
        index=channel.index,
    )


def _distinct_count_in_expanding_ranges(
    city_codes: "np.ndarray", left: "np.ndarray"
) -> tuple[list[int], list[int]]:
    """For a single card's events (already time-sorted), return, for each
    position i: (a) the number of DISTINCT city codes in the half-open range
    [left[i], i) — i.e. strictly-before-i history, windowed by `left` — and
    (b) whether city_codes[i] itself is NEW relative to that same range (1 if
    city_codes[i] does not appear in [left[i], i), else 0). (b) is used for
    `is_new_city_30d` when this is called with the 30d window's `left` array.

    `left` (the window's start index per position) must be non-decreasing,
    which holds here because it is derived from a monotonic time cutoff
    (searchsorted over a sorted array). Under that guarantee, both window
    endpoints only move forward as i increases, so a two-pointer sweep visits
    each position O(1) amortized times total — no per-event dict/object
    allocation, just a small int->int frequency map over city codes.
    """
    n = len(city_codes)
    distinct_out = [0] * n
    is_new_out = [0] * n
    freq: dict[int, int] = {}
    distinct = 0
    lo = 0
    hi = 0  # elements [0, hi) have been added to the frequency map so far
    for i in range(n):
        target_hi = i
        while hi < target_hi:
            c = int(city_codes[hi])
            cnt = freq.get(c, 0) + 1
            freq[c] = cnt
            if cnt == 1:
                distinct += 1
            hi += 1
        target_lo = int(left[i])
        while lo < target_lo:
            c = int(city_codes[lo])
            cnt = freq[c] - 1
            if cnt == 0:
                del freq[c]
                distinct -= 1
            else:
                freq[c] = cnt
            lo += 1
        distinct_out[i] = distinct
        is_new_out[i] = 1 if freq.get(int(city_codes[i]), 0) == 0 else 0
    return distinct_out, is_new_out


def compute_card_features_vectorized(events_df: "pd.DataFrame") -> "pd.DataFrame":
    """Vectorized, leakage-safe equivalent of `compute_offline_card_features`
    for the FULL dataset at once (all cards), used by the fast training path.

    Required input columns: card_token, event_time (datetime64, any tz),
    amount (float), merchant_city (str), has_error (bool).

    Design: count/amount_sum/amount_mean/decline_rate for every CARD_WINDOWS
    window are pure prefix-sum-over-a-range computations once each card's
    events are time-ordered (range = [window_start_index(i), i), found per
    card via `numpy.searchsorted` on that card's sorted epoch-seconds array —
    no Python loop over rows). Only distinct-city count resists prefix sums
    (union-of-a-range, not a sum), so it keeps a small O(n) two-pointer loop
    per card via `_distinct_count_in_expanding_ranges` — but that loop touches
    plain ints, never per-event dicts/objects, which is what actually made the
    row-wise reference implementation too slow at 24M-row scale (datetime
    parsing + dict/Counter[str] construction per event). The same loop's
    "is new" byproduct (from the 30d window pass) becomes `is_new_city_30d`.
    `time_since_last_txn_s` is a plain vectorized `diff` per card group
    (capped at the 30d window) since it needs no windowing at all — it's
    always just "this event's time minus the previous event's time".

    The only genuine Python-level loop here iterates over CARD groups (at
    most a few thousand for TabFormer), not over rows.

    Returns a DataFrame with columns = window feature names +
    "time_since_last_txn_s" + "is_new_city_30d", aligned to `events_df`'s
    original row order (its index is preserved). Note this does NOT include
    `amount_over_mean_30d` — that's a trivial ratio against `amount_mean_30d`
    that the caller (`build_dataset_vectorized`) computes directly.
    """
    import numpy as np
    import pandas as pd

    window_cols = [name for w in CARD_WINDOWS for name in window_feature_names(w)]
    extra_cols = ["time_since_last_txn_s", "is_new_city_30d"]
    all_cols = window_cols + extra_cols

    n_total = len(events_df)
    if n_total == 0:
        return pd.DataFrame(columns=all_cols, index=events_df.index)

    work = events_df.reset_index(drop=True)
    seq = np.arange(n_total)
    card_codes, _ = pd.factorize(work["card_token"].to_numpy(), sort=False)
    epoch_seconds = work["event_time"].to_numpy("datetime64[ns]").astype("int64") // 1_000_000_000
    city_codes_all, _ = pd.factorize(work["merchant_city"].fillna("").to_numpy(), sort=False)
    amounts_all = work["amount"].to_numpy(dtype="float64")
    has_error_all = work["has_error"].to_numpy(dtype="float64")

    # Sort by (card, time, original row order) — stable tie-break matches the
    # row-wise reference, which sorts a per-card event list with Python's
    # stable sort keyed only on event_time (ties preserve input/CSV order).
    sort_order = np.lexsort((seq, epoch_seconds, card_codes))

    card_sorted = card_codes[sort_order]
    time_sorted = epoch_seconds[sort_order]
    amount_sorted = amounts_all[sort_order]
    error_sorted = has_error_all[sort_order]
    city_sorted = city_codes_all[sort_order]

    n = len(sort_order)
    out_cols: dict[str, "np.ndarray"] = {c: np.zeros(n, dtype="float64") for c in all_cols}

    # Group boundaries: card_sorted is contiguous-by-group after the sort above.
    group_start_positions = np.flatnonzero(np.r_[True, card_sorted[1:] != card_sorted[:-1]])
    group_end_positions = np.r_[group_start_positions[1:], n]

    amount_prefix_global = np.concatenate(([0.0], np.cumsum(amount_sorted)))
    error_prefix_global = np.concatenate(([0.0], np.cumsum(error_sorted)))

    cap_seconds = _TIME_SINCE_LAST_TXN_CAP_SECONDS

    for start, end in zip(group_start_positions, group_end_positions, strict=True):
        times_g = time_sorted[start:end]
        amount_prefix_g = amount_prefix_global[start : end + 1] - amount_prefix_global[start]
        error_prefix_g = error_prefix_global[start : end + 1] - error_prefix_global[start]
        idx = np.arange(len(times_g))
        len_g = end - start

        # time_since_last_txn_s: pure diff against the previous event in this
        # card's history, capped; first event in the card's history gets the cap.
        diffs = np.empty(len_g, dtype="float64")
        diffs[0] = cap_seconds
        if len_g > 1:
            diffs[1:] = np.minimum(times_g[1:] - times_g[:-1], cap_seconds)
        out_cols["time_since_last_txn_s"][start:end] = diffs

        for window_key, window_seconds in CARD_WINDOWS.items():
            cutoff = times_g - window_seconds
            left = np.searchsorted(times_g, cutoff, side="left")

            count = idx - left
            amount_sum = amount_prefix_g[idx] - amount_prefix_g[left]
            error_count = error_prefix_g[idx] - error_prefix_g[left]
            with np.errstate(invalid="ignore", divide="ignore"):
                amount_mean = np.where(count > 0, amount_sum / np.maximum(count, 1), 0.0)
                decline_rate = np.where(count > 0, error_count / np.maximum(count, 1), 0.0)

            distinct, is_new = _distinct_count_in_expanding_ranges(city_sorted[start:end], left)

            out_cols[f"txn_count_{window_key}"][start:end] = count
            out_cols[f"amount_sum_{window_key}"][start:end] = amount_sum
            out_cols[f"amount_mean_{window_key}"][start:end] = amount_mean
            out_cols[f"distinct_merchant_city_{window_key}"][start:end] = distinct
            out_cols[f"decline_rate_{window_key}"][start:end] = decline_rate

            if window_key == "30d":
                out_cols["is_new_city_30d"][start:end] = is_new

    # Undo the sort: out_cols are in `sort_order` order; scatter back to the
    # original row order of `events_df`.
    result = pd.DataFrame(out_cols)
    result["_orig_pos"] = sort_order
    result = result.sort_values("_orig_pos", kind="mergesort").drop(columns="_orig_pos")
    result.index = events_df.index
    return result[all_cols]


# --- Spark Structured Streaming job (lazy pyspark import; not needed for tests) --

_AVRO_SCHEMA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "contracts",
    "transaction.avsc",
)


def _avro_schema_json_str() -> str:
    """Read the checked-in, CI-synced contracts/transaction.avsc (ADR 0006
    decision 2) — the reader schema `from_avro` decodes against, replacing
    the old hand-mirrored `_event_schema()` StructType as the single
    source of truth for the wire shape."""
    with open(_AVRO_SCHEMA_PATH, encoding="utf-8") as f:
        return f.read()


def _build_spark_session():
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("fraud-pipeline-features")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.spark:spark-avro_2.12:3.5.1",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return builder.getOrCreate()


def latest_card_feature_mappings(history_pdf: "pd.DataFrame") -> "pd.DataFrame":
    """Given the (bounded) event history of a set of cards, return one row per
    card holding the CURRENT online feature hash for that card: every
    CARD_WINDOWS aggregate evaluated over the trailing window ending at (and
    including) the card's most recent event, plus ``last_event_ts`` (epoch
    seconds of that most recent event).

    Required columns in `history_pdf`: card_token, event_time (datetime64,
    tz-naive UTC), amount (float), merchant_city (str), has_error (bool).

    Train/serve-skew rule: the aggregate math is NOT reimplemented here — a
    synthetic probe row is appended 1µs after each card's last event and the
    whole frame is run through the SAME ``compute_card_features_vectorized``
    the offline trainer uses; the probe rows' strictly-before-the-probe
    features are exactly "every real event up to and including the latest",
    i.e. the state the next incoming authorization should be scored against.
    The probe's own amount/city/error values never contribute (a row never
    contributes to its own features), so dummies are safe.
    """
    import pandas as pd

    last_ts = history_pdf.groupby("card_token", sort=False)["event_time"].max().reset_index()
    probes = last_ts.copy()
    probes["event_time"] = probes["event_time"] + pd.Timedelta(microseconds=1)
    probes["amount"] = 0.0
    probes["merchant_city"] = ""
    probes["has_error"] = False

    combined = pd.concat([history_pdf, probes], ignore_index=True)
    feats = compute_card_features_vectorized(combined)
    probe_feats = feats.iloc[len(history_pdf) :].reset_index(drop=True)

    window_cols = [c for w in CARD_WINDOWS for c in window_feature_names(w)]
    out = last_ts[["card_token"]].copy()
    for col in window_cols:
        out[col] = probe_feats[col].to_numpy()
    out["last_event_ts"] = last_ts["event_time"].astype("int64") / 1e9
    return out


def run_stream(once: bool = False) -> None:  # pragma: no cover - requires a live Kafka/Spark cluster
    """Consume KAFKA_TOPIC_TRANSACTIONS, compute windowed card features, sink
    to Redis (online) + Delta (offline). `once=True` uses trigger(availableNow)
    for bounded runs (tests/smoke) instead of running forever.
    """
    from pyspark.sql.avro.functions import from_avro
    from pyspark.sql import functions as F

    spark = _build_spark_session()
    schema_json = _avro_schema_json_str()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_TRANSACTIONS)
        .option("startingOffsets", "earliest")
        .load()
    )

    # ADR 0006 decision 2: OSS Spark's from_avro has no registry integration
    # and doesn't understand Confluent wire framing — strip the 5-byte
    # magic-byte + schema-id header manually, then decode against the
    # checked-in reader schema. PERMISSIVE mode nulls out corrupt frames
    # instead of failing the query; those rows are filtered below.
    # NOTE: F.substring()'s pos/len args must be plain ints in PySpark 3.5 —
    # only Column.substr() accepts Column expressions for a data-dependent
    # length like `total length - 5`.
    avro_payload = F.col("value").substr(F.lit(6), F.length(F.col("value")) - 5)
    decoded = raw.select(from_avro(avro_payload, schema_json, {"mode": "PERMISSIVE"}).alias("e"))
    parsed = decoded.filter(F.col("e").isNotNull()).select("e.*")

    required_cols = [
        "event_id",
        "event_time",
        "card_token",
        "user_id",
        "amount",
        "currency",
        "channel",
        "merchant_name",
        "merchant_city",
        "merchant_state",
        "merchant_country",
        "mcc",
    ]
    is_valid = None
    for c in required_cols:
        cond = F.col(c).isNotNull()
        is_valid = cond if is_valid is None else (is_valid & cond)

    quarantine_path = os.path.join(DELTA_ROOT, "_quarantine")
    valid_df = parsed.filter(is_valid)
    invalid_df = parsed.filter(~is_valid)

    def sink_batch(batch_df, batch_id: int) -> None:
        import redis

        invalid, valid = batch_df.filter(~is_valid), batch_df.filter(is_valid)
        if invalid.take(1):
            invalid.write.format("delta").mode("append").save(quarantine_path)
        if valid.take(1):
            enriched = (
                valid.withColumn("event_ts", F.to_timestamp("event_time"))
                .withColumn("is_cnp", F.col("channel") == F.lit("online"))
                .withColumn("is_chip", F.col("channel") == F.lit("chip"))
                .withColumn("is_swipe", F.col("channel") == F.lit("swipe"))
                .withColumn(
                    "is_cross_border",
                    ~F.col("merchant_country").isin("US", "XX"),
                )
                .withColumn("amount_log", F.log1p(F.abs(F.col("amount"))))
                .withColumn("has_error", F.col("errors").isNotNull())
                .withColumn("hour_of_day", F.hour("event_ts"))
                .withColumn("day_of_week", F.dayofweek("event_ts"))
                .withColumn("updated_at", F.current_timestamp().cast("string"))
            )

            events_path = os.path.join(DELTA_ROOT, "events")
            enriched.write.format("delta").mode("append").save(events_path)

            # Online per-card features: NOT a Spark sliding-window aggregation.
            # v2 windows go up to 30 days — with F.window's slide semantics
            # that's tens of thousands of window instances per event, which
            # does not scale. Instead each microbatch replays the affected
            # cards' trailing-30d Delta history through the SAME vectorized
            # feature code the offline trainer uses (skew rule) and upserts
            # each card's latest hash into Redis. A microbatch is bounded and
            # TabFormer has O(thousands) of cards, so the driver-side pandas
            # pass is bounded too. `amount_over_mean_30d`/`time_since_last_txn_s`
            # are not stored: build_feature_row derives them at serving time
            # from amount_mean_30d / last_event_ts.
            batch_cards = [row["card_token"] for row in enriched.select("card_token").distinct().collect()]
            # Bound the history read to what the 30d window can ever use. The
            # cutoff is relative to the BATCH's event times (replayed data is
            # historic), not the wall clock: any event older than (a card's
            # last event - 30d) can't contribute, and every card's last event
            # is >= this batch's minimum event time.
            min_batch_ts = enriched.agg(F.min("event_ts")).collect()[0][0]
            history_cutoff = min_batch_ts - timedelta(days=31)
            history = (
                spark.read.format("delta")
                .load(events_path)
                .filter(F.col("card_token").isin(batch_cards) & (F.col("event_ts") >= F.lit(history_cutoff)))
                .select(
                    "card_token",
                    F.col("event_ts").alias("event_time"),
                    "amount",
                    "merchant_city",
                    F.col("errors").isNotNull().alias("has_error"),
                )
                .toPandas()
            )
            if getattr(history["event_time"].dtype, "tz", None) is not None:
                history["event_time"] = history["event_time"].dt.tz_localize(None)

            mappings = latest_card_feature_mappings(history)
            updated_at = datetime.now(timezone.utc).isoformat()

            redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            pipe = redis_client.pipeline()
            for row in mappings.to_dict(orient="records"):
                card_token = row.pop("card_token")
                mapping = {key: float(value) for key, value in row.items()}
                mapping["updated_at"] = updated_at
                pipe.hset(f"features:{card_token}", mapping=mapping)
            pipe.execute()

            features_pdf = mappings.copy()
            features_pdf["updated_at"] = updated_at
            spark.createDataFrame(features_pdf).write.format("delta").mode("append").save(
                os.path.join(DELTA_ROOT, "card_features")
            )

    query_builder = valid_df.union(invalid_df).writeStream.foreachBatch(sink_batch).outputMode("append")
    if once:
        query = query_builder.trigger(availableNow=True).start()
    else:
        query = query_builder.trigger(processingTime="10 seconds").start()
    query.awaitTermination()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Windowed card-feature streaming job.")
    parser.add_argument("--run-stream", action="store_true", help="Run the continuous streaming job.")
    parser.add_argument(
        "--once", action="store_true", help="Bounded run: trigger(availableNow) then exit."
    )
    args = parser.parse_args(argv)

    if args.run_stream or args.once:
        run_stream(once=args.once)
    else:
        parser.print_help()


if __name__ == "__main__":
    main(sys.argv[1:])
