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
   single contract-v1 event: card-not-present flag, cross-border flag, MCC
   grouping, log-amount, hour-of-day, day-of-week. No history required.
2. **Windowed per-card aggregates** (``CARD_WINDOWS``) — count, amount
   sum/mean, distinct merchant-city count, and decline-ish rate over the
   trailing 1 minute / 10 minutes / 1 hour of a card's activity, computed
   **strictly before** the event being scored (point-in-time correctness /
   no label leakage).

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

Valid events are watermarked 10 minutes on ``event_time``, aggregated over
sliding windows per ``CARD_WINDOWS``, and in `foreachBatch`:
  * upserted into the Redis hash ``features:{card_token}`` (online store), and
  * appended to Delta tables ``{DELTA_ROOT}/events`` (raw enriched events) and
    ``{DELTA_ROOT}/card_features`` (windowed aggregates), for offline reuse.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from collections import Counter, deque
from datetime import datetime
from typing import Any

from dotenv import load_dotenv

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

# Trailing windows evaluated per card_token, in seconds.
CARD_WINDOWS: dict[str, int] = {"1m": 60, "10m": 600, "1h": 3600}

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

    Returns a dict with keys: is_cnp, is_cross_border, mcc_group, mcc_group_id,
    amount_log, hour_of_day, day_of_week.
    """
    channel = event["channel"]
    merchant_country = event["merchant_country"]
    amount = float(event["amount"])
    mcc = int(event["mcc"])
    event_dt = _parse_event_time(event["event_time"])

    mcc_group = _mcc_group(mcc)

    return {
        "is_cnp": channel == "online",
        "is_cross_border": merchant_country not in ("US", "XX"),
        "mcc_group": mcc_group,
        "mcc_group_id": MCC_GROUP_IDS[mcc_group],
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
# enrichments first, then each window's aggregates in CARD_WINDOWS order.
FEATURE_COLUMNS: list[str] = [
    "amount",
    "amount_log",
    "is_cnp",
    "is_cross_border",
    "mcc_group_id",
    "hour_of_day",
    "day_of_week",
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

    Implemented with a per-window sliding deque so each window is O(n) in the
    number of events for the card, not O(n^2).
    """
    order = sorted(range(len(events)), key=lambda i: events[i]["event_time"])
    sorted_events = [events[i] for i in order]
    n = len(sorted_events)
    times = [_parse_event_time(e["event_time"]) for e in sorted_events]

    results: list[dict[str, float]] = [dict() for _ in range(n)]

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
            cutoff = times[right].timestamp() - window_delta
            while buf and times[buf[0]].timestamp() < cutoff:
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


def build_feature_row(event: dict[str, Any], window_features: dict[str, float]) -> dict[str, Any]:
    """Combine an event's enrichment + precomputed window features into one
    row keyed exactly by FEATURE_COLUMNS (plus card_token/label passthrough)."""
    enriched = enrich(event)
    row: dict[str, Any] = {"card_token": event["card_token"], "amount": event["amount"]}
    row["amount_log"] = enriched["amount_log"]
    row["is_cnp"] = int(enriched["is_cnp"])
    row["is_cross_border"] = int(enriched["is_cross_border"])
    row["mcc_group_id"] = enriched["mcc_group_id"]
    row["hour_of_day"] = enriched["hour_of_day"]
    row["day_of_week"] = enriched["day_of_week"]
    row.update(window_features)
    return row


# --- Spark Structured Streaming job (lazy pyspark import; not needed for tests) --


def _event_schema():
    """Explicit Spark schema mirroring contracts/transaction.schema.json."""
    from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

    return StructType(
        [
            StructField("schema_version", StringType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("card_token", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("merchant_name", StringType(), True),
            StructField("merchant_city", StringType(), True),
            StructField("merchant_state", StringType(), True),
            StructField("merchant_country", StringType(), True),
            StructField("zip", StringType(), True),
            StructField("mcc", IntegerType(), True),
            StructField("errors", StringType(), True),
            StructField("is_fraud", BooleanType(), True),
        ]
    )


def _build_spark_session():
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("fraud-pipeline-features")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return builder.getOrCreate()


def _foreach_batch(batch_df, batch_id: int) -> None:  # pragma: no cover - exercised only with a live cluster
    """Per-microbatch sink: upsert Redis online features + append Delta card_features."""
    import redis

    features_path = os.path.join(DELTA_ROOT, "card_features")

    window_cols = [c for w in CARD_WINDOWS for c in window_feature_names(w)]
    feature_df = batch_df.select("card_token", "updated_at", *window_cols)
    feature_df.write.format("delta").mode("append").save(features_path)

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    rows = feature_df.collect()
    pipe = r.pipeline()
    for row in rows:
        mapping = {c: row[c] for c in window_cols}
        mapping["updated_at"] = row["updated_at"]
        pipe.hset(f"features:{row['card_token']}", mapping=mapping)
    pipe.execute()


def run_stream(once: bool = False) -> None:  # pragma: no cover - requires a live Kafka/Spark cluster
    """Consume KAFKA_TOPIC_TRANSACTIONS, compute windowed card features, sink
    to Redis (online) + Delta (offline). `once=True` uses trigger(availableNow)
    for bounded runs (tests/smoke) instead of running forever.
    """
    from pyspark.sql import functions as F

    spark = _build_spark_session()
    schema = _event_schema()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_TRANSACTIONS)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.*")

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
        invalid, valid = batch_df.filter(~is_valid), batch_df.filter(is_valid)
        if invalid.take(1):
            invalid.write.format("delta").mode("append").save(quarantine_path)
        if valid.take(1):
            enriched = (
                valid.withColumn("event_ts", F.to_timestamp("event_time"))
                .withColumn("is_cnp", F.col("channel") == F.lit("online"))
                .withColumn(
                    "is_cross_border",
                    ~F.col("merchant_country").isin("US", "XX"),
                )
                .withColumn("amount_log", F.log1p(F.abs(F.col("amount"))))
                .withColumn("hour_of_day", F.hour("event_ts"))
                .withColumn("day_of_week", F.dayofweek("event_ts"))
                .withColumn("updated_at", F.current_timestamp().cast("string"))
            )

            windowed = enriched.withWatermark("event_ts", "10 minutes")
            for window_key, seconds in CARD_WINDOWS.items():
                agg = (
                    windowed.groupBy(
                        F.window("event_ts", f"{seconds} seconds", "30 seconds"),
                        "card_token",
                    )
                    .agg(
                        F.count("*").alias(f"txn_count_{window_key}"),
                        F.sum("amount").alias(f"amount_sum_{window_key}"),
                        F.avg("amount").alias(f"amount_mean_{window_key}"),
                        F.countDistinct("merchant_city").alias(f"distinct_merchant_city_{window_key}"),
                        (F.sum(F.when(F.col("errors").isNotNull(), 1).otherwise(0)) / F.count("*")).alias(
                            f"decline_rate_{window_key}"
                        ),
                    )
                    .withColumn("updated_at", F.current_timestamp().cast("string"))
                )
                _foreach_batch(agg, batch_id)

            enriched.write.format("delta").mode("append").save(os.path.join(DELTA_ROOT, "events"))

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
