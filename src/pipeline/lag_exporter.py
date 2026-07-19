"""Consumer-lag + pipeline-freshness Prometheus exporter (ticket 13, v1.3).

Data flow
---------
A tiny HTTP server (``prometheus_client.start_http_server``) exposing, in
Prometheus text format on ``LAG_EXPORTER_PORT``:

- ``kafka_consumergroup_lag{group,topic,partition}`` — committed-offset vs
  high-watermark distance for each consumer group the pipeline runs
  (``scorer`` on ``transactions``, ``cdc-transformer`` on the CDC topic).
  This is *the* operational DE signal: "how far behind is scoring right now,
  and is it recovering?"
- ``bank_rows_total{table}`` — row counts of ``bank.scored_transactions`` /
  ``bank.fraud_alerts`` / ``bank.card_transactions`` (the end-to-end
  progress counters the v1.2 kill tests were verified against).
- ``scoring_staleness_seconds`` — seconds since the newest
  ``bank.scored_transactions.scored_at``; NaN until the first score lands.
- ``lag_exporter_target_up{target}`` — 1/0 per polled backend (kafka,
  bankdb), so Grafana can show *why* a panel went blank.

Why hand-rolled rather than a stock kafka-exporter image: the point of this
project is showing the mechanics (committed offset vs high watermark is two
API calls), and no stock exporter also knows about the bank DB freshness
side. See ADR 0004.

Testability
-----------
``compute_lag`` / ``parse_group_topics`` are pure. ``collect_kafka_lag`` and
``collect_bank_metrics`` take their client objects as parameters (a
confluent_kafka ``Consumer``-shaped object per group, a SQLAlchemy engine)
so tests drive them with mocks — no broker, no DB. Only ``main()`` builds
real clients and owns the poll loop.

Reading another group's committed offsets uses a non-subscribing Consumer
constructed with that ``group.id``: ``committed()`` is a read-only offset
fetch and never joins the group, so the exporter cannot trigger a rebalance
in the group it is observing.
"""

from __future__ import annotations

import logging
import math
import os
import sys
import time
from typing import Any

from dotenv import load_dotenv
from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server
from sqlalchemy import Engine, text

load_dotenv()

logger = logging.getLogger(__name__)

# --- Env-driven configuration (defaults mirror .env.example) ---------------

LAG_EXPORTER_PORT = int(os.environ.get("LAG_EXPORTER_PORT", "9105"))
LAG_POLL_INTERVAL_S = float(os.environ.get("LAG_POLL_INTERVAL_S", "5"))
# "group:topic,group:topic" — every consumer group the pipeline runs, and the
# topic it consumes. Both demo modes' groups are listed by default; a group
# that has never committed (e.g. cdc-transformer in replay mode) reports lag
# from the topic's low watermark, which is 0 on an empty topic.
LAG_GROUPS = os.environ.get(
    "LAG_GROUPS",
    "scorer:transactions,cdc-transformer:bankdb.frauddemo.bank.card_transactions",
)

KAFKA_TIMEOUT_S = 10.0

BANK_TABLES = ("scored_transactions", "fraud_alerts", "card_transactions")

_STALENESS_SQL = text(
    "SELECT DATEDIFF(second, MAX(scored_at), SYSUTCDATETIME()) FROM bank.scored_transactions"
)


def parse_group_topics(spec: str) -> list[tuple[str, str]]:
    """Parse "group:topic,group:topic" into [(group, topic), ...]."""
    pairs: list[tuple[str, str]] = []
    for chunk in spec.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        group, _, topic = chunk.partition(":")
        if not group or not topic:
            raise ValueError(f"invalid LAG_GROUPS entry {chunk!r} (want group:topic)")
        pairs.append((group, topic))
    return pairs


def compute_lag(committed: int | None, low: int, high: int) -> int:
    """Lag for one partition. A group with no committed offset yet (None, or
    confluent_kafka's OFFSET_INVALID = -1001) has consumed nothing, so its
    effective position is the low watermark — everything retained is pending."""
    effective = committed if committed is not None and committed >= 0 else low
    return max(high - effective, 0)


def collect_kafka_lag(
    group_consumers: dict[str, Any], group_topics: list[tuple[str, str]]
) -> list[dict[str, Any]]:
    """Return one {group, topic, partition, lag} sample per partition.

    ``group_consumers`` maps group id -> a Consumer constructed with that
    ``group.id`` (never subscribed). Import of TopicPartition is deferred so
    tests never need librdkafka loaded when they pass pre-built samples."""
    from confluent_kafka import TopicPartition

    samples: list[dict[str, Any]] = []
    for group, topic in group_topics:
        consumer = group_consumers[group]
        meta = consumer.list_topics(topic, timeout=KAFKA_TIMEOUT_S)
        topic_meta = meta.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None:
            # Topic not created yet (e.g. CDC topic in replay mode before any
            # demo-cdc run): lag is 0 pending, not an exporter failure.
            logger.debug("topic %s not available yet; skipping", topic)
            continue
        partitions = [TopicPartition(topic, p) for p in topic_meta.partitions]
        committed = consumer.committed(partitions, timeout=KAFKA_TIMEOUT_S)
        for tp in committed:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, tp.partition), timeout=KAFKA_TIMEOUT_S
            )
            samples.append(
                {
                    "group": group,
                    "topic": topic,
                    "partition": tp.partition,
                    "lag": compute_lag(tp.offset, low, high),
                }
            )
    return samples


def collect_bank_metrics(engine: Engine) -> dict[str, Any]:
    """Row counts per bank table + scoring staleness (seconds since newest
    scored_at; NaN while bank.scored_transactions is empty)."""
    rows: dict[str, int] = {}
    with engine.connect() as conn:
        for table in BANK_TABLES:
            rows[table] = conn.execute(
                text(f"SELECT COUNT(*) FROM bank.{table}")  # noqa: S608 — fixed table allowlist
            ).scalar_one()
        staleness_raw = conn.execute(_STALENESS_SQL).scalar_one()
    staleness = float(staleness_raw) if staleness_raw is not None else math.nan
    return {"rows": rows, "staleness_seconds": staleness}


def build_metrics(registry: CollectorRegistry) -> dict[str, Any]:
    """Create the exporter's metric objects on `registry`."""
    return {
        "lag": Gauge(
            "kafka_consumergroup_lag",
            "Messages between the group's committed offset and the partition high watermark.",
            ["group", "topic", "partition"],
            registry=registry,
        ),
        "rows": Gauge(
            "bank_rows_total",
            "Row count of the bank-DB table (end-to-end pipeline progress counter).",
            ["table"],
            registry=registry,
        ),
        "staleness": Gauge(
            "scoring_staleness_seconds",
            "Seconds since the newest bank.scored_transactions.scored_at (NaN before first score).",
            registry=registry,
        ),
        "target_up": Gauge(
            "lag_exporter_target_up",
            "1 if the exporter's last poll of this backend succeeded, else 0.",
            ["target"],
            registry=registry,
        ),
        "poll_errors": Counter(
            "lag_exporter_poll_errors_total",
            "Backend poll failures, by target.",
            ["target"],
            registry=registry,
        ),
    }


def update_once(
    metrics: dict[str, Any],
    group_consumers: dict[str, Any],
    group_topics: list[tuple[str, str]],
    engine: Engine,
) -> None:
    """One poll cycle: refresh every gauge, failing per-backend (a Kafka
    outage must not blank the bank-freshness metrics, and vice versa)."""
    try:
        for sample in collect_kafka_lag(group_consumers, group_topics):
            metrics["lag"].labels(
                group=sample["group"],
                topic=sample["topic"],
                partition=str(sample["partition"]),
            ).set(sample["lag"])
        metrics["target_up"].labels(target="kafka").set(1)
    except Exception as exc:  # noqa: BLE001 — exporter must outlive backend outages
        logger.warning("kafka lag poll failed: %s", exc)
        metrics["target_up"].labels(target="kafka").set(0)
        metrics["poll_errors"].labels(target="kafka").inc()

    try:
        bank = collect_bank_metrics(engine)
        for table, count in bank["rows"].items():
            metrics["rows"].labels(table=table).set(count)
        metrics["staleness"].set(bank["staleness_seconds"])
        metrics["target_up"].labels(target="bankdb").set(1)
    except Exception as exc:  # noqa: BLE001
        logger.warning("bank metrics poll failed: %s", exc)
        metrics["target_up"].labels(target="bankdb").set(0)
        metrics["poll_errors"].labels(target="bankdb").inc()


def main() -> None:
    from confluent_kafka import Consumer

    from src.bank.db import get_engine
    from src.pipeline.ingestion import kafka_config

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    group_topics = parse_group_topics(LAG_GROUPS)
    group_consumers = {}
    for group, _topic in group_topics:
        config = kafka_config()
        config["group.id"] = group
        # Read-only observer: never subscribes, so it never joins the group.
        config["enable.auto.commit"] = False
        group_consumers[group] = Consumer(config)

    engine = get_engine()
    registry = CollectorRegistry()
    metrics = build_metrics(registry)

    start_http_server(LAG_EXPORTER_PORT, registry=registry)
    logger.info(
        "lag exporter serving :%d, polling every %.1fs for %s",
        LAG_EXPORTER_PORT,
        LAG_POLL_INTERVAL_S,
        group_topics,
    )
    while True:
        update_once(metrics, group_consumers, group_topics, engine)
        time.sleep(LAG_POLL_INTERVAL_S)


if __name__ == "__main__":
    sys.exit(main())
