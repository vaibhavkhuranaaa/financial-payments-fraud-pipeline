"""Live scorer loop — Kafka transactions -> POST /score -> bank.scored_transactions.

Data flow
---------
Consumes contract-v1 events from ``KAFKA_TOPIC_TRANSACTIONS`` (consumer group
``scorer``), POSTs each one to the Flask ``/score`` endpoint (see
``src/app.py`` for the response shape: ``fraud_probability``, ``decision``,
``threshold``, ``cold_card``, ``latency_ms``), and writes an audit row to
``bank.scored_transactions``. When ``fraud_probability`` clears the alert
threshold (``ALERT_THRESHOLD`` env override, else the model's own threshold
from the response) an additional row is written to ``bank.fraud_alerts``.

This closes the demo loop: replay a CSV onto Kafka and the dashboard (ticket
08) goes live with zero manual steps in between.

Testability
-----------
``handle_event(event, session, engine, buffer)`` is broker-free: it takes a
plain dict (what a Kafka message's JSON value decodes to), a ``requests``
session, a SQLAlchemy engine, and a ``ScoreBuffer`` accumulator, and performs
no Kafka calls itself. Tests drive it directly with mocked ``session``/
``engine`` and call ``flush`` explicitly. The confluent_kafka ``Consumer``
only appears in ``run()``/``main()``, which owns the delivery-semantics
boundary: auto-commit is off and offsets are committed synchronously only
AFTER the buffered rows are flushed to the bank DB (commit-after-flush;
redelivery is absorbed by the event_id PK dedupe).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import requests
from dotenv import load_dotenv
from sqlalchemy import Engine, text
from sqlalchemy.exc import IntegrityError

from src.bank.db import get_engine
from src.pipeline.ingestion import KAFKA_TOPIC_TRANSACTIONS, kafka_config

load_dotenv()

logger = logging.getLogger(__name__)

# --- Env-driven configuration (defaults mirror .env.example) ---------------

SCORE_URL = os.environ.get("SCORE_URL", "http://api:8000/score")
_ALERT_THRESHOLD_RAW = os.environ.get("ALERT_THRESHOLD", "").strip()
ALERT_THRESHOLD: float | None = float(_ALERT_THRESHOLD_RAW) if _ALERT_THRESHOLD_RAW else None
SCORER_MAX_EVENTS_RAW = os.environ.get("SCORER_MAX_EVENTS", "").strip()
SCORER_MAX_EVENTS: int | None = int(SCORER_MAX_EVENTS_RAW) if SCORER_MAX_EVENTS_RAW else None

CONSUMER_GROUP_ID = "scorer"

SCORE_TIMEOUT_S = 2.0
SCORE_MAX_ATTEMPTS = 3
SCORE_RETRY_BACKOFF_S = 0.2

BATCH_SIZE = 50
FLUSH_INTERVAL_S = 2.0

_INSERT_SCORED_TXN_SQL = text(
    """
    INSERT INTO bank.scored_transactions
        (event_id, card_token, event_time, amount, merchant_name, merchant_city,
         merchant_state, mcc, channel, fraud_probability, decision, cold_card, latency_ms)
    VALUES
        (:event_id, :card_token, :event_time, :amount, :merchant_name, :merchant_city,
         :merchant_state, :mcc, :channel, :fraud_probability, :decision, :cold_card, :latency_ms)
    """
)

_INSERT_FRAUD_ALERT_SQL = text(
    """
    INSERT INTO bank.fraud_alerts
        (event_id, card_token, fraud_probability, amount, merchant_name)
    VALUES
        (:event_id, :card_token, :fraud_probability, :amount, :merchant_name)
    """
)


def _parse_event_time(event_time: str) -> datetime:
    """Parse contract-v1's UTC ISO-8601 ``event_time`` (suffix ``Z``) to a naive
    UTC datetime, which is what DATETIME2 over pymssql expects."""
    return datetime.fromisoformat(event_time.replace("Z", "+00:00")).replace(tzinfo=None)


def score_event(session: requests.Session, event: dict[str, Any]) -> dict[str, Any] | None:
    """POST `event` to SCORE_URL. Returns the parsed response, or None if the
    event should be skipped (4xx, or repeated 5xx/connection failure after
    retries with backoff)."""
    url = SCORE_URL
    last_error: str | None = None
    for attempt in range(1, SCORE_MAX_ATTEMPTS + 1):
        try:
            response = session.post(url, json=event, timeout=SCORE_TIMEOUT_S)
        except requests.exceptions.RequestException as exc:
            last_error = f"connection error: {exc}"
            if attempt < SCORE_MAX_ATTEMPTS:
                time.sleep(SCORE_RETRY_BACKOFF_S * attempt)
                continue
            logger.warning(
                "score request failed after %d attempts for event_id=%s: %s",
                attempt,
                event.get("event_id"),
                last_error,
            )
            return None

        if response.status_code == 200:
            return response.json()

        if 500 <= response.status_code < 600:
            last_error = f"HTTP {response.status_code}: {response.text[:200]}"
            if attempt < SCORE_MAX_ATTEMPTS:
                time.sleep(SCORE_RETRY_BACKOFF_S * attempt)
                continue
            logger.warning(
                "score request 5xx after %d attempts for event_id=%s: %s",
                attempt,
                event.get("event_id"),
                last_error,
            )
            return None

        # 4xx (or any other non-retryable status): skip immediately, no retry.
        logger.warning(
            "score request rejected (HTTP %d) for event_id=%s: %s",
            response.status_code,
            event.get("event_id"),
            response.text[:200],
        )
        return None

    return None


def build_scored_row(event: dict[str, Any], score_response: dict[str, Any]) -> dict[str, Any]:
    """Map a contract-v1 event + /score response onto a bank.scored_transactions row."""
    return {
        "event_id": event["event_id"],
        "card_token": event["card_token"],
        "event_time": _parse_event_time(event["event_time"]),
        "amount": event["amount"],
        "merchant_name": event.get("merchant_name"),
        "merchant_city": event.get("merchant_city"),
        "merchant_state": event.get("merchant_state"),
        "mcc": event.get("mcc"),
        "channel": event.get("channel"),
        "fraud_probability": score_response["fraud_probability"],
        "decision": score_response["decision"],
        "cold_card": bool(score_response.get("cold_card", False)),
        "latency_ms": score_response.get("latency_ms"),
    }


def build_alert_row(event: dict[str, Any], score_response: dict[str, Any]) -> dict[str, Any] | None:
    """Return a bank.fraud_alerts row if `fraud_probability` clears the alert
    threshold (ALERT_THRESHOLD env override, else the response's own model
    threshold), else None."""
    threshold = ALERT_THRESHOLD if ALERT_THRESHOLD is not None else score_response.get("threshold", 1.0)
    fraud_probability = score_response["fraud_probability"]
    if fraud_probability < threshold:
        return None
    return {
        "event_id": event["event_id"],
        "card_token": event["card_token"],
        "fraud_probability": fraud_probability,
        "amount": event["amount"],
        "merchant_name": event.get("merchant_name"),
    }


@dataclass
class ScoreBuffer:
    """Accumulates scored_transactions/fraud_alerts rows and flushes in
    batches (every `batch_size` rows or `flush_interval_s` seconds,
    whichever comes first) instead of one round-trip per event."""

    batch_size: int = BATCH_SIZE
    flush_interval_s: float = FLUSH_INTERVAL_S
    scored_rows: list[dict[str, Any]] = field(default_factory=list)
    alert_rows: list[dict[str, Any]] = field(default_factory=list)
    _last_flush: float = field(default_factory=time.monotonic)

    def add(self, scored_row: dict[str, Any], alert_row: dict[str, Any] | None) -> None:
        self.scored_rows.append(scored_row)
        if alert_row is not None:
            self.alert_rows.append(alert_row)

    def should_flush(self) -> bool:
        if len(self.scored_rows) >= self.batch_size:
            return True
        return bool(self.scored_rows) and (time.monotonic() - self._last_flush) >= self.flush_interval_s

    def mark_flushed(self) -> None:
        self.scored_rows = []
        self.alert_rows = []
        self._last_flush = time.monotonic()


def _insert_rows_swallow_duplicates(engine: Engine, stmt: Any, rows: list[dict[str, Any]]) -> None:
    """Batch-insert `rows`; on a duplicate-key IntegrityError (replay /
    rebalance re-delivering an event_id already scored), fall back to
    inserting one row at a time so only the actual duplicates are swallowed."""
    if not rows:
        return
    try:
        with engine.begin() as conn:
            conn.execute(stmt, rows)
        return
    except IntegrityError:
        logger.info("duplicate key in batch insert; retrying row-by-row to isolate duplicates")

    for row in rows:
        try:
            with engine.begin() as conn:
                conn.execute(stmt, row)
        except IntegrityError:
            logger.info("swallowed duplicate-key insert for event_id=%s", row.get("event_id"))


def flush(engine: Engine, buffer: ScoreBuffer) -> None:
    """Write buffered rows to bank.scored_transactions / bank.fraud_alerts and
    reset the buffer."""
    _insert_rows_swallow_duplicates(engine, _INSERT_SCORED_TXN_SQL, buffer.scored_rows)
    _insert_rows_swallow_duplicates(engine, _INSERT_FRAUD_ALERT_SQL, buffer.alert_rows)
    buffer.mark_flushed()


def handle_event(
    event: dict[str, Any],
    session: requests.Session,
    engine: Engine,
    buffer: ScoreBuffer,
) -> bool:
    """Score one event and buffer the resulting row(s). Returns True if the
    event was scored, False if it was skipped (score request failed).

    Flushing is owned by the caller (`run`), which pairs every SQL flush with
    a Kafka offset commit — the delivery-semantics boundary lives in exactly
    one place. `engine` stays a parameter so tests can drive an explicit
    `flush(engine, buffer)` after this returns."""
    del engine  # flush ownership moved to run(); kept for test-facing signature
    score_response = score_event(session, event)
    if score_response is None:
        return False

    scored_row = build_scored_row(event, score_response)
    alert_row = build_alert_row(event, score_response)
    buffer.add(scored_row, alert_row)

    return True


def _consumer_config() -> dict[str, Any]:
    config = kafka_config()
    config["group.id"] = CONSUMER_GROUP_ID
    config["auto.offset.reset"] = "earliest"
    # At-least-once with a hard boundary: offsets are committed manually in
    # run(), strictly AFTER the corresponding rows are flushed to the bank DB.
    # A crash between flush and commit re-delivers already-written events,
    # which the PK-dedupe in _insert_rows_swallow_duplicates absorbs — so the
    # observable result in SQL is effectively-once.
    config["enable.auto.commit"] = False
    return config


def _commit_offsets(consumer: Any) -> None:
    """Synchronously commit the consumer's current offsets, tolerating the
    no-offsets-yet case (nothing consumed since the last commit)."""
    from confluent_kafka import KafkaException

    try:
        consumer.commit(asynchronous=False)
    except KafkaException as exc:
        logger.debug("offset commit skipped: %s", exc)


def run(max_events: int | None) -> int:
    """Consume from KAFKA_TOPIC_TRANSACTIONS and score/write until `max_events`
    have been consumed (or forever if None). Returns the count of events
    successfully scored.

    Delivery semantics: auto-commit is disabled; offsets are committed
    synchronously immediately AFTER each SQL flush (and once more on
    shutdown), so an event's offset is never committed before its scored row
    is durable in the bank DB. Redelivery after a crash is absorbed by the
    event_id PK dedupe on insert."""
    from confluent_kafka import Consumer

    consumer = Consumer(_consumer_config())
    consumer.subscribe([KAFKA_TOPIC_TRANSACTIONS])

    session = requests.Session()
    engine = get_engine()
    buffer = ScoreBuffer()

    consumed = 0
    scored = 0
    try:
        while max_events is None or consumed < max_events:
            msg = consumer.poll(1.0)
            if msg is None:
                # Idle stream: flush the sub-batch tail once the time
                # threshold passes, or it would sit buffered (and its offsets
                # uncommitted) until the next message arrives.
                if buffer.should_flush():
                    flush(engine, buffer)
                    _commit_offsets(consumer)
                continue
            if msg.error():
                logger.warning("kafka consumer error: %s", msg.error())
                continue

            consumed += 1
            try:
                event = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError) as exc:
                logger.warning("skipping unparseable message: %s", exc)
                continue

            if handle_event(event, session, engine, buffer):
                scored += 1

            if buffer.should_flush():
                flush(engine, buffer)
                _commit_offsets(consumer)
    finally:
        if buffer.scored_rows or buffer.alert_rows:
            flush(engine, buffer)
        if consumed:
            _commit_offsets(consumer)
        consumer.close()

    return scored


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Consume transaction events, score them via /score, write results to the bank DB."
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=SCORER_MAX_EVENTS,
        help="Stop after consuming N events (for tests/smoke).",
    )
    args = parser.parse_args(argv)

    scored = run(max_events=args.max_events)
    print(f"scored {scored} events")


if __name__ == "__main__":
    main(sys.argv[1:])
