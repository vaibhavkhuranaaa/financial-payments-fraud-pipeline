"""CDC envelope transformer — Debezium change events -> contract-v1 events (ticket 11, v1.2).

Data flow
---------
Consumes Debezium SQL Server source-connector change events from
``KAFKA_TOPIC_CDC`` (default ``bankdb.frauddemo.bank.card_transactions``, the topic
Debezium names as ``<topic.prefix>.<schemaName>.<tableName>``). Each message
value is a flat JSON envelope (``key.converter.schemas.enable`` /
``value.converter.schemas.enable`` are both ``false``) shaped like::

    {"before": null, "after": {...columns...}, "op": "c", "ts_ms": ..., ...}

``envelope_to_event`` maps the ``after`` row of insert/snapshot events
(``op`` ``"c"``/``"r"``) back onto the exact contract-v1 shape
(``contracts/transaction.schema.json``) that ``src.pipeline.ingestion``
already produces, so **zero changes** are needed downstream (Spark features
job, scorer, API): update/delete events (``op`` ``"u"``/``"d"``) are skipped
(authorizations are immutable — there is nothing to reconcile), and
tombstone messages (Kafka's null-value marker for a deleted key) are
skipped too.

Valid transformed events are validated against the contract (like the
replay producer) and produced to ``KAFKA_TOPIC_TRANSACTIONS`` keyed by
``card_token``; events that fail to map or fail validation are produced to
``KAFKA_TOPIC_DLQ`` instead of being dropped silently.

Delivery semantics mirror ``src.pipeline.scorer``: ``enable.auto.commit`` is
off, and offsets are committed synchronously only AFTER the producer has
been flushed — every ``BATCH_SIZE`` messages, or on an idle poll — so an
event is never "acked" to Kafka before it has actually been handed to the
broker for the downstream topic.

Testability
-----------
``envelope_to_event`` is pure (dict/None in, ``(event | None, reason | None)``
out) and is exercised directly with no broker. The confluent_kafka
Consumer/Producer only appear in ``run()``/``main()``.

CLI: ``python -m src.pipeline.cdc_transformer [--max-events N]``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv

from src.pipeline.ingestion import (
    KAFKA_TOPIC_DLQ,
    KAFKA_TOPIC_TRANSACTIONS,
    kafka_config,
    validate_event,
)

load_dotenv()

logger = logging.getLogger(__name__)

# --- Env-driven configuration (defaults mirror .env.example) ---------------

KAFKA_TOPIC_CDC = os.environ.get("KAFKA_TOPIC_CDC", "bankdb.frauddemo.bank.card_transactions")

CONSUMER_GROUP_ID = "cdc-transformer"

BATCH_SIZE = 100

_IMMUTABLE_OPS = ("u", "d")
_MAPPABLE_OPS = ("c", "r")


def _epoch_millis_to_iso(ms: int) -> str:
    """Convert an epoch-millis int (``time.precision.mode=connect``) to a UTC
    ISO-8601 string with a ``Z`` suffix, matching contract-v1's ``event_time``."""
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _normalize_bool(raw: Any) -> bool | None:
    """``is_fraud`` (SQL ``BIT NULL``) may arrive as a real bool, 0/1, or
    None depending on converter behavior; normalize to bool | None."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    if raw in (0, 1):
        return bool(raw)
    raise ValueError(f"unexpected is_fraud value: {raw!r}")


def _strip_if_str(value: Any) -> Any:
    """Fixed-length CHAR columns (card_token CHAR(64), currency CHAR(3),
    merchant_country CHAR(2)) arrive space-padded from SQL Server; NVARCHAR
    columns don't need this but stripping a non-string is a no-op anyway."""
    return value.strip() if isinstance(value, str) else value


def _after_to_event(after: dict[str, Any]) -> dict[str, Any]:
    """Map one Debezium `after` row (bank.card_transactions columns) onto a
    contract-v1 event dict. Raises KeyError/TypeError/ValueError for rows so
    malformed that mapping itself is impossible; the caller routes those to
    the DLQ via `envelope_to_event`'s (None, reason) return."""
    return {
        "schema_version": after["schema_version"],
        "event_id": after["event_id"],
        "event_time": _epoch_millis_to_iso(int(after["event_time"])),
        "card_token": _strip_if_str(after["card_token"]),
        "user_id": after["user_id"],
        "amount": float(after["amount"]),
        "currency": _strip_if_str(after["currency"]),
        "channel": after["channel"],
        "merchant_name": after.get("merchant_name"),
        "merchant_city": after.get("merchant_city"),
        "merchant_state": after.get("merchant_state"),
        "merchant_country": _strip_if_str(after.get("merchant_country")),
        "zip": after.get("zip"),
        "mcc": after.get("mcc"),
        "errors": after.get("errors"),
        "is_fraud": _normalize_bool(after.get("is_fraud")),
    }


def envelope_to_event(value: dict[str, Any] | None) -> tuple[dict[str, Any] | None, str | None]:
    """Map one Debezium change-event envelope to a contract-v1 event.

    Returns ``(event, None)`` for insert/snapshot ops (``"c"``/``"r"``).
    Returns ``(None, None)`` for update/delete ops (``"u"``/``"d"`` —
    authorizations are immutable, nothing to reconcile) and for
    tombstones/None values. Returns ``(None, reason)`` when the envelope's
    `op` is unrecognized or mapping the `after` row fails.
    """
    if value is None:
        # Tombstone: Kafka's null-value marker following a delete's tombstone
        # (or a defensively-passed null envelope). Nothing to do.
        return None, None

    op = value.get("op")
    if op in _IMMUTABLE_OPS:
        return None, None
    if op not in _MAPPABLE_OPS:
        return None, f"unsupported op: {op!r}"

    after = value.get("after")
    if not after:
        return None, "missing 'after' payload"

    try:
        event = _after_to_event(after)
    except (KeyError, TypeError, ValueError) as exc:
        return None, f"mapping error: {exc}"

    return event, None


def kafka_consumer_config() -> dict[str, Any]:
    config = kafka_config()
    config["group.id"] = CONSUMER_GROUP_ID
    config["auto.offset.reset"] = "earliest"
    # Same commit-after-flush boundary as scorer.run(): offsets are committed
    # manually in run(), strictly AFTER the producer has been flushed.
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
    """Consume from KAFKA_TOPIC_CDC and transform/produce until `max_events`
    have been consumed (or forever if None). Returns the count of events
    successfully produced to KAFKA_TOPIC_TRANSACTIONS.

    Delivery semantics: auto-commit is disabled; the producer is flushed and
    offsets committed synchronously every BATCH_SIZE messages or on an idle
    poll (no message within the poll timeout), so a message's offset is
    never committed before it has actually been produced downstream."""
    from confluent_kafka import Consumer, Producer

    consumer = Consumer(kafka_consumer_config())
    consumer.subscribe([KAFKA_TOPIC_CDC])
    producer = Producer(kafka_config())

    consumed = 0
    produced = 0
    invalid = 0
    skipped = 0
    since_flush = 0

    def _flush_and_commit() -> None:
        nonlocal since_flush
        if since_flush == 0:
            return
        producer.flush(10.0)
        _commit_offsets(consumer)
        since_flush = 0

    try:
        while max_events is None or consumed < max_events:
            msg = consumer.poll(1.0)
            if msg is None:
                _flush_and_commit()
                continue
            if msg.error():
                logger.warning("kafka consumer error: %s", msg.error())
                continue

            consumed += 1
            since_flush += 1
            raw = msg.value()

            try:
                value = json.loads(raw) if raw is not None else None
            except (json.JSONDecodeError, TypeError) as exc:
                logger.warning("skipping unparseable CDC message: %s", exc)
                producer.produce(KAFKA_TOPIC_DLQ, value=json.dumps({"raw": None, "error": f"unparseable: {exc}"}))
                invalid += 1
                if since_flush >= BATCH_SIZE:
                    _flush_and_commit()
                continue

            event, reason = envelope_to_event(value)

            if event is None and reason is None:
                skipped += 1
            elif event is None:
                producer.produce(KAFKA_TOPIC_DLQ, value=json.dumps({"raw": value, "error": reason}))
                invalid += 1
            else:
                validation_error = validate_event(event)
                if validation_error is None:
                    producer.produce(
                        KAFKA_TOPIC_TRANSACTIONS,
                        key=event["card_token"],
                        value=json.dumps(event),
                    )
                    produced += 1
                else:
                    producer.produce(
                        KAFKA_TOPIC_DLQ, value=json.dumps({"raw": value, "error": validation_error})
                    )
                    invalid += 1

            if since_flush >= BATCH_SIZE:
                _flush_and_commit()
    finally:
        _flush_and_commit()
        consumer.close()

    logger.info(
        "cdc-transformer: consumed=%d produced=%d invalid=%d skipped=%d",
        consumed,
        produced,
        invalid,
        skipped,
    )
    return produced


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Transform Debezium CDC change events into contract-v1 events on Kafka."
    )
    parser.add_argument(
        "--max-events", type=int, default=None, help="Stop after consuming N CDC messages (for tests/smoke)."
    )
    args = parser.parse_args(argv)

    produced = run(max_events=args.max_events)
    print(f"produced {produced} valid events")


if __name__ == "__main__":
    main(sys.argv[1:])
