"""Ticket 11 CDC ingestion: replay the TabFormer CSV INTO bank.card_transactions.

Data flow
---------
Reuses the exact row -> event mapping `src.pipeline.ingestion` uses to
produce onto Kafka (`to_event`, `validate_event`, `_RateLimiter`,
`TOKENIZATION_SALT`), but the write target becomes the bank DB instead of a
Kafka topic: rows are INSERTed into `bank.card_transactions`, which makes SQL
the true system-of-record. Once CDC is enabled on that table
(`src.bank.cdc --enable`) and the scan pump is running (`src.bank.cdc
--scan`), Debezium (a later chunk of ticket 11) reads the resulting change
table directly — the replay CSV never touches Kafka on this path.

Validation happens here, at the system-of-record boundary, exactly like the
Kafka producer's contract-v1 validation: valid events are batch-INSERTed;
invalid rows are counted + logged. There is no DLQ topic on this path (there
is no Kafka involved) — a rejected row is simply not written to the OLTP
table, mirroring how a real core-banking system would reject a malformed
authorization at the point of entry rather than downstream.

Testability
-----------
`event_to_row(event)` is a pure dict -> dict mapper (event_time parsed to a
naive-UTC datetime, same approach as `src.pipeline.scorer._parse_event_time`)
that tests exercise directly, no DB required. `replay(...)` accepts an
injectable `engine` (like `scorer.handle_event`) so the batch-insert logic is
testable with a mocked SQLAlchemy engine.

CLI: `python -m src.bank.txn_writer [--input CSV] [--eps N] [--max-events N]`.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import Engine, text

from src.bank.db import get_engine
from src.pipeline.ingestion import (
    PRODUCER_EVENTS_PER_SEC,
    PRODUCER_INPUT_CSV,
    TOKENIZATION_SALT,
    _iter_csv_rows,
    _RateLimiter,
    to_event,
    validate_event,
)

load_dotenv()

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
FLUSH_INTERVAL_S = 2.0

_INSERT_CARD_TXN_SQL = text(
    """
    INSERT INTO bank.card_transactions
        (event_id, schema_version, card_token, user_id, event_time, amount, currency,
         channel, merchant_name, merchant_city, merchant_state, merchant_country,
         zip, mcc, errors, is_fraud)
    VALUES
        (:event_id, :schema_version, :card_token, :user_id, :event_time, :amount, :currency,
         :channel, :merchant_name, :merchant_city, :merchant_state, :merchant_country,
         :zip, :mcc, :errors, :is_fraud)
    """
)


def _parse_event_time(event_time: str) -> datetime:
    """Parse contract-v1's UTC ISO-8601 ``event_time`` (suffix ``Z``) to a naive
    UTC datetime, which is what DATETIME2 over pymssql expects."""
    return datetime.fromisoformat(event_time.replace("Z", "+00:00")).replace(tzinfo=None)


def event_to_row(event: dict[str, Any]) -> dict[str, Any]:
    """Map a contract-v1 event onto a bank.card_transactions row."""
    return {
        "event_id": event["event_id"],
        "schema_version": event["schema_version"],
        "card_token": event["card_token"],
        "user_id": event["user_id"],
        "event_time": _parse_event_time(event["event_time"]),
        "amount": event["amount"],
        "currency": event["currency"],
        "channel": event["channel"],
        "merchant_name": event.get("merchant_name"),
        "merchant_city": event.get("merchant_city"),
        "merchant_state": event.get("merchant_state"),
        "merchant_country": event.get("merchant_country"),
        "zip": event.get("zip"),
        "mcc": event.get("mcc"),
        "errors": event.get("errors"),
        "is_fraud": event.get("is_fraud"),
    }


def _flush(engine: Engine, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(_INSERT_CARD_TXN_SQL, rows)


def replay(
    input_path: str,
    salt: str,
    eps: float,
    max_events: int | None,
    engine: Engine | None,
) -> tuple[int, int]:
    """Replay `input_path` into bank.card_transactions (or count-only when
    `engine` is None). Returns (valid, invalid)."""
    limiter = _RateLimiter(eps)
    valid_count = 0
    invalid_count = 0
    buffer: list[dict[str, Any]] = []
    last_flush = time.monotonic()

    for i, row in enumerate(_iter_csv_rows(input_path)):
        if max_events is not None and i >= max_events:
            break
        limiter.wait()
        try:
            event = to_event(row, salt)
            reason = validate_event(event)
        except (KeyError, ValueError) as exc:
            event = None
            reason = f"mapping error: {exc}"

        if reason is None:
            valid_count += 1
            if engine is not None:
                buffer.append(event_to_row(event))
        else:
            invalid_count += 1
            logger.warning("invalid row skipped (no system-of-record insert): %s", reason)

        if engine is not None and (
            len(buffer) >= BATCH_SIZE or (buffer and time.monotonic() - last_flush >= FLUSH_INTERVAL_S)
        ):
            _flush(engine, buffer)
            buffer = []
            last_flush = time.monotonic()

    if engine is not None:
        _flush(engine, buffer)

    return valid_count, invalid_count


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Replay TabFormer CSV rows into bank.card_transactions (system of record)."
    )
    parser.add_argument("--input", default=PRODUCER_INPUT_CSV, help="Path to source CSV.")
    parser.add_argument(
        "--eps", type=float, default=PRODUCER_EVENTS_PER_SEC, help="Target events/sec."
    )
    parser.add_argument(
        "--max-events", type=int, default=None, help="Stop after N rows (for tests/smoke)."
    )
    args = parser.parse_args(argv)

    engine = get_engine()
    valid, invalid = replay(
        input_path=args.input,
        salt=TOKENIZATION_SALT,
        eps=args.eps,
        max_events=args.max_events,
        engine=engine,
    )
    print(f"wrote {valid} valid transactions to bank.card_transactions, {invalid} invalid rows skipped")


if __name__ == "__main__":
    main(sys.argv[1:])
