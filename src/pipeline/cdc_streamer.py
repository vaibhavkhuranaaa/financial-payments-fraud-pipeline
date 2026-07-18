"""CDC streamer — SQL Server change tables -> Debezium-shaped Kafka events (ticket 11, v1.2).

Data flow
---------
Reads the *real* SQL Server CDC change table for ``bank.card_transactions``
(populated from the transaction log by the ``src.bank.cdc --scan`` pump) in
LSN-windowed batches via ``cdc.fn_cdc_get_all_changes_bank_card_transactions``
and produces one Debezium-envelope-shaped JSON event per change row onto
``KAFKA_TOPIC_CDC`` (default ``bankdb.frauddemo.bank.card_transactions``) —
byte-compatible with what the Debezium SQL Server source connector would
produce, so ``src.pipeline.cdc_transformer`` (and everything downstream)
cannot tell the difference.

Why this exists instead of Debezium itself
------------------------------------------
Debezium's SQL Server streaming loop advances its LSN position with
``sys.fn_cdc_increment_lsn``, which is CLR-backed — and Azure SQL Edge (the
only ARM-native SQL Server image, see ADR 0002) ships with CLR permanently
disabled (``sp_configure 'clr enabled'`` is rejected as unsupported), so the
connector snapshots fine and then fails every streaming iteration. Verified
live; see ADR 0003. Everything *else* about Edge CDC works: change tables,
``fn_cdc_get_min_lsn``/``fn_cdc_get_max_lsn``/``fn_cdc_get_all_changes``.
This module therefore replaces exactly one missing piece — LSN increment is
10 bytes of big-endian arithmetic (``_increment_lsn``) done in Python — and
keeps the real log-derived CDC mechanics. Against full SQL Server / Azure
SQL, drop in the real connector unchanged: the config is checked in at
``docker/connect/bankdb-source.json`` (compose profile ``debezium``) and the
topic contract is identical.

Delivery semantics
------------------
At-least-once with the same commit-after-flush boundary as the scorer and
transformer: the LSN offset is persisted to ``bank.cdc_offsets`` only AFTER
``producer.flush()`` succeeds for the window, so a crash between flush and
persist re-emits the window and the duplicates are absorbed downstream by
the ``event_id`` PK dedupe.

Testability
-----------
``_increment_lsn``, ``row_to_envelope`` and ``read_offset``/``write_offset``
are pure/engine-injectable; ``stream_once(engine, producer)`` performs one
window read+produce+persist cycle and is exercised with mocks. The
confluent_kafka Producer only appears in ``run()``/``main()``.

CLI: ``python -m src.pipeline.cdc_streamer [--interval S] [--max-batches N]``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import Engine, text

from src.bank.db import get_engine
from src.pipeline.ingestion import kafka_config

load_dotenv()

logger = logging.getLogger(__name__)

# --- Env-driven configuration (defaults mirror .env.example) ---------------

KAFKA_TOPIC_CDC = os.environ.get("KAFKA_TOPIC_CDC", "bankdb.frauddemo.bank.card_transactions")
CDC_POLL_INTERVAL_S = float(os.environ.get("CDC_POLL_INTERVAL_S", "1.0"))

CONSUMER_NAME = "cdc-streamer"
CAPTURE_INSTANCE = "bank_card_transactions"

# CDC __$operation codes for fn_cdc_get_all_changes(..., 'all'):
# 1=delete, 2=insert, 4=update (after image). Mapped onto Debezium op codes.
_OPERATION_TO_OP = {1: "d", 2: "c", 4: "u"}

_META_COLUMNS = ("__$start_lsn", "__$end_lsn", "__$seqval", "__$operation", "__$update_mask")

_READ_OFFSET_SQL = text("SELECT last_lsn FROM bank.cdc_offsets WHERE consumer_name = :name")

_WRITE_OFFSET_SQL = text(
    """
    MERGE bank.cdc_offsets AS target
    USING (SELECT :name AS consumer_name) AS src
    ON target.consumer_name = src.consumer_name
    WHEN MATCHED THEN
        UPDATE SET last_lsn = :lsn, updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (consumer_name, last_lsn) VALUES (:name, :lsn);
    """
)

_LSN_RANGE_SQL = text(
    f"SELECT sys.fn_cdc_get_min_lsn('{CAPTURE_INSTANCE}'), sys.fn_cdc_get_max_lsn()"
)

_CHANGES_SQL = text(
    f"""
    SELECT * FROM cdc.fn_cdc_get_all_changes_{CAPTURE_INSTANCE}(:from_lsn, :to_lsn, N'all')
    ORDER BY __$start_lsn, __$seqval
    """
)


def _increment_lsn(lsn: bytes) -> bytes:
    """Add 1 to a BINARY(10) LSN, big-endian — the pure-arithmetic equivalent
    of ``sys.fn_cdc_increment_lsn`` (CLR-backed, unavailable on SQL Edge)."""
    value = int.from_bytes(lsn, "big") + 1
    return value.to_bytes(len(lsn), "big")


def _json_value(value: Any) -> Any:
    """Normalize SQL column values to the JSON shapes Debezium's JsonConverter
    emits with decimal.handling.mode=double / time.precision.mode=connect:
    DATETIME2 -> epoch millis int, DECIMAL -> float, everything else as-is."""
    if isinstance(value, datetime):
        return int(value.replace(tzinfo=timezone.utc).timestamp() * 1000)
    if isinstance(value, Decimal):
        return float(value)
    return value


def row_to_envelope(row: dict[str, Any]) -> dict[str, Any] | None:
    """Map one change-table row (column dict incl. __$ meta columns) onto a
    Debezium-shaped envelope, or None for CDC operation codes that have no
    Debezium 'all'-mode equivalent (3 = update before-image, not returned by
    'all' but tolerated defensively)."""
    op = _OPERATION_TO_OP.get(row["__$operation"])
    if op is None:
        return None
    after = {k: _json_value(v) for k, v in row.items() if k not in _META_COLUMNS}
    return {
        "before": None,
        "after": None if op == "d" else after,
        "op": op,
        "ts_ms": int(time.time() * 1000),
        "source": {
            "connector": CONSUMER_NAME,
            "db": "frauddemo",
            "schema": "bank",
            "table": "card_transactions",
            "lsn": row["__$start_lsn"].hex(),
        },
    }


def read_offset(engine: Engine) -> bytes | None:
    """Return the last durably-produced LSN for this consumer, or None."""
    with engine.connect() as conn:
        row = conn.execute(_READ_OFFSET_SQL, {"name": CONSUMER_NAME}).fetchone()
    return bytes(row[0]) if row and row[0] is not None else None


def write_offset(engine: Engine, lsn: bytes) -> None:
    with engine.begin() as conn:
        conn.execute(_WRITE_OFFSET_SQL, {"name": CONSUMER_NAME, "lsn": lsn})


def stream_once(engine: Engine, producer: Any) -> int:
    """One cycle: read the next LSN window from the change table, produce one
    envelope per change row, flush the producer, then (and only then) persist
    the window's end LSN. Returns the number of events produced (0 when the
    window is empty)."""
    with engine.connect() as conn:
        min_lsn, max_lsn = conn.execute(_LSN_RANGE_SQL).fetchone()
    if max_lsn is None or min_lsn is None:
        return 0  # capture instance not ready yet

    last = read_offset(engine)
    from_lsn = _increment_lsn(bytes(last)) if last is not None else bytes(min_lsn)
    # A cleaned-up change table can leave the stored offset below min_lsn.
    if from_lsn < bytes(min_lsn):
        from_lsn = bytes(min_lsn)
    if from_lsn > bytes(max_lsn):
        return 0  # nothing new

    produced = 0
    with engine.connect() as conn:
        result = conn.execute(_CHANGES_SQL, {"from_lsn": from_lsn, "to_lsn": max_lsn})
        columns = list(result.keys())
        for values in result:
            row = dict(zip(columns, values))
            envelope = row_to_envelope(row)
            if envelope is None:
                continue
            # Keyed like Debezium keys SQL Server topics: by primary key.
            key = json.dumps({"event_id": row["event_id"]})
            producer.produce(KAFKA_TOPIC_CDC, key=key, value=json.dumps(envelope))
            produced += 1

    producer.flush(10.0)
    write_offset(engine, bytes(max_lsn))
    return produced


def run(interval: float, max_batches: int | None) -> int:
    """Loop stream_once every `interval` seconds (forever if `max_batches` is
    None). Returns total events produced."""
    from confluent_kafka import Producer

    engine = get_engine()
    producer = Producer(kafka_config())

    total = 0
    batches = 0
    while max_batches is None or batches < max_batches:
        try:
            total += stream_once(engine, producer)
        except Exception as exc:  # noqa: BLE001 — pump must survive transient DB/broker errors
            logger.warning("cdc stream cycle failed (will retry): %s", exc)
        batches += 1
        if max_batches is None or batches < max_batches:
            time.sleep(interval)
    return total


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Stream bank.card_transactions CDC changes onto Kafka as Debezium-shaped events."
    )
    parser.add_argument(
        "--interval", type=float, default=CDC_POLL_INTERVAL_S, help="Seconds between change-table polls."
    )
    parser.add_argument(
        "--max-batches", type=int, default=None, help="Stop after N poll cycles (for tests/smoke)."
    )
    args = parser.parse_args(argv)

    total = run(interval=args.interval, max_batches=args.max_batches)
    print(f"produced {total} change events")


if __name__ == "__main__":
    main(sys.argv[1:])
