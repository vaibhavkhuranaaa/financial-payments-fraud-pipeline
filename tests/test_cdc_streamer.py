"""Unit tests for src.pipeline.cdc_streamer.

Hermetic — no Kafka broker, no live DB. `_increment_lsn` / `row_to_envelope`
are pure; `stream_once` is exercised with a mocked SQLAlchemy engine and a
mocked producer to pin the produce → flush → persist-offset ordering (the
delivery-semantics boundary).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

from src.pipeline import cdc_streamer

CHANGE_ROW = {
    "__$start_lsn": bytes.fromhex("0000002500000c080094"),
    "__$end_lsn": None,
    "__$seqval": bytes.fromhex("0000002500000c080093"),
    "__$operation": 2,
    "__$update_mask": b"\xff",
    "event_id": "11111111-1111-1111-1111-111111111111",
    "schema_version": "1.0.0",
    "card_token": "a" * 64,
    "user_id": "19",
    "event_time": datetime(2019, 2, 13, 14, 6, 0),
    "amount": Decimal("80.00"),
    "currency": "USD",
    "channel": "chip",
    "merchant_name": "Some Merchant",
    "merchant_city": "Tucson",
    "merchant_state": "AZ",
    "merchant_country": "US",
    "zip": "85719",
    "mcc": 4829,
    "errors": None,
    "is_fraud": False,
    "inserted_at": datetime(2026, 7, 18, 12, 0, 0),
}


def test_increment_lsn_adds_one_big_endian() -> None:
    assert cdc_streamer._increment_lsn(b"\x00" * 10) == b"\x00" * 9 + b"\x01"
    # carry across bytes
    assert (
        cdc_streamer._increment_lsn(b"\x00" * 8 + b"\x00\xff")
        == b"\x00" * 8 + b"\x01\x00"
    )


def test_row_to_envelope_insert_maps_debezium_shape() -> None:
    envelope = cdc_streamer.row_to_envelope(CHANGE_ROW)

    assert envelope is not None
    assert envelope["op"] == "c"
    assert envelope["before"] is None
    after = envelope["after"]
    # __$ meta columns never leak into the payload
    assert not any(k.startswith("__$") for k in after)
    # DATETIME2 -> epoch millis (what time.precision.mode=connect emits)
    assert after["event_time"] == int(
        datetime(2019, 2, 13, 14, 6, 0).replace(tzinfo=__import__("datetime").timezone.utc).timestamp() * 1000
    )
    # DECIMAL -> float (what decimal.handling.mode=double emits)
    assert after["amount"] == 80.0
    assert isinstance(after["amount"], float)
    assert after["event_id"] == CHANGE_ROW["event_id"]
    assert envelope["source"]["lsn"] == CHANGE_ROW["__$start_lsn"].hex()


def test_row_to_envelope_delete_has_no_after() -> None:
    envelope = cdc_streamer.row_to_envelope(dict(CHANGE_ROW, **{"__$operation": 1}))
    assert envelope is not None
    assert envelope["op"] == "d"
    assert envelope["after"] is None


def test_row_to_envelope_update_maps_after_image() -> None:
    envelope = cdc_streamer.row_to_envelope(dict(CHANGE_ROW, **{"__$operation": 4}))
    assert envelope is not None
    assert envelope["op"] == "u"
    assert envelope["after"]["event_id"] == CHANGE_ROW["event_id"]


def test_row_to_envelope_unknown_operation_is_skipped() -> None:
    assert cdc_streamer.row_to_envelope(dict(CHANGE_ROW, **{"__$operation": 3})) is None


def test_envelope_round_trips_through_cdc_transformer() -> None:
    """The streamer's output must be indistinguishable from Debezium's as far
    as the transformer is concerned: envelope -> contract-v1 event -> valid."""
    from src.pipeline.cdc_transformer import envelope_to_event
    from src.pipeline.ingestion import validate_event

    envelope = cdc_streamer.row_to_envelope(CHANGE_ROW)
    event, reason = envelope_to_event(envelope)

    assert reason is None
    assert event is not None
    assert validate_event(event) is None
    assert event["card_token"] == CHANGE_ROW["card_token"]
    assert event["amount"] == 80.0
    assert event["event_time"] == "2019-02-13T14:06:00.000Z"


class _FakeResult:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def keys(self):  # noqa: ANN201 — mirrors SQLAlchemy Result
        return list(self._rows[0].keys()) if self._rows else []

    def __iter__(self):
        return iter(tuple(r.values()) for r in self._rows)

    def fetchone(self):
        return next(iter(self), None)


def test_stream_once_produces_then_flushes_then_persists_offset() -> None:
    """Delivery boundary: offset write must come strictly after producer.flush."""
    order: list[str] = []

    min_lsn = b"\x00" * 9 + b"\x01"
    max_lsn = b"\x00" * 9 + b"\x05"

    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    engine.begin.return_value.__enter__.return_value = conn

    def execute_side_effect(stmt, params=None):
        sql = str(stmt)
        if "fn_cdc_get_min_lsn" in sql:
            return _FakeResult([{"min": min_lsn, "max": max_lsn}])
        if "MERGE" in sql:
            order.append("persist-offset")
            assert params["lsn"] == max_lsn
            return MagicMock()
        if "cdc_offsets" in sql and "SELECT" in sql:
            return _FakeResult([])  # no stored offset yet -> start at min_lsn
        if "fn_cdc_get_all_changes" in sql:
            assert params["from_lsn"] == min_lsn
            assert params["to_lsn"] == max_lsn
            return _FakeResult([CHANGE_ROW])
        raise AssertionError(f"unexpected SQL: {sql[:80]}")

    conn.execute.side_effect = execute_side_effect

    producer = MagicMock()
    producer.produce.side_effect = lambda *a, **k: order.append("produce")
    producer.flush.side_effect = lambda *a, **k: order.append("flush")

    produced = cdc_streamer.stream_once(engine, producer)

    assert produced == 1
    assert order == ["produce", "flush", "persist-offset"]


def test_stream_once_no_new_changes_is_a_noop() -> None:
    max_lsn = b"\x00" * 9 + b"\x05"

    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn

    def execute_side_effect(stmt, params=None):
        sql = str(stmt)
        if "fn_cdc_get_min_lsn" in sql:
            return _FakeResult([{"min": b"\x00" * 10, "max": max_lsn}])
        if "cdc_offsets" in sql:
            return _FakeResult([{"last_lsn": max_lsn}])  # already caught up
        raise AssertionError(f"unexpected SQL: {sql[:80]}")

    conn.execute.side_effect = execute_side_effect

    producer = MagicMock()
    assert cdc_streamer.stream_once(engine, producer) == 0
    producer.produce.assert_not_called()
