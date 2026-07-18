"""Unit tests for src.pipeline.cdc_transformer.

Hermetic — no live Kafka Connect/Debezium/broker. `envelope_to_event` is
pure (dict/None in, `(event | None, reason | None)` out) and is exercised
directly against realistic Debezium envelope shapes; the mapped event is
additionally checked against `validate_event` (contract-v1) to make sure the
transform actually produces a schema-valid event, not just a dict.
"""

from __future__ import annotations

from src.pipeline.cdc_transformer import envelope_to_event
from src.pipeline.ingestion import validate_event

# Epoch millis for 2019-02-13T14:06:00.000Z.
EVENT_TIME_MS = 1550066760000

AFTER_ROW = {
    "event_id": "11111111-1111-1111-1111-111111111111",
    "schema_version": "1.0.0",
    "card_token": ("a" * 64 + "  ")[:66],  # simulate CHAR(64) space padding
    "user_id": "19",
    "event_time": EVENT_TIME_MS,
    "amount": 80.0,
    "currency": "USD",  # CHAR(3), no padding needed at exactly 3 chars
    "channel": "chip",
    "merchant_name": "Some Merchant",
    "merchant_city": "Tucson",
    "merchant_state": "AZ",
    "merchant_country": "US",  # CHAR(2), no padding needed at exactly 2 chars
    "zip": "85719",
    "mcc": 4829,
    "errors": None,
    "is_fraud": None,
    "inserted_at": 1550066761000,
}


def _envelope(op: str, after: dict | None, before: dict | None = None) -> dict:
    return {"before": before, "after": after, "op": op, "ts_ms": 1550066761000, "transaction": None}


def test_envelope_to_event_maps_insert_op_c() -> None:
    event, reason = envelope_to_event(_envelope("c", AFTER_ROW))

    assert reason is None
    assert event is not None
    assert event["event_id"] == AFTER_ROW["event_id"]
    assert event["schema_version"] == "1.0.0"
    assert event["event_time"] == "2019-02-13T14:06:00.000Z"
    assert event["card_token"] == "a" * 64  # padding stripped
    assert event["amount"] == 80.0
    assert isinstance(event["amount"], float)
    assert event["is_fraud"] is None
    assert validate_event(event) is None


def test_envelope_to_event_maps_snapshot_op_r() -> None:
    event, reason = envelope_to_event(_envelope("r", AFTER_ROW))

    assert reason is None
    assert event is not None
    assert validate_event(event) is None


def test_envelope_to_event_is_fraud_true_from_int_1() -> None:
    after = dict(AFTER_ROW, is_fraud=1)
    event, reason = envelope_to_event(_envelope("c", after))

    assert reason is None
    assert event["is_fraud"] is True


def test_envelope_to_event_is_fraud_false_from_int_0() -> None:
    after = dict(AFTER_ROW, is_fraud=0)
    event, reason = envelope_to_event(_envelope("c", after))

    assert reason is None
    assert event["is_fraud"] is False


def test_envelope_to_event_is_fraud_from_bool() -> None:
    after = dict(AFTER_ROW, is_fraud=True)
    event, reason = envelope_to_event(_envelope("c", after))

    assert reason is None
    assert event["is_fraud"] is True


def test_envelope_to_event_skips_update_op() -> None:
    event, reason = envelope_to_event(_envelope("u", AFTER_ROW, before=AFTER_ROW))

    assert event is None
    assert reason is None


def test_envelope_to_event_skips_delete_op() -> None:
    event, reason = envelope_to_event(_envelope("d", None, before=AFTER_ROW))

    assert event is None
    assert reason is None


def test_envelope_to_event_skips_tombstone_none_value() -> None:
    event, reason = envelope_to_event(None)

    assert event is None
    assert reason is None


def test_envelope_to_event_reason_on_unsupported_op() -> None:
    event, reason = envelope_to_event(_envelope("t", AFTER_ROW))

    assert event is None
    assert reason is not None
    assert "unsupported op" in reason


def test_envelope_to_event_reason_on_missing_after() -> None:
    event, reason = envelope_to_event(_envelope("c", None))

    assert event is None
    assert reason == "missing 'after' payload"


def test_envelope_to_event_reason_on_malformed_after() -> None:
    malformed = dict(AFTER_ROW)
    del malformed["event_time"]
    event, reason = envelope_to_event(_envelope("c", malformed))

    assert event is None
    assert reason is not None
    assert "mapping error" in reason


def test_envelope_to_event_reason_on_bad_is_fraud_value() -> None:
    after = dict(AFTER_ROW, is_fraud="maybe")
    event, reason = envelope_to_event(_envelope("c", after))

    assert event is None
    assert reason is not None
    assert "mapping error" in reason
