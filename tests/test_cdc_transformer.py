"""Unit tests for src.pipeline.cdc_transformer.

Hermetic — no live Kafka Connect/Debezium/broker. `envelope_to_event` is
pure (dict/None in, `(event | None, reason | None)` out) and is exercised
directly against realistic Debezium envelope shapes; the mapped event is
additionally checked against `validate_event` (contract-v1) to make sure the
transform actually produces a schema-valid event, not just a dict.

`run()`'s Avro produce path (ADR 0006) is exercised hermetically by
monkeypatching `confluent_kafka.Consumer`/`Producer` and
`schema_registry.registry_client` with fakes — no broker, no live registry.
"""

from __future__ import annotations

import io
import json
import struct

import fastavro

from src.pipeline import schema_registry as sr
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


# --- run(): Avro produce path (ADR 0006) ------------------------------------


class _FakeMessage:
    def __init__(self, value: bytes | None) -> None:
        self._value = value

    def error(self):
        return None

    def value(self) -> bytes | None:
        return self._value


class _FakeConsumer:
    """Stands in for confluent_kafka.Consumer: replays a fixed queue of
    messages, then None. subscribe/commit/close are no-ops."""

    def __init__(self, *_args, **_kwargs) -> None:
        self.queue: list[_FakeMessage] = []

    def subscribe(self, _topics) -> None:
        pass

    def poll(self, _timeout):
        if self.queue:
            return self.queue.pop(0)
        return None

    def commit(self, asynchronous: bool = False):
        return None

    def close(self) -> None:
        pass


class _FakeProducer:
    def __init__(self, *_args, **_kwargs) -> None:
        self.messages: list[dict] = []

    def produce(self, topic, key=None, value=None):
        self.messages.append({"topic": topic, "key": key, "value": value})

    def flush(self, *_args, **_kwargs):
        return 0


class _FakeRegistryClient:
    def __init__(self, schema_id: int = 1) -> None:
        self.schema_id = schema_id

    def get_subjects(self):
        return ["transactions-value"]

    def register_schema(self, subject_name, schema, normalize_schemas=False) -> int:
        return self.schema_id

    def get_compatibility(self, subject_name=None) -> str:
        return "BACKWARD"

    def set_compatibility(self, subject_name=None, level=None) -> str:
        return level


def _decode_avro_value(raw: bytes) -> dict:
    magic, schema_id = struct.unpack(">bI", raw[:5])
    assert magic == 0
    with open(sr._AVRO_SCHEMA_PATH, encoding="utf-8") as f:
        avro_schema = fastavro.parse_schema(json.load(f))
    return fastavro.schemaless_reader(io.BytesIO(raw[5:]), avro_schema)


def _patch_kafka_clients(monkeypatch, consumer: _FakeConsumer, producer: _FakeProducer) -> None:
    import confluent_kafka

    monkeypatch.setattr(confluent_kafka, "Consumer", lambda *_a, **_kw: consumer)
    monkeypatch.setattr(confluent_kafka, "Producer", lambda *_a, **_kw: producer)
    monkeypatch.setattr(sr, "registry_client", lambda: _FakeRegistryClient())


def test_run_produces_confluent_framed_avro_value(monkeypatch) -> None:
    import src.pipeline.cdc_transformer as cdc_module

    consumer = _FakeConsumer()
    consumer.queue = [_FakeMessage(json.dumps(_envelope("c", AFTER_ROW)).encode("utf-8"))]
    producer = _FakeProducer()
    _patch_kafka_clients(monkeypatch, consumer, producer)

    produced = cdc_module.run(max_events=1)

    assert produced == 1
    txn_messages = [m for m in producer.messages if m["topic"] == cdc_module.KAFKA_TOPIC_TRANSACTIONS]
    assert len(txn_messages) == 1
    msg = txn_messages[0]
    assert isinstance(msg["value"], bytes)
    assert msg["value"][0] == 0x00
    decoded = _decode_avro_value(msg["value"])
    assert decoded["card_token"] == "a" * 64
    assert msg["key"] == "a" * 64


def test_run_dlq_payloads_remain_json_on_validation_failure(monkeypatch) -> None:
    import src.pipeline.cdc_transformer as cdc_module

    invalid_after = dict(AFTER_ROW)
    del invalid_after["mcc"]  # required field missing -> fails contract validation
    consumer = _FakeConsumer()
    consumer.queue = [_FakeMessage(json.dumps(_envelope("c", invalid_after)).encode("utf-8"))]
    producer = _FakeProducer()
    _patch_kafka_clients(monkeypatch, consumer, producer)

    produced = cdc_module.run(max_events=1)

    assert produced == 0
    dlq_messages = [m for m in producer.messages if m["topic"] == cdc_module.KAFKA_TOPIC_DLQ]
    assert len(dlq_messages) == 1
    payload = json.loads(dlq_messages[0]["value"])
    assert "error" in payload and "raw" in payload


def test_run_avro_serialization_failure_routes_to_dlq_and_counts(monkeypatch, caplog) -> None:
    import src.pipeline.cdc_transformer as cdc_module

    consumer = _FakeConsumer()
    consumer.queue = [_FakeMessage(json.dumps(_envelope("c", AFTER_ROW)).encode("utf-8"))]
    producer = _FakeProducer()
    _patch_kafka_clients(monkeypatch, consumer, producer)

    def _raising_serializer(*_a, **_kw):
        raise ValueError("boom: schema mismatch")

    monkeypatch.setattr(sr, "build_avro_serializer", lambda _client: _raising_serializer)

    with caplog.at_level("INFO", logger="src.pipeline.cdc_transformer"):
        produced = cdc_module.run(max_events=1)

    assert produced == 0
    txn_messages = [m for m in producer.messages if m["topic"] == cdc_module.KAFKA_TOPIC_TRANSACTIONS]
    assert txn_messages == []
    dlq_messages = [m for m in producer.messages if m["topic"] == cdc_module.KAFKA_TOPIC_DLQ]
    assert len(dlq_messages) == 1
    payload = json.loads(dlq_messages[0]["value"])
    assert "avro serialization failed" in payload["error"]
    assert "avro_errors=1" in caplog.text
