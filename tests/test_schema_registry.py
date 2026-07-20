"""Unit tests for src.pipeline.schema_registry (ADR 0006).

Hermetic — no live registry, no network. `registry_client`/`wait_for_registry`
/`ensure_backward_compatibility` are exercised against a fake
SchemaRegistryClient-shaped object; `build_avro_serializer` is exercised
against the real `confluent_kafka.schema_registry.avro.AvroSerializer` (no
network calls happen during construction or serialization for a
reference-free schema) wired to that same fake client, so the produced bytes
are genuine Confluent wire framing decodable with fastavro against the
checked-in contracts/transaction.avsc.
"""

from __future__ import annotations

import io
import json
import struct

import fastavro
import pytest

from src.pipeline import schema_registry as sr

SAMPLE_EVENT = {
    "schema_version": "1.0.0",
    "event_id": "b2b1c1a0-1111-4a2b-8c3d-0123456789ab",
    "event_time": "2019-02-13T14:06:00Z",
    "card_token": "a" * 64,
    "user_id": "19",
    "amount": 80.0,
    "currency": "USD",
    "channel": "chip",
    "merchant_name": "-4282466774399734331",
    "merchant_city": "Tucson",
    "merchant_state": "AZ",
    "merchant_country": "US",
    "zip": "85719",
    "mcc": 4829,
    "errors": None,
    "is_fraud": False,
}


class _FakeRegistryClient:
    """Stands in for confluent_kafka.schema_registry.SchemaRegistryClient.
    Never makes a network call; used both for direct helper tests and
    injected into a real AvroSerializer (safe because the checked-in schema
    has no named-schema references, so AvroSerializer.__init__ never calls
    the client)."""

    def __init__(self, schema_id: int = 7, reachable: bool = True, compatibility: str | None = None):
        self.schema_id = schema_id
        self._reachable = reachable
        self._compatibility = compatibility
        self.registered_subjects: list[str] = []
        self.set_compatibility_calls: list[tuple[str, str]] = []

    def get_subjects(self) -> list[str]:
        if not self._reachable:
            raise ConnectionError("registry not reachable")
        return ["transactions-value"]

    def register_schema(self, subject_name, schema, normalize_schemas=False) -> int:
        self.registered_subjects.append(subject_name)
        return self.schema_id

    def get_compatibility(self, subject_name=None) -> str:
        from confluent_kafka.schema_registry.error import SchemaRegistryError

        if self._compatibility is None:
            raise SchemaRegistryError(404, 40401, "Subject not found")
        return self._compatibility

    def set_compatibility(self, subject_name=None, level=None) -> str:
        self.set_compatibility_calls.append((subject_name, level))
        self._compatibility = level
        return level


# --- registry_url ------------------------------------------------------------


def test_registry_url_reads_env(monkeypatch) -> None:
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
    assert sr.registry_url() == "http://redpanda:8081"


def test_registry_url_raises_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("SCHEMA_REGISTRY_URL", raising=False)
    with pytest.raises(RuntimeError, match="SCHEMA_REGISTRY_URL"):
        sr.registry_url()


# --- wait_for_registry ---------------------------------------------------


def test_wait_for_registry_returns_when_reachable() -> None:
    client = _FakeRegistryClient(reachable=True)
    sr.wait_for_registry(client, timeout_s=1.0)  # must not raise


def test_wait_for_registry_times_out_when_unreachable(monkeypatch) -> None:
    monkeypatch.setattr(sr, "REGISTRY_WAIT_POLL_INTERVAL_S", 0.01)
    client = _FakeRegistryClient(reachable=False)
    with pytest.raises(TimeoutError, match="schema registry"):
        sr.wait_for_registry(client, timeout_s=0.03)


# --- ensure_backward_compatibility ---------------------------------------


def test_ensure_backward_compatibility_sets_when_unset() -> None:
    client = _FakeRegistryClient(compatibility=None)
    sr.ensure_backward_compatibility(client)
    assert client.set_compatibility_calls == [("transactions-value", "BACKWARD")]


def test_ensure_backward_compatibility_noop_when_already_backward() -> None:
    client = _FakeRegistryClient(compatibility="BACKWARD")
    sr.ensure_backward_compatibility(client)
    assert client.set_compatibility_calls == []


def test_ensure_backward_compatibility_sets_when_different_level() -> None:
    client = _FakeRegistryClient(compatibility="FORWARD")
    sr.ensure_backward_compatibility(client)
    assert client.set_compatibility_calls == [("transactions-value", "BACKWARD")]


# --- build_avro_serializer / wire framing --------------------------------


def _decode_confluent_frame(raw: bytes) -> tuple[int, bytes]:
    magic, schema_id = struct.unpack(">bI", raw[:5])
    assert magic == 0
    return schema_id, raw[5:]


def test_build_avro_serializer_produces_confluent_framed_bytes() -> None:
    from confluent_kafka.serialization import MessageField, SerializationContext

    client = _FakeRegistryClient(schema_id=42)
    serializer = sr.build_avro_serializer(client)

    raw = serializer(SAMPLE_EVENT, SerializationContext("transactions", MessageField.VALUE))

    assert raw[0] == 0x00
    schema_id, payload = _decode_confluent_frame(raw)
    assert schema_id == 42
    assert client.registered_subjects == ["transactions-value"]

    with open(sr._AVRO_SCHEMA_PATH, encoding="utf-8") as f:
        avro_schema = fastavro.parse_schema(json.load(f))
    decoded = fastavro.schemaless_reader(io.BytesIO(payload), avro_schema)
    assert decoded == SAMPLE_EVENT


def test_build_avro_serializer_round_trips_nullable_fields_null() -> None:
    from confluent_kafka.serialization import MessageField, SerializationContext

    client = _FakeRegistryClient(schema_id=1)
    serializer = sr.build_avro_serializer(client)
    event = dict(SAMPLE_EVENT, zip=None, errors=None, is_fraud=None)

    raw = serializer(event, SerializationContext("transactions", MessageField.VALUE))
    _, payload = _decode_confluent_frame(raw)

    with open(sr._AVRO_SCHEMA_PATH, encoding="utf-8") as f:
        avro_schema = fastavro.parse_schema(json.load(f))
    decoded = fastavro.schemaless_reader(io.BytesIO(payload), avro_schema)
    assert decoded == event
