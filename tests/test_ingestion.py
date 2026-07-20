"""Unit tests for src.pipeline.ingestion: TabFormer row -> contract-v1 event.

No Kafka broker required — these exercise `to_event`/`validate_event`/
`kafka_config` directly, plus the CLI's --dry-run path against the real
sample CSV. The non-dry-run `replay()` path (Avro produce + DLQ, ADR 0006) is
exercised hermetically by monkeypatching `confluent_kafka.Producer` and
`schema_registry.registry_client` with fakes — no broker, no live registry.
"""

from __future__ import annotations

import copy
import io
import json
import struct

import fastavro

from src.pipeline import schema_registry as sr
from src.pipeline.ingestion import kafka_config, to_event, validate_event

SALT = "test-salt"


class _FakeProducer:
    """Stands in for confluent_kafka.Producer: records produce() calls,
    flush() is a no-op (nothing async to wait for)."""

    def __init__(self, *_args, **_kwargs):
        self.messages: list[dict] = []

    def produce(self, topic, key=None, value=None):
        self.messages.append({"topic": topic, "key": key, "value": value})

    def flush(self, *_args, **_kwargs):
        return 0


class _FakeRegistryClient:
    """Same shape as tests/test_schema_registry.py's fake — no network."""

    def __init__(self, schema_id: int = 1):
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


def _row(**overrides: object) -> dict[str, object]:
    base = {
        "User": "19",
        "Card": "4",
        "Year": "2019",
        "Month": "2",
        "Day": "13",
        "Time": "14:06",
        "Amount": "$80.00",
        "Use Chip": "Chip Transaction",
        "Merchant Name": "-4282466774399734331",
        "Merchant City": "Tucson",
        "Merchant State": "AZ",
        "Zip": "85719.0",
        "MCC": "4829",
        "Errors?": "",
        "Is Fraud?": "No",
    }
    base.update(overrides)
    return base


def test_to_event_maps_core_fields() -> None:
    event = to_event(_row(), SALT)
    assert event["schema_version"] == "1.0.0"
    assert event["event_time"] == "2019-02-13T14:06:00+00:00".replace("+00:00", "Z")
    assert event["amount"] == 80.00
    assert event["currency"] == "USD"
    assert event["channel"] == "chip"
    assert event["merchant_city"] == "Tucson"
    assert event["merchant_state"] == "AZ"
    assert event["merchant_country"] == "US"
    assert event["zip"] == "85719"
    assert event["mcc"] == 4829
    assert event["errors"] is None
    assert event["is_fraud"] is False
    assert len(event["card_token"]) == 64
    int(event["card_token"], 16)  # hex string


def test_amount_parsing_handles_negative_reversals() -> None:
    event = to_event(_row(Amount="$-54.00"), SALT)
    assert event["amount"] == -54.00


def test_channel_mapping() -> None:
    assert to_event(_row(**{"Use Chip": "Chip Transaction"}), SALT)["channel"] == "chip"
    assert to_event(_row(**{"Use Chip": "Swipe Transaction"}), SALT)["channel"] == "swipe"
    online_row = _row(**{"Use Chip": "Online Transaction", "Merchant City": " ONLINE", "Merchant State": ""})
    event = to_event(online_row, SALT)
    assert event["channel"] == "online"
    assert event["merchant_state"] == "ONLINE"
    assert event["merchant_country"] == "XX"


def test_cross_border_country_resolution() -> None:
    event = to_event(_row(**{"Merchant State": "Italy", "Merchant City": "Milan"}), SALT)
    assert event["merchant_country"] == "IT"
    assert event["merchant_state"] == "Italy"


def test_unknown_state_falls_back_to_xx() -> None:
    event = to_event(_row(**{"Merchant State": "Atlantis"}), SALT)
    assert event["merchant_country"] == "XX"
    assert event["merchant_state"] == "Atlantis"


def test_tokenization_is_deterministic() -> None:
    row = _row()
    token_a = to_event(row, SALT)["card_token"]
    token_b = to_event(copy.deepcopy(row), SALT)["card_token"]
    assert token_a == token_b


def test_tokenization_is_salt_sensitive() -> None:
    row = _row()
    token_a = to_event(row, "salt-one")["card_token"]
    token_b = to_event(row, "salt-two")["card_token"]
    assert token_a != token_b


def test_tokenization_differs_by_user_and_card() -> None:
    token_user19 = to_event(_row(User="19"), SALT)["card_token"]
    token_user20 = to_event(_row(User="20"), SALT)["card_token"]
    token_card5 = to_event(_row(Card="5"), SALT)["card_token"]
    assert token_user19 != token_user20
    assert token_user19 != token_card5


def test_valid_event_passes_contract_validation() -> None:
    event = to_event(_row(), SALT)
    assert validate_event(event) is None


def test_missing_required_field_fails_validation() -> None:
    event = to_event(_row(), SALT)
    del event["mcc"]
    reason = validate_event(event)
    assert reason is not None
    assert "mcc" in reason or "required" in reason.lower()


def test_wrong_type_fails_validation() -> None:
    event = to_event(_row(), SALT)
    event["amount"] = "not-a-number"
    assert validate_event(event) is not None


def test_kafka_config_plaintext_by_default(monkeypatch) -> None:
    monkeypatch.delenv("KAFKA_SASL_PASSWORD", raising=False)
    import src.pipeline.ingestion as ingestion_module

    monkeypatch.setattr(ingestion_module, "KAFKA_SASL_PASSWORD", "")
    config = kafka_config()
    assert config["security.protocol"] == "PLAINTEXT"
    assert "sasl.mechanism" not in config


def test_kafka_config_sasl_ssl_when_password_set(monkeypatch) -> None:
    import src.pipeline.ingestion as ingestion_module

    monkeypatch.setattr(ingestion_module, "KAFKA_SASL_PASSWORD", "conn-string")
    monkeypatch.setattr(ingestion_module, "KAFKA_SASL_USERNAME", "$ConnectionString")
    config = ingestion_module.kafka_config()
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "PLAIN"
    assert config["sasl.username"] == "$ConnectionString"
    assert config["sasl.password"] == "conn-string"


def test_dry_run_replay_against_sample_csv() -> None:
    from src.pipeline.ingestion import replay

    valid, invalid = replay(
        input_path="data/sample/transactions_sample.csv",
        salt=SALT,
        eps=0,  # unlimited for the test
        max_events=200,
        dry_run=True,
    )
    assert valid + invalid == 200
    assert valid > 0


# --- Avro produce path (ADR 0006) -------------------------------------------


def test_replay_produces_confluent_framed_avro_value(monkeypatch) -> None:
    import confluent_kafka

    import src.pipeline.ingestion as ingestion_module

    fake_producer = _FakeProducer()
    monkeypatch.setattr(confluent_kafka, "Producer", lambda *_a, **_kw: fake_producer)
    monkeypatch.setattr(sr, "registry_client", lambda: _FakeRegistryClient())

    valid, invalid = ingestion_module.replay(
        input_path="data/sample/transactions_sample.csv",
        salt=SALT,
        eps=0,
        max_events=5,
        dry_run=False,
    )

    assert valid + invalid == 5
    txn_messages = [m for m in fake_producer.messages if m["topic"] == "transactions"]
    assert len(txn_messages) == valid
    assert valid > 0
    for msg in txn_messages:
        assert isinstance(msg["value"], bytes)
        assert msg["value"][0] == 0x00
        assert msg["key"] is not None  # keyed by card_token
        decoded = _decode_avro_value(msg["value"])
        assert decoded["card_token"] == msg["key"]


def test_replay_dlq_payloads_remain_json(monkeypatch, tmp_path) -> None:
    import csv

    import confluent_kafka

    import src.pipeline.ingestion as ingestion_module

    # The checked-in sample CSV has zero invalid rows; write a tiny CSV with
    # one row whose Time is unparseable, to force the mapping-error -> DLQ path.
    bad_row = _row(Time="not-a-time")
    csv_path = tmp_path / "one_bad_row.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(bad_row.keys()))
        writer.writeheader()
        writer.writerow(bad_row)

    fake_producer = _FakeProducer()
    monkeypatch.setattr(confluent_kafka, "Producer", lambda *_a, **_kw: fake_producer)
    monkeypatch.setattr(sr, "registry_client", lambda: _FakeRegistryClient())

    valid, invalid = ingestion_module.replay(
        input_path=str(csv_path), salt=SALT, eps=0, max_events=None, dry_run=False
    )

    assert valid == 0
    assert invalid == 1
    dlq_messages = [m for m in fake_producer.messages if m["topic"] == ingestion_module.KAFKA_TOPIC_DLQ]
    assert len(dlq_messages) == 1
    payload = json.loads(dlq_messages[0]["value"])
    assert "error" in payload and "raw_row" in payload
    assert "mapping error" in payload["error"]


def test_replay_avro_serialization_failure_routes_to_dlq_and_counts(monkeypatch, caplog) -> None:
    import confluent_kafka

    import src.pipeline.ingestion as ingestion_module

    fake_producer = _FakeProducer()

    def _raising_serializer(*_a, **_kw):
        raise ValueError("boom: schema mismatch")

    monkeypatch.setattr(confluent_kafka, "Producer", lambda *_a, **_kw: fake_producer)
    monkeypatch.setattr(sr, "registry_client", lambda: _FakeRegistryClient())
    monkeypatch.setattr(sr, "build_avro_serializer", lambda _client: _raising_serializer)

    with caplog.at_level("INFO", logger="src.pipeline.ingestion"):
        valid, invalid = ingestion_module.replay(
            input_path="data/sample/transactions_sample.csv",
            salt=SALT,
            eps=0,
            max_events=5,
            dry_run=False,
        )

    # Every valid row's avro-serialization fails, so nothing lands on
    # `transactions` — everything valid instead lands on the DLQ.
    txn_messages = [m for m in fake_producer.messages if m["topic"] == "transactions"]
    assert txn_messages == []
    assert valid > 0

    avro_failure_dlq = [
        json.loads(m["value"])
        for m in fake_producer.messages
        if m["topic"] == ingestion_module.KAFKA_TOPIC_DLQ
        and "avro serialization failed" in json.loads(m["value"])["error"]
    ]
    assert len(avro_failure_dlq) == valid

    assert f"avro_serialization_failures={valid}" in caplog.text
