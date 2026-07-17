"""Unit tests for src.pipeline.ingestion: TabFormer row -> contract-v1 event.

No Kafka broker required — these exercise `to_event`/`validate_event`/
`kafka_config` directly, plus the CLI's --dry-run path against the real
sample CSV.
"""

from __future__ import annotations

import copy

from src.pipeline.ingestion import kafka_config, to_event, validate_event

SALT = "test-salt"


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
