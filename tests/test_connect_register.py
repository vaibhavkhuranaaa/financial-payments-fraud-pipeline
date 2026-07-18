"""Unit tests for src.pipeline.connect_register.

Hermetic — no live Kafka Connect. `substitute_env` is pure; `register`'s
HTTP calls are exercised against a mocked `requests`-shaped session. A
separate set of tests lints the checked-in
`docker/connect/bankdb-source.json` template itself (valid JSON, correct
connector class/settings, no literal secret baked in).
"""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock

import pytest

from src.pipeline.connect_register import _DEFAULT_CONFIG_PATH, register, substitute_env

TEMPLATE = {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "database.password": "${BANK_DB_PASSWORD}",
    "database.encrypt": "false",
    "snapshot.mode": "initial",
}


def test_substitute_env_replaces_placeholder() -> None:
    resolved = substitute_env(TEMPLATE, {"BANK_DB_PASSWORD": "s3cr3t"})

    assert resolved["database.password"] == "s3cr3t"
    # Non-placeholder values pass through unchanged.
    assert resolved["database.encrypt"] == "false"
    assert resolved["snapshot.mode"] == "initial"


def test_substitute_env_raises_on_missing_var() -> None:
    with pytest.raises(KeyError):
        substitute_env(TEMPLATE, {})


def test_substitute_env_does_not_mutate_input() -> None:
    original = dict(TEMPLATE)
    substitute_env(TEMPLATE, {"BANK_DB_PASSWORD": "s3cr3t"})

    assert TEMPLATE == original


def _mock_response(status_code: int, json_body: dict | None = None, text: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body or {}
    resp.text = text
    return resp


def test_register_happy_path_puts_config_and_reports_status() -> None:
    session = MagicMock()
    # First GET (readiness wait) succeeds immediately.
    session.get.side_effect = [
        _mock_response(200),
        _mock_response(200, {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}),
    ]
    session.put.return_value = _mock_response(200)

    register({"connector.class": "io.debezium.connector.sqlserver.SqlServerConnector"}, "http://connect:8083", session)

    session.put.assert_called_once()
    put_args, put_kwargs = session.put.call_args
    assert put_args[0] == "http://connect:8083/connectors/bankdb-source/config"
    assert put_kwargs["json"] == {"connector.class": "io.debezium.connector.sqlserver.SqlServerConnector"}


def test_register_raises_on_put_failure() -> None:
    session = MagicMock()
    session.get.return_value = _mock_response(200)
    session.put.return_value = _mock_response(500, text="internal error")

    with pytest.raises(RuntimeError):
        register({"connector.class": "x"}, "http://connect:8083", session)


# --- Checked-in docker/connect/bankdb-source.json template ------------------


def _load_template() -> dict:
    with open(_DEFAULT_CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def test_bankdb_source_json_parses_and_has_required_settings() -> None:
    config = _load_template()

    assert config["connector.class"] == "io.debezium.connector.sqlserver.SqlServerConnector"
    assert config["decimal.handling.mode"] == "double"
    assert config["time.precision.mode"] == "connect"
    assert config["database.encrypt"] == "false"
    assert config["table.include.list"] == "bank.card_transactions"
    assert config["topic.prefix"] == "bankdb"
    # All values must be strings (Kafka Connect config requirement).
    assert all(isinstance(v, str) for v in config.values())


def test_bankdb_source_json_has_no_literal_password() -> None:
    raw = open(_DEFAULT_CONFIG_PATH, encoding="utf-8").read()

    assert "${BANK_DB_PASSWORD}" in raw
    assert os.environ.get("BANK_DB_PASSWORD", "LocalDev!Passw0rd") not in raw
    assert "LocalDev!Passw0rd" not in raw
