"""Unit tests for src.bank.seed / src.bank.db.

Hermetic — no live database. `derive_seed_data` is pure (CSV in, dataclass
out); the write path (`write_dims`, `apply_schema`) is exercised against a
mocked SQLAlchemy engine so we assert the *shape* of what would be executed
without needing Azure SQL Edge running.
"""

from __future__ import annotations

import os
import re
from unittest.mock import MagicMock, patch

from src.bank.db import run_script
from src.bank.seed import (
    SEED_INPUT_CSV,
    build_cards,
    derive_seed_data,
    fingerprint,
    write_dims,
)
from src.pipeline.ingestion import _card_token, to_event

SALT = "test-salt"
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SAMPLE_CSV = os.path.join(_REPO_ROOT, SEED_INPUT_CSV)
_SCHEMA_PATH = os.path.join(_REPO_ROOT, "src", "bank", "schema.sql")


def test_seed_is_deterministic_same_input_same_fingerprint() -> None:
    data_a = derive_seed_data(_SAMPLE_CSV, SALT)
    data_b = derive_seed_data(_SAMPLE_CSV, SALT)
    assert fingerprint(data_a) == fingerprint(data_b)
    assert data_a.customers == data_b.customers
    assert data_a.accounts == data_b.accounts
    assert data_a.cards == data_b.cards


def test_seed_derives_at_least_one_of_each_dimension() -> None:
    data = derive_seed_data(_SAMPLE_CSV, SALT)
    assert len(data.customers) > 0
    assert len(data.accounts) == len(data.customers)
    assert len(data.cards) > 0
    # every account belongs to a real customer
    customer_ids = {c["customer_id"] for c in data.customers}
    for account in data.accounts:
        assert account["customer_id"] in customer_ids
    # every card belongs to a real account
    account_ids = {a["account_id"] for a in data.accounts}
    for card in data.cards:
        assert card["account_id"] in account_ids
        assert len(card["card_token"]) == 64
        assert re.fullmatch(r"[a-f0-9]{64}", card["card_token"])


def test_card_token_matches_ingestion_tokenization_exactly() -> None:
    """The whole point of importing _card_token: seed.py must never drift
    from the tokenization the streaming producer uses."""
    user, card = "19", "4"
    cards = build_cards([(user, card)], SALT)
    assert cards[0]["card_token"] == _card_token(user, card, SALT)


def test_card_token_parity_with_to_event_for_a_sample_row() -> None:
    row = {
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
    event = to_event(row, SALT)
    cards = build_cards([("19", "4")], SALT)
    assert cards[0]["card_token"] == event["card_token"]


def test_seed_sensitive_to_salt() -> None:
    data_default = derive_seed_data(_SAMPLE_CSV, "salt-one")
    data_other = derive_seed_data(_SAMPLE_CSV, "salt-two")
    tokens_a = {c["card_token"] for c in data_default.cards}
    tokens_b = {c["card_token"] for c in data_other.cards}
    assert tokens_a.isdisjoint(tokens_b)


def test_write_dims_deletes_then_inserts_in_fk_order() -> None:
    data = derive_seed_data(_SAMPLE_CSV, SALT)
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_engine.begin.return_value.__exit__.return_value = False

    write_dims(mock_engine, data)

    executed_sql = [call.args[0].text for call in mock_conn.execute.call_args_list]
    delete_statements = [s for s in executed_sql if s.strip().upper().startswith("DELETE")]
    insert_statements = [s for s in executed_sql if s.strip().upper().startswith("INSERT")]

    assert any("bank.cards" in s for s in delete_statements)
    assert any("bank.accounts" in s for s in delete_statements)
    assert any("bank.customers" in s for s in delete_statements)
    # deletes happen child-first (cards -> accounts -> customers)
    delete_order = [s for s in executed_sql if s.strip().upper().startswith("DELETE")]
    assert "bank.cards" in delete_order[0]
    assert "bank.accounts" in delete_order[1]
    assert "bank.customers" in delete_order[2]

    assert any("bank.customers" in s for s in insert_statements)
    assert any("bank.accounts" in s for s in insert_statements)
    assert any("bank.cards" in s for s in insert_statements)


def test_run_script_splits_on_go_and_executes_each_batch() -> None:
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_engine.begin.return_value.__exit__.return_value = False

    script = "SELECT 1\nGO\nSELECT 2\nGO\n"
    run_script(mock_engine, script)

    assert mock_conn.execute.call_count == 2


@patch("src.bank.seed.get_engine")
def test_seed_cli_entrypoint_applies_schema_and_writes_dims(mock_get_engine: MagicMock) -> None:
    from src.bank import seed as seed_module

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_engine.begin.return_value.__exit__.return_value = False
    mock_get_engine.return_value = mock_engine

    data = seed_module.seed(csv_path=_SAMPLE_CSV, salt=SALT)

    assert len(data.customers) > 0
    # apply_schema + write_dims both ran a transaction against the mock engine
    assert mock_engine.begin.call_count >= 2


def test_schema_sql_guards_every_table_with_if_not_exists() -> None:
    with open(_SCHEMA_PATH, encoding="utf-8") as f:
        sql_text = f.read()

    assert "CREATE SCHEMA bank" in sql_text
    for table in ("customers", "accounts", "cards", "scored_transactions", "fraud_alerts"):
        pattern = rf"IF NOT EXISTS[\s\S]{{0,400}}CREATE TABLE bank\.{table}"
        assert re.search(pattern, sql_text), f"bank.{table} is not idempotently guarded"

    # every CREATE TABLE / CREATE INDEX statement in the file is preceded by
    # an IF NOT EXISTS guard within a reasonable lookback window
    for match in re.finditer(r"CREATE (TABLE|INDEX)\s+([\w.]+)", sql_text):
        start = max(0, match.start() - 400)
        preceding = sql_text[start : match.start()]
        assert "IF NOT EXISTS" in preceding, f"{match.group(0)} lacks an IF NOT EXISTS guard"

    # batches are GO-separated (sqlcmd convention consumed by db.run_script)
    assert sql_text.count("\nGO\n") >= 5
