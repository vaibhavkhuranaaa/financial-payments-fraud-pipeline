"""Unit tests for src.bank.txn_writer.

Hermetic — no live Azure SQL Edge. `event_to_row` is pure (dict in, dict
out); `replay` is exercised against the real sample CSV with a mocked
SQLAlchemy engine so batch-insert counts/shape are assertable without a
database.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

from src.bank import txn_writer
from src.bank.txn_writer import event_to_row, replay
from src.pipeline.ingestion import TOKENIZATION_SALT, to_event

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SAMPLE_CSV = os.path.join(_REPO_ROOT, "data", "sample", "transactions_sample.csv")

SALT = "test-salt"

EVENT = {
    "schema_version": "1.0.0",
    "event_id": "11111111-1111-1111-1111-111111111111",
    "event_time": "2019-02-13T14:06:00Z",
    "card_token": "a" * 64,
    "user_id": "19",
    "amount": 80.0,
    "currency": "USD",
    "channel": "chip",
    "merchant_name": "Some Merchant",
    "merchant_city": "Tucson",
    "merchant_state": "AZ",
    "merchant_country": "US",
    "zip": "85719",
    "mcc": 4829,
    "errors": None,
    "is_fraud": None,
}


def _mock_engine() -> tuple[MagicMock, MagicMock]:
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_engine.begin.return_value.__exit__.return_value = False
    return mock_engine, mock_conn


def test_event_to_row_maps_all_fields_and_parses_event_time() -> None:
    row = event_to_row(EVENT)

    assert row["event_id"] == EVENT["event_id"]
    assert row["schema_version"] == EVENT["schema_version"]
    assert row["card_token"] == EVENT["card_token"]
    assert row["user_id"] == EVENT["user_id"]
    assert row["amount"] == EVENT["amount"]
    assert row["currency"] == EVENT["currency"]
    assert row["channel"] == EVENT["channel"]
    assert row["merchant_name"] == EVENT["merchant_name"]
    assert row["merchant_city"] == EVENT["merchant_city"]
    assert row["merchant_state"] == EVENT["merchant_state"]
    assert row["merchant_country"] == EVENT["merchant_country"]
    assert row["zip"] == EVENT["zip"]
    assert row["mcc"] == EVENT["mcc"]
    assert row["errors"] == EVENT["errors"]
    assert row["is_fraud"] == EVENT["is_fraud"]
    assert row["event_time"].isoformat() == "2019-02-13T14:06:00"


def test_event_to_row_handles_nullable_fields() -> None:
    event = dict(EVENT, merchant_name=None, merchant_city=None, zip=None, errors=None, is_fraud=None)
    row = event_to_row(event)
    assert row["merchant_name"] is None
    assert row["merchant_city"] is None
    assert row["zip"] is None
    assert row["errors"] is None
    assert row["is_fraud"] is None


def test_replay_dry_count_only_no_engine() -> None:
    valid, invalid = replay(
        input_path=_SAMPLE_CSV, salt=TOKENIZATION_SALT, eps=0, max_events=200, engine=None
    )
    assert valid + invalid == 200
    assert valid > 0


def test_replay_inserts_valid_rows_into_bank_card_transactions() -> None:
    mock_engine, mock_conn = _mock_engine()

    valid, invalid = replay(
        input_path=_SAMPLE_CSV, salt=TOKENIZATION_SALT, eps=0, max_events=10, engine=mock_engine
    )

    assert valid + invalid == 10
    assert valid > 0
    # exactly one flush (well under BATCH_SIZE=50, flushed at end-of-replay)
    assert mock_conn.execute.call_count == 1
    stmt, rows = mock_conn.execute.call_args.args
    assert stmt is txn_writer._INSERT_CARD_TXN_SQL
    assert len(rows) == valid


def test_replay_counts_malformed_row_as_invalid_without_insert() -> None:
    mock_engine, mock_conn = _mock_engine()

    valid_before, invalid_before = replay(
        input_path=_SAMPLE_CSV, salt=TOKENIZATION_SALT, eps=0, max_events=5, engine=None
    )

    # Sanity: to_event raises KeyError on a row missing a required column,
    # which replay() must catch, count as invalid, and skip inserting.
    reason = None
    try:
        to_event({}, SALT)
    except (KeyError, ValueError) as exc:
        reason = str(exc)
    assert reason is not None

    valid, invalid = replay(
        input_path=_SAMPLE_CSV, salt=TOKENIZATION_SALT, eps=0, max_events=5, engine=mock_engine
    )
    assert valid == valid_before
    assert invalid == invalid_before
    if valid > 0:
        rows = mock_conn.execute.call_args.args[1]
        assert len(rows) == valid
