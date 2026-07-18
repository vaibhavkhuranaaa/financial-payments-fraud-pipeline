"""Unit tests for src.bank.cdc.

Hermetic — no live Azure SQL Edge. `enable_cdc`/`scan_once`/`scan_loop` are
exercised against a mocked SQLAlchemy engine so we assert the *shape* of what
would be executed (which statements, in what order) without a real database.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemy.exc import SQLAlchemyError

from src.bank import cdc


def _mock_engine() -> tuple[MagicMock, MagicMock]:
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_engine.begin.return_value.__exit__.return_value = False
    return mock_engine, mock_conn


def test_enable_cdc_issues_db_then_table_enable_in_order() -> None:
    mock_engine, mock_conn = _mock_engine()

    cdc.enable_cdc(mock_engine)

    assert mock_conn.execute.call_count == 2
    first_stmt = mock_conn.execute.call_args_list[0].args[0]
    second_stmt = mock_conn.execute.call_args_list[1].args[0]
    assert first_stmt is cdc._ENABLE_DB_CDC_SQL
    assert second_stmt is cdc._ENABLE_TABLE_CDC_SQL


def test_enable_cdc_uses_two_separate_transactions() -> None:
    # cdc.change_tables (queried by _ENABLE_TABLE_CDC_SQL) only exists once
    # sp_cdc_enable_db has committed, so the two statements must not share a
    # single engine.begin() transaction.
    mock_engine, _ = _mock_engine()

    cdc.enable_cdc(mock_engine)

    assert mock_engine.begin.call_count == 2


def test_scan_once_returns_true_on_success() -> None:
    mock_engine, mock_conn = _mock_engine()

    assert cdc.scan_once(mock_engine) is True
    mock_conn.execute.assert_called_once_with(cdc._SCAN_SQL)


def test_scan_once_returns_false_and_swallows_error() -> None:
    mock_engine, mock_conn = _mock_engine()
    mock_conn.execute.side_effect = SQLAlchemyError("concurrent scan in progress")

    assert cdc.scan_once(mock_engine) is False


def test_scan_loop_respects_max_scans() -> None:
    mock_engine, _ = _mock_engine()

    successes = cdc.scan_loop(mock_engine, interval=0.0, max_scans=3)

    assert successes == 3
    assert mock_engine.begin.call_count == 3


def test_scan_loop_survives_a_scan_raising_and_continues() -> None:
    mock_engine, mock_conn = _mock_engine()
    # Second scan's execute() raises; loop should not crash and should still
    # attempt the third scan.
    mock_conn.execute.side_effect = [None, SQLAlchemyError("boom"), None]

    successes = cdc.scan_loop(mock_engine, interval=0.0, max_scans=3)

    assert successes == 2
    assert mock_conn.execute.call_count == 3
