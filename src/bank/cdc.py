"""Change Data Capture (CDC) enablement + scan pump for the bank DB.

Data flow
---------
Azure SQL Edge supports SQL Server's CDC feature set (`sys.sp_cdc_enable_db`
/ `sys.sp_cdc_enable_table`) on a user database — never on `master`, which is
why ticket 11 moved the bank DB to a dedicated `frauddemo` database (see
`src.bank.db.bootstrap_database`). Once CDC is enabled on
`bank.card_transactions`, every INSERT written by `src.bank.txn_writer` is
mirrored into a `cdc.dbo_card_transactions_CT`-style change table that
Debezium (a later chunk of ticket 11, not this module) reads directly.

Why the scan pump exists
-------------------------
On real SQL Server, a SQL Server Agent job (`cdc.*_capture`) polls the
transaction log and populates change tables automatically. SQL Server Agent
is NOT running inside the `azure-sql-edge` container, so change tables would
sit empty forever without something driving `EXEC sys.sp_cdc_scan` — the
same procedure the Agent job calls internally. `scan_loop` is that
replacement: a simple loop issuing `sp_cdc_scan` on an interval, run as the
long-lived `cdc-scan` compose service in place of the (unavailable) Agent.

CLI: `python -m src.bank.cdc --enable` (idempotent, one-shot) or
`python -m src.bank.cdc --scan [--interval SECONDS] [--max-scans N]`
(long-running pump; `--max-scans` bounds it for tests/smoke).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.bank.db import get_engine

logger = logging.getLogger(__name__)

# --- Env-driven configuration (defaults mirror .env.example) ---------------

CDC_SCAN_INTERVAL_S = 2.0

_ENABLE_DB_CDC_SQL = text(
    """
    IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0
    BEGIN
        EXEC sys.sp_cdc_enable_db;
    END
    """
)

# cdc.change_tables only exists once sp_cdc_enable_db has created the `cdc`
# schema/catalog views, so this must run strictly after _ENABLE_DB_CDC_SQL.
_ENABLE_TABLE_CDC_SQL = text(
    """
    IF NOT EXISTS (
        SELECT 1 FROM cdc.change_tables
        WHERE source_object_id = OBJECT_ID('bank.card_transactions')
    )
    BEGIN
        EXEC sys.sp_cdc_enable_table
            @source_schema = N'bank',
            @source_name = N'card_transactions',
            @role_name = NULL,
            @supports_net_changes = 0;
    END
    """
)

_SCAN_SQL = text("EXEC sys.sp_cdc_scan")


def enable_cdc(engine: Engine) -> None:
    """Idempotently enable CDC on the current database, then on
    bank.card_transactions. Safe to call any number of times."""
    with engine.begin() as conn:
        conn.execute(_ENABLE_DB_CDC_SQL)
    with engine.begin() as conn:
        conn.execute(_ENABLE_TABLE_CDC_SQL)


def scan_once(engine: Engine) -> bool:
    """Run one `sp_cdc_scan`. Returns True on success, False if the scan
    raised (e.g. a concurrent scan already in progress) — logged, not
    fatal, so the pump loop keeps running."""
    try:
        with engine.begin() as conn:
            conn.execute(_SCAN_SQL)
        return True
    except SQLAlchemyError as exc:
        logger.warning("sp_cdc_scan failed (will retry next interval): %s", exc)
        return False


def scan_loop(
    engine: Engine,
    interval: float = CDC_SCAN_INTERVAL_S,
    max_scans: int | None = None,
) -> int:
    """Run `scan_once` every `interval` seconds. Loops forever if `max_scans`
    is None, else stops after `max_scans` attempts (success or failure).
    Returns the number of successful scans."""
    successes = 0
    scans = 0
    while max_scans is None or scans < max_scans:
        if scan_once(engine):
            successes += 1
        scans += 1
        if max_scans is None or scans < max_scans:
            time.sleep(interval)
    return successes


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Enable CDC on the bank DB, or run the sp_cdc_scan pump loop."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--enable", action="store_true", help="Idempotently enable CDC. Exits after.")
    mode.add_argument("--scan", action="store_true", help="Run the sp_cdc_scan pump loop.")
    parser.add_argument(
        "--interval", type=float, default=CDC_SCAN_INTERVAL_S, help="Seconds between scans (--scan)."
    )
    parser.add_argument(
        "--max-scans", type=int, default=None, help="Stop after N scans (for tests/smoke; --scan)."
    )
    args = parser.parse_args(argv)

    engine = get_engine()

    if args.enable:
        enable_cdc(engine)
        print("CDC enabled on the bank DB and bank.card_transactions.")
        return

    successes = scan_loop(engine, interval=args.interval, max_scans=args.max_scans)
    print(f"scan pump stopped after {successes} successful sp_cdc_scan calls")


if __name__ == "__main__":
    main(sys.argv[1:])
