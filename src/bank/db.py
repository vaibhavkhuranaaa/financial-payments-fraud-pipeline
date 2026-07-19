"""Bank database connection helper — shared by `src.bank.seed` and the ticket
07 (scorer loop) / ticket 08 (dashboard) consumers.

Data flow
---------
This module owns exactly one thing: building a SQLAlchemy engine for the
Azure SQL Edge "core banking" system-of-record (see `src/bank/schema.sql`)
via the `pymssql` DB-API driver (`mssql+pymssql://`), configured entirely
from environment variables so the same code targets the local Docker
container and a future managed SQL Server/Azure SQL instance unchanged.

Connection target: this connects to a dedicated user database (`BANK_DB_NAME`,
default `frauddemo`) with all objects living under the `bank` schema inside
it (`bank.customers`, `bank.cards`, ...). A dedicated database — rather than
the stock `master` — is required from ticket 11 onward because SQL Server/SQL
Edge Change Data Capture (`sys.sp_cdc_enable_db`) can only be enabled on a
user database, never on `master`. `bootstrap_database()` provisions
`BANK_DB_NAME` if it doesn't exist yet; `get_engine()` then connects straight
to it. Set `BANK_DB_NAME=master` to opt back into the pre-ticket-11 layout
(bootstrap becomes a no-op in that case; CDC won't be available).
"""

from __future__ import annotations

import logging
import os

from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine, text

load_dotenv()

logger = logging.getLogger(__name__)

# --- Env-driven configuration (defaults mirror .env.example) ---------------

# Ticket 14: the fallback below is the same demo-only value committed in
# docker/demo.env/.env.example, kept for local-dev ergonomics (no compose or
# .env required to just `import src.bank.db`) — but its use is logged loudly
# so it can never silently end up pointed at anything non-local.
_DEMO_BANK_DB_PASSWORD = "LocalDev!Passw0rd"

BANK_DB_HOST = os.environ.get("BANK_DB_HOST", "localhost")
BANK_DB_PORT = int(os.environ.get("BANK_DB_PORT", "1433"))
BANK_DB_USER = os.environ.get("BANK_DB_USER", "sa")
BANK_DB_PASSWORD = os.environ.get("BANK_DB_PASSWORD", _DEMO_BANK_DB_PASSWORD)
# Dedicated user database (not `master`) so CDC (sp_cdc_enable_db) can be
# enabled on it — see `bootstrap_database()`.
BANK_DB_NAME = os.environ.get("BANK_DB_NAME", "frauddemo")

if BANK_DB_PASSWORD == _DEMO_BANK_DB_PASSWORD:
    logger.warning("demo credential in use — not for production (BANK_DB_PASSWORD)")

def get_engine() -> Engine:
    """Build (but do not connect) a SQLAlchemy engine for the bank DB.

    Uses the `pymssql` DB-API driver: `mssql+pymssql://user:pass@host:port/db`.
    """
    url = (
        f"mssql+pymssql://{BANK_DB_USER}:{BANK_DB_PASSWORD}"
        f"@{BANK_DB_HOST}:{BANK_DB_PORT}/{BANK_DB_NAME}"
    )
    return create_engine(url, pool_pre_ping=True, future=True)


def bootstrap_database() -> None:
    """Create BANK_DB_NAME if it doesn't exist yet. No-op when BANK_DB_NAME
    is `master` (nothing to create).

    Connects to `master` (the one database guaranteed to exist) with
    `isolation_level="AUTOCOMMIT"` because `CREATE DATABASE` cannot run
    inside a transaction. Must run before `get_engine()`/`apply_schema()`
    target a not-yet-provisioned database.
    """
    if BANK_DB_NAME == "master":
        return
    url = (
        f"mssql+pymssql://{BANK_DB_USER}:{BANK_DB_PASSWORD}"
        f"@{BANK_DB_HOST}:{BANK_DB_PORT}/master"
    )
    engine = create_engine(url, pool_pre_ping=True, future=True, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            # An unclean bank-db shutdown (e.g. teardown SIGKILLing SQL Edge
            # mid-write) can leave the database SUSPECT/RECOVERY_PENDING, where
            # every login "fails to open the explicitly specified database" and
            # the whole demo wedges. This is disposable, deterministically
            # re-seeded demo data — drop the carcass and start clean rather
            # than attempting repair. Found live 2026-07-19.
            state = conn.execute(
                text("SELECT state_desc FROM sys.databases WHERE name = :name"),
                {"name": BANK_DB_NAME},
            ).scalar()
            if state is not None and state != "ONLINE":
                # Plain DROP (no SET OFFLINE first): a SUSPECT/RECOVERY_PENDING
                # database has no sessions to roll back, and ALTER ... SET
                # OFFLINE can itself fail on a database that can't be opened.
                conn.execute(text(f"DROP DATABASE [{BANK_DB_NAME}]"))
                state = None
            if state is None:
                conn.execute(text(f"CREATE DATABASE [{BANK_DB_NAME}]"))
    finally:
        engine.dispose()


def run_script(engine: Engine, sql_text: str) -> None:
    """Execute a `;`-free, `GO`-batched SQL script (e.g. `schema.sql`).

    SQL Server's `GO` batch separator is a sqlcmd/SSMS-only convention, not
    real T-SQL, so the DB-API driver can't send a whole multi-batch script in
    one call. We split on bare `GO` lines ourselves and execute each batch in
    its own statement, mirroring what sqlcmd does under the hood.
    """
    batches: list[str] = []
    current: list[str] = []
    for line in sql_text.splitlines():
        if line.strip() == "GO":
            batches.append("\n".join(current))
            current = []
        else:
            current.append(line)
    if current:
        batches.append("\n".join(current))

    with engine.begin() as conn:
        for batch in batches:
            if batch.strip():
                conn.execute(text(batch))
