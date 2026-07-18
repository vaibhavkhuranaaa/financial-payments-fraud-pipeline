"""Bank database connection helper — shared by `src.bank.seed` and the ticket
07 (scorer loop) / ticket 08 (dashboard) consumers.

Data flow
---------
This module owns exactly one thing: building a SQLAlchemy engine for the
Azure SQL Edge "core banking" system-of-record (see `src/bank/schema.sql`)
via the `pymssql` DB-API driver (`mssql+pymssql://`), configured entirely
from environment variables so the same code targets the local Docker
container and a future managed SQL Server/Azure SQL instance unchanged.

Connection target: by default this connects to the `master` database and
all objects live under the `bank` schema (`bank.customers`, `bank.cards`,
...) so no `CREATE DATABASE` step is required against the stock
`azure-sql-edge` image. Set `BANK_DB_NAME` to point at a dedicated database
instead (e.g. once one has been provisioned) — `get_engine()` honors
whatever value is configured, defaulting to `master`.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine, text

load_dotenv()

# --- Env-driven configuration (defaults mirror .env.example) ---------------

BANK_DB_HOST = os.environ.get("BANK_DB_HOST", "localhost")
BANK_DB_PORT = int(os.environ.get("BANK_DB_PORT", "1433"))
BANK_DB_USER = os.environ.get("BANK_DB_USER", "sa")
BANK_DB_PASSWORD = os.environ.get("BANK_DB_PASSWORD", "LocalDev!Passw0rd")
# Azure SQL Edge has no `CREATE DATABASE bank` step out of the box; objects
# live under the `bank` SCHEMA inside the default `master` database unless a
# dedicated database has been provisioned and BANK_DB_NAME overridden.
BANK_DB_NAME = os.environ.get("BANK_DB_NAME", "master")

def get_engine() -> Engine:
    """Build (but do not connect) a SQLAlchemy engine for the bank DB.

    Uses the `pymssql` DB-API driver: `mssql+pymssql://user:pass@host:port/db`.
    """
    url = (
        f"mssql+pymssql://{BANK_DB_USER}:{BANK_DB_PASSWORD}"
        f"@{BANK_DB_HOST}:{BANK_DB_PORT}/{BANK_DB_NAME}"
    )
    return create_engine(url, pool_pre_ping=True, future=True)


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
