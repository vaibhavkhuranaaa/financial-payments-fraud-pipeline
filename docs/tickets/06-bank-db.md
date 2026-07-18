# Ticket 06 — Core banking system-of-record (Azure SQL Edge)

**Owner:** Bank-DB subagent. **Scope:** `docker/docker-compose.yml` (add services only), `src/bank/`, `tests/test_bank_seed.py`, `requirements`/lockfile additions. Do not touch `src/pipeline/`, `src/app.py`, `src/dashboard/`, README.

## Context
- Read `CLAUDE.md`, `docs/STATE.md`, `docs/tickets/01-pipeline.md` (for the TabFormer CSV shape + tokenization), `contracts/transaction.schema.json` first.
- v1.1 plan: this DB is the "core banking" store the fraud pipeline hangs off. Downstream consumers: ticket 07 (scorer writes `scored_transactions`/`fraud_alerts`), ticket 08 (dashboard reads everything + writes alert status).
- Env: `.venv` Python 3.11. All config env-driven with defaults mirroring `.env.example` (extend `.env.example`).
- Hard constraints: synthetic data only (Faker, seed=42 — NO real PII); do not commit large data; `make check` must stay green.

## Deliverables
1. **Compose service `bank-db`**: image `mcr.microsoft.com/azure-sql-edge` (ARM-native), `ACCEPT_EULA=1`, `MSSQL_SA_PASSWORD` from env (default `LocalDev!Passw0rd` in `.env.example`), port 1433, named volume, healthcheck (`sqlcmd` isn't in the Edge image — use a TCP/`python -c` probe or `/opt/mssql-tools` alternative; document choice).
2. **`src/bank/schema.sql`**: schema `bank` with:
   - `customers` (customer_id PK, name, email, created_at, risk_tier)
   - `accounts` (account_id PK, customer_id FK, opened_at, credit_limit, status)
   - `cards` (card_token CHAR(64) PK — MUST equal the pipeline's salted-SHA256 token, account_id FK, card_type, issued_at)
   - `scored_transactions` (event_id PK, card_token, event_time, amount, merchant_name/city/state, mcc, channel, fraud_probability, decision, cold_card, latency_ms, scored_at) — insert-heavy audit table, index on (scored_at), (card_token)
   - `fraud_alerts` (alert_id IDENTITY PK, event_id, card_token, fraud_probability, amount, merchant_name, created_at, status DEFAULT 'open' CHECK IN ('open','confirmed_fraud','dismissed'), reviewed_at NULL) — index on (status, created_at)
   - Idempotent: `IF NOT EXISTS` guards throughout; safe to re-run.
3. **`src/bank/seed.py`**: derive dims deterministically from `data/sample/transactions_sample.csv`: unique (User, Card) → one card row with `card_token = sha256(salt + ":" + User + ":" + Card)` reusing the EXACT tokenization from `src/pipeline/ingestion.py` (import it — do not reimplement); one customer per User (Faker seeded with 42 for name/email); one account per customer (credit_limit deterministic from user id). Applies `schema.sql` first, then upserts (MERGE or delete+insert). CLI: `python -m src.bank.seed`. Idempotent.
4. **DB helper `src/bank/db.py`**: SQLAlchemy engine factory via `pymssql` (`mssql+pymssql://`), env vars `BANK_DB_HOST/PORT/USER/PASSWORD/NAME` (db `master`, schema `bank` — or create db `bank`; document). Shared by tickets 07/08 — keep the API tiny: `get_engine()`.
5. **Compose one-shot service `init-bank`** (profile `demo`): runs `python -m src.bank.seed` after `bank-db` healthy; pipeline image can be reused (context `..`, `docker/Dockerfile.pipeline`) if deps added there.
6. **`tests/test_bank_seed.py`**: hermetic (no live DB): seed determinism (same input ⇒ same customers/cards fingerprint), card_token parity with `ingestion.to_event` for a sample row, schema.sql parses (string-level sanity: every table guarded idempotent). Mock the engine for write-path tests.
7. Add `pymssql`, `sqlalchemy`, `faker` to the pinned deps (respect existing pinning style).

## Acceptance
- `docker compose -f docker/docker-compose.yml --profile demo up bank-db init-bank` exits 0 and a follow-up `seed.py` run is a no-op.
- `SELECT COUNT(*) FROM bank.cards` > 0 and every card_token appears in the sample stream's tokens.
- `make check` green; tests added to the suite.
