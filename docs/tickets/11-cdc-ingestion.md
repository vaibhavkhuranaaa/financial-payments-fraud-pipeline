# Ticket 11 — v1.2: CDC ingestion (Debezium: bank DB → Kafka)

**Status: BACKLOG — do not start until v1.1 (tickets 06–10) is shipped and tagged.**

**Owner:** Pipeline subagent (v1.2). **Scope:** `docker/` (Debezium/connect service), `src/bank/` (txn writer), `src/pipeline/` (CDC-topic consumer path), docs. Builds directly on ticket 06's SQL Edge.

## Why (the v1.2 headline)
v1.x ingests by replaying a CSV into Kafka — fine for a demo, but real banks stream the *change feed of the system of record*. Swapping replay for CDC upgrades the story to the production-real architecture: transactions are INSERTed into SQL (OLTP), Debezium captures the log, Kafka carries the change events, and the existing Spark/Redis/scoring path consumes them unchanged. Interview arc: "v1 replayed files; v1.2 moved to CDC because that's how the upstream actually behaves."

## Design sketch
1. **New table `bank.card_transactions`** (OLTP-shaped, one row per authorization; ticket 06 schema style, CDC-enabled). A small writer (`src/bank/txn_writer.py`, compose profile `cdc`) replays the TabFormer sample INTO SQL at `PRODUCER_EVENTS_PER_SEC` — the CSV replay code's row→event mapping is reused, but the write target becomes the DB, making SQL the true system of record.
2. **Debezium**: Kafka Connect container (`debezium/connect`) + SQL Server source connector against SQL Edge (SQL Edge supports SQL Server CDC agent semantics — verify early; if Edge's CDC is unavailable on ARM, fall back to a polling-based incremental source (timestamp/rowversion column) and DOCUMENT the tradeoff in the ADR — the topic contract stays identical). Connector config as code, registered idempotently by a one-shot service.
3. **Topic mapping**: CDC topic (e.g. `bankdb.bank.card_transactions`) → a thin transformer (or SMT config) producing the existing contract-v1 `transactions` topic so **zero changes** in Spark job / scorer / API. Envelope handling (Debezium `op`/`after`) lives in one module with tests.
4. **Compose profile `cdc`** as an alternative to `replay`: `make demo CDC=1` (or `make demo-cdc`) switches ingest modes; both must keep working.
5. **Docs**: ADR 0003 (CDC vs replay, Edge CDC caveats, exactly-once-ish semantics and where dupes can occur — dedupe by event_id already exists downstream); lineage + README arch diagram gain the CDC path; README "What I'd Improve Next" updated.

## Acceptance
- `make demo-cdc`: rows INSERTed into `bank.card_transactions` appear scored in `bank.scored_transactions` within seconds, dashboard live, zero manual steps; `make demo` (replay mode) still green.
- Kill/restart the connect container mid-run: no pipeline crash, resume without data loss (offsets), duplicates absorbed downstream.
- `make check` green; connector config validated in CI (JSON lint + unit tests on the envelope transformer).
