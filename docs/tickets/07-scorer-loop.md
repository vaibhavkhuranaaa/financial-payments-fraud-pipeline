# Ticket 07 — Live scorer loop (Kafka → /score → SQL)

**Owner:** Scorer subagent. **Scope:** `src/pipeline/scorer.py`, `tests/test_scorer.py`, compose service addition only. Do not touch other `src/pipeline/` modules, `src/app.py`, `src/bank/` (import-only), `src/dashboard/`.

## Context
- Read `CLAUDE.md`, `docs/STATE.md`, `docs/tickets/06-bank-db.md` (table shapes), `src/pipeline/ingestion.py` (kafka client + config helper — REUSE `kafka_config()`), `src/app.py` (response shape: fraud_probability, decision, cold_card, latency_ms, threshold).
- Purpose: close the demo loop — every replayed transaction gets scored and lands in SQL so the dashboard is live with zero manual steps.

## Deliverables
1. **`src/pipeline/scorer.py`**: confluent_kafka Consumer, group `scorer`, topic `KAFKA_TOPIC_TRANSACTIONS`, `auto.offset.reset=earliest`. For each event: POST to `SCORE_URL` (default `http://api:8000/score`; requests.Session, timeout ~2s, small retry with backoff on connection errors; skip+log on 4xx). Write result to `bank.scored_transactions` (idempotent on event_id PK — swallow duplicate-key on replays/rebalances) and, when `fraud_probability >= ALERT_THRESHOLD` (default: model threshold from response), insert `bank.fraud_alerts`. Batch SQL inserts (executemany every N=50 or 2s flush). Use `src/bank/db.get_engine()`.
2. Env knobs in `.env.example`: `SCORE_URL`, `ALERT_THRESHOLD` (empty = use response threshold), `SCORER_MAX_EVENTS` (bounded runs/tests). CLI `python -m src.pipeline.scorer [--max-events N]`.
3. **Compose service `scorer`** (profile `demo`): pipeline image, depends_on api + bank-db healthy + init-bank completed; env per the x-pipeline-env anchor.
4. **`tests/test_scorer.py`**: hermetic — fake Kafka messages (plain dicts through the handler function; structure scorer so `handle_event(event, session, engine)` is unit-testable without a broker), mocked requests + engine: asserts scored_transactions row shape, alert only above threshold, duplicate event_id swallowed, 5xx retried then skipped.

## Acceptance
- With the full demo stack up, `SELECT COUNT(*) FROM bank.scored_transactions` grows continuously during replay; alerts appear for high scores.
- Bounded smoke: `python -m src.pipeline.scorer --max-events 100` exits 0 against the local stack.
- `make check` green.
