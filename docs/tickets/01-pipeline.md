# Ticket 01 — Pipeline: ingestion, streaming features, training

**Owner:** Pipeline subagent. **Scope:** `src/pipeline/` + `databricks.yml` + `tests/test_features.py`, `tests/test_ingestion.py`. Do not touch `src/app.py`, `docker/`, `infra/`, README.

## Context
- Read `CLAUDE.md`, `docs/adr/0001-stack-and-architecture.md`, `contracts/transaction.schema.json`, `docs/STATE.md` first.
- Env: run inside `.venv` (Python 3.11, deps preinstalled). Config comes from env vars per `.env.example` (use `python-dotenv`, every var has a default matching `.env.example`).
- Source data: `data/sample/transactions_sample.csv` (TabFormer schema: `User,Card,Year,Month,Day,Time,Amount,Use Chip,Merchant Name,Merchant City,Merchant State,Zip,MCC,Errors?,Is Fraud?`; `Amount` like `$134.09`, `Is Fraud?` is `Yes/No`). Full data may exist at `data/raw/card_transaction.v1.csv` — code must accept either via `PRODUCER_INPUT_CSV` / a `--input` flag.

## Deliverables

### 1. `src/pipeline/ingestion.py` — replay producer
- Function `to_event(row: dict, salt: str) -> dict`: map a TabFormer CSV row to a contract-v1 event (see schema): build UTC ISO `event_time` from Year/Month/Day/Time; parse `Amount` to float; `channel` from `Use Chip` (`Chip Transaction`→`chip`, `Swipe Transaction`→`swipe`, `Online Transaction`→`online`); `card_token = sha256(salt + ":" + User + ":" + Card)` hexdigest; `merchant_country`: `US` if `Merchant State` is a 2-letter US state code, `XX` if missing/`ONLINE`, else map foreign state names best-effort (a small dict for the common TabFormer countries, fallback `XX`); `currency` = `USD`; keep `is_fraud` bool; `event_id` = uuid4.
- Validate every event against `contracts/transaction.schema.json` (jsonschema, compiled validator once). Valid → produce to `KAFKA_TOPIC_TRANSACTIONS` keyed by `card_token`; invalid → produce original row + error reason to `KAFKA_TOPIC_DLQ`.
- confluent_kafka Producer; config helper `kafka_config()` reading env (PLAINTEXT locally; SASL_SSL/PLAIN when `KAFKA_SASL_PASSWORD` set — works unchanged against Event Hubs).
- Rate control: `PRODUCER_EVENTS_PER_SEC` (token-bucket or sleep-based), `--max-events` flag for tests/smoke.
- CLI: `python -m src.pipeline.ingestion --input data/sample/transactions_sample.csv --eps 200 --max-events 1000`.

### 2. `src/pipeline/features.py` — shared feature definitions + streaming job
- **Shared, import-safe feature spec** used by BOTH streaming and offline training (this is the skew-prevention contract):
  - `CARD_WINDOWS = {"1m": 60, "10m": 600, "1h": 3600}` — per `card_token`: txn count, amount sum, amount mean, distinct merchant_city count, decline-ish rate (share of events with non-null `errors`).
  - Event-level enrichments (pure function `enrich(event) -> dict`): `is_cnp` (channel==online), `is_cross_border` (merchant_country not in ("US","XX")), `mcc_group` (small MCC→group dict: travel, grocery, cash, online_retail, other), `amount_log`, hour-of-day, day-of-week.
  - `FEATURE_COLUMNS`: the ordered list the model trains/serves on.
- **Streaming job** `run_stream()`: Spark Structured Streaming from Kafka (`spark-sql-kafka-0-10` package, delta enabled) → parse JSON with explicit schema → re-validate required fields (violations → DLQ topic via foreachBatch producer or a `_quarantine` Delta table — choose one, document in module docstring) → watermark 10 min on `event_time` → sliding window aggregates per `CARD_WINDOWS` → `foreachBatch`: upsert latest per-card feature row into Redis hash `features:{card_token}` (fields = feature names, plus `updated_at`) AND append raw enriched events to Delta `data/delta/events` and window features to `data/delta/card_features`.
- CLI: `python -m src.pipeline.features --run-stream` (plus `--once` trigger availableNow for bounded runs/tests).

### 3. `src/pipeline/train.py` — offline build + XGBoost
- Batch mode: read events CSV (sample or full) OR Delta events table; apply the SAME `enrich()` + point-in-time window features computed per event over each card's history (pandas/Spark groupby with time-based rolling — must be leakage-safe: features for event t use only events < t).
- Time-based split: train on earliest 80% of the time range, test on latest 20% (no shuffling).
- XGBoost with `scale_pos_weight = neg/pos`, early stopping on PR-AUC (aucpr).
- Threshold selection: maximize F1 on the PR curve of the *validation* slice; write `models/model.json` (xgboost native), `models/threshold.json` ({threshold, chosen_by}), `models/feature_columns.json`, `models/metrics.json` (pr_auc, roc_auc, precision/recall/f1 at threshold, confusion counts, train/test row counts, fraud rates, train timestamp, input path).
- CLI: `python -m src.pipeline.train --input data/sample/transactions_sample.csv`.

### 4. `databricks.yml` — Asset Bundle
- Bundle with two jobs (feature_build, train) pointing at the same entrypoints as spark_python_task; targets: `dev` (workspace placeholder). It must be valid YAML per Databricks Asset Bundle schema; it is documentation-grade (not deployed in v1).

### 5. Tests (pytest, no Kafka/Redis/Spark required)
- `to_event` mapping incl. amount parsing, channel mapping, tokenization determinism + salt sensitivity, contract validation pass/fail (missing field → invalid).
- `enrich()` flags (cross-border, cnp, mcc_group), leakage safety of offline window features (construct 3-event toy history, assert feature at t excludes t itself).
- Mark anything needing Spark with `@pytest.mark.spark` and skip if `pyspark` session can't start (CI runs them; keep them < 60s using tiny data).

## Acceptance criteria
- `.venv/bin/ruff check src tests` clean; `.venv/bin/pytest tests -q` green.
- `python -m src.pipeline.train --input data/sample/transactions_sample.csv` completes on the sample, writes all four `models/*` files, PR-AUC printed.
- Producer smoke (no broker): `--dry-run` flag prints N validated events instead of producing.
- Module docstrings explain data flow; no secrets/hardcoded paths; env-driven config only.

## Verification commands
```bash
.venv/bin/ruff check src tests
.venv/bin/pytest tests -q
.venv/bin/python -m src.pipeline.ingestion --input data/sample/transactions_sample.csv --max-events 500 --dry-run
.venv/bin/python -m src.pipeline.train --input data/sample/transactions_sample.csv
```

## Report back (four-status contract)
State: files changed; verification command outputs (verbatim tails); metrics from models/metrics.json; anything skipped or uncertain.
