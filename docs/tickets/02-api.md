# Ticket 02 — Scoring API: Flask /score + latency benchmark

**Owner:** API subagent. **Scope:** `src/app.py`, `scripts/benchmark.py`, `tests/test_api.py`. Do not touch `src/pipeline/`, `docker/`, `infra/`, README, STATE.md.

## Context
Read first: `CLAUDE.md`, `docs/adr/0001-stack-and-architecture.md`, `contracts/transaction.schema.json`, `src/pipeline/features.py` (FEATURE_COLUMNS + enrich()), `models/` artifacts produced by ticket 01 (`model.json`, `threshold.json`, `feature_columns.json`, `metrics.json`).

Serving contract: the streaming job maintains Redis hash `features:{card_token}` with the latest per-card window features. The API enriches the incoming event (same `enrich()` from `src/pipeline/features.py` — import it, never reimplement) and joins the Redis features to build the model vector.

## Deliverables

### 1. `src/app.py`
- Flask app factory `create_app()`; gunicorn-compatible (`app = create_app()`).
- Model loaded ONCE at startup (xgboost Booster from `MODEL_DIR`), threshold from `SCORE_THRESHOLD_PATH`, feature order from `models/feature_columns.json`.
- `POST /score`: body = a contract-v1 transaction event (without `is_fraud`). Validate against the JSON Schema (reject 400 with reason). Enrich + fetch `features:{card_token}` from Redis; **cold-card fallback**: missing hash → zeros for window features plus `"cold_card": true` in the response. Respond `{"fraud_probability": float, "decision": "approve"|"review", "threshold": float, "cold_card": bool, "latency_ms": float}` where latency_ms is measured server-side around the scoring path.
- `GET /healthz`: 200 `{"status":"ok","model_loaded":true}` (503 if model missing).
- `GET /metrics`: Prometheus format via prometheus_client — request counter by endpoint/status, scoring latency histogram.
- Config via env per `.env.example`. Redis client with short socket timeout (50ms) and graceful degradation (Redis down → cold-card path + a counter increment, not a 500).

### 2. `scripts/benchmark.py`
- Fires N requests (default 2000, `--n`) at `--url` (default http://localhost:8000/score) using a thread pool (`--concurrency`, default 8), events sampled from `data/sample/transactions_sample.csv` via `src/pipeline/ingestion.to_event`.
- Reports wall-clock p50/p95/p99 latency (ms), mean, throughput req/s, error count — printed as a markdown table (for pasting into README) and written to `benchmarks/latest.json`.

### 3. `tests/test_api.py`
- Use Flask test client + fakeredis-style stubbing (monkeypatch the redis client; do NOT require a live Redis).
- Cases: valid event scores 200 with probability in [0,1]; invalid event (missing field) → 400; cold card → cold_card true; healthz 200; metrics exposes histogram.
- If `models/model.json` is absent, tests must train nothing — build a tiny Booster fixture in-test (train xgboost on 100 random rows with the real FEATURE_COLUMNS) so tests are hermetic.

## Acceptance criteria
- `.venv/bin/ruff check src tests scripts` clean; `.venv/bin/pytest tests -q` green.
- With Redis absent, `POST /score` still returns 200 cold-card responses.
- Server-side scoring path (excluding network) must be < 50ms p99 locally — if not, profile and fix before reporting.

## Verification commands
```bash
.venv/bin/ruff check src tests scripts
.venv/bin/pytest tests -q
MODEL_DIR=models .venv/bin/gunicorn -w 2 -b 127.0.0.1:8000 'src.app:app' --daemon && sleep 2
curl -s -X POST localhost:8000/score -H 'content-type: application/json' -d @- <<'EOF'
<build one valid event via python -c using to_event on the sample's first row>
EOF
.venv/bin/python scripts/benchmark.py --n 500 --concurrency 4
pkill -f gunicorn
```

## Report back
Status (complete/complete-with-caveats/blocked/failed); files changed; verbatim verification tails; the benchmark table; anything skipped.
