# Ticket 08 — Fraud-ops dashboard (Plotly Dash)

**Owner:** Dashboard subagent. **Scope:** `src/dashboard/`, `docker/Dockerfile.dashboard`, compose service addition, `tests/test_dashboard.py`. Do not touch `src/pipeline/`, `src/app.py`, `src/bank/` (import `db.get_engine` only).

## Context
- Read `CLAUDE.md`, `docs/STATE.md`, `docs/tickets/06-bank-db.md` (tables), `src/app.py` (Prometheus `/metrics` exposes `score_latency_seconds` histogram + `http_requests_total`), `models/metrics.json` (model card numbers).
- **Before writing ANY chart code, load the `dataviz` skill** and follow it (palette, dark/light, accessibility). The bar is "beautiful, functional, zero manual intervention".
- Audience: a recruiter watching a 3-minute screen share. Clarity > density.

## Deliverables
1. **Dash app `src/dashboard/app.py`** (Plotly Dash ≥2.17, pure Python, no CDN — assets bundled), port 8050, `dcc.Interval` refresh ~2s, graceful when tables are empty or services are down (never a stack trace on screen; show waiting states).
2. Panels:
   - **Header stat tiles**: transactions scored (total + last-60s rate), open alerts, p50/p95/p99 scoring latency (parse the Prometheus text from `API_METRICS_URL`, default `http://api:8000/metrics` — histogram-quantile estimate from buckets), model PR-AUC/ROC-AUC (metrics.json, read once).
   - **Live feed**: last ~20 scored transactions (time, masked card `…{last6 of token}`, merchant, amount, score, decision) from `bank.scored_transactions`.
   - **Alerts queue**: open `bank.fraud_alerts` newest-first joined to `bank.cards→accounts→customers` (customer name, risk_tier, credit_limit); per-row buttons **Confirm fraud** / **Dismiss** updating `status`+`reviewed_at` via SQL (the "proper functionality" ask).
   - **Charts**: score distribution (log-y histogram), throughput over time (1-min buckets), fraud-alert mix by channel + by MCC group (reuse the grouping semantics of `dbt/macros/mcc_group.sql` — SQL CASE mirroring it; note the drift caveat in a comment).
   - **Cold-card share** over last 5 min (tells the Redis-degradation resilience story live).
3. **`docker/Dockerfile.dashboard`** (slim, non-root) + compose service `dashboard` (profile `demo`, port 8050, depends_on bank-db healthy; API optional — degrade, don't crash).
4. Env: `BANK_DB_*` (shared), `API_METRICS_URL`, `DASHBOARD_PORT`. Extend `.env.example`.
5. **`tests/test_dashboard.py`**: hermetic — Prometheus-text parser unit tests (fixture string → quantiles), SQL query builders return expected columns, alert-action callback issues the right UPDATE (mock engine), app factory imports clean (`create_app()` smoke).

## Acceptance
- `docker compose --profile demo up` → http://localhost:8050 renders all panels within ~2 min of replay start; alert buttons persist status to SQL; kill redis container → cold-card tile visibly rises, dashboard stays up.
- `make check` green.
