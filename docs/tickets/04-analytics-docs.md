# Ticket 04 — Analytics (dbt) + governance docs + README + tests sweep

**Owner:** Docs/analytics subagent. **Scope:** `dbt/`, `docs/governance/`, `README.md`, `tests/` (additive only), CHANGELOG entry. Do not touch `src/`, `docker/`, `infra/`, STATE.md.

## Context
Read first: `CLAUDE.md`, ADRs, contract, `src/pipeline/features.py` (feature list), `models/metrics.json` (real model metrics), `benchmarks/latest.json` (real latency numbers — if missing, leave clearly-marked `<PENDING BENCHMARK>` placeholders that the orchestrator fills in Phase 5).

## Deliverables

### 1. `dbt/` project (dbt-duckdb, installed in .venv)
- `dbt_project.yml`, `profiles.yml` (duckdb, path `dbt/fraud.duckdb`), packages: none (keep hermetic).
- Sources: external parquet/CSV — read `data/sample/transactions_sample.csv` via a staging model that mirrors the producer's `to_event` mapping in SQL (documented as the analytical mirror of the contract) OR, if `data/delta/events` exists, read its parquet files. Sample CSV path must work so CI can run `dbt build`.
- Models: `staging/stg_transactions.sql` (typed, renamed columns per contract); marts: `fct_daily_fraud_rate.sql` (by date: txn count, fraud count, fraud rate, gross amount), `fct_merchant_risk.sql` (by mcc_group + merchant_state: volume, fraud rate, avg amount), `fct_channel_mix.sql` (chip/swipe/online volumes + fraud rate, cross-border share).
- Tests: not_null/unique on keys, accepted_values on channel, a custom test asserting fraud_rate between 0 and 1.
- `dbt build` must pass from `dbt/` with `--profiles-dir .`.

### 2. `docs/governance/`
- `data-dictionary.md`: every contract field + every FEATURE_COLUMNS feature with type, source, meaning.
- `lineage.md`: mermaid flowchart source→topic→stream→(redis|delta)→(train|dbt marts)→api.
- `tokenization-policy.md`: salted SHA-256 card tokenization, salt handling (env/Key Vault in cloud), why raw PANs never enter the pipeline, rotation consequences.

### 3. `README.md`
Follow the existing template headings exactly (Problem, Data, Architecture, Key Results, How to Run Locally, How to Deploy (Azure), Tech Stack, What I'd Improve Next). Mermaid architecture diagram. Key Results: model metrics from models/metrics.json (real numbers) + latency table (real if benchmarks/latest.json exists, else `<PENDING BENCHMARK>`). Local run = `docker compose -f docker/docker-compose.yml up` + producer profile; deploy = `infra/terraform/deploy.sh`. Honest limitations section (synthetic data, single-node streaming, dbt-over-duckdb vs Databricks SQL).

### 4. Tests sweep
- Add any missing high-value unit tests (do not duplicate existing); ensure `pytest -q` green and total runtime < 120s.

## Acceptance criteria
- `cd dbt && ../.venv/bin/dbt build --profiles-dir .` green.
- `make check` passes at repo root.
- README contains zero placeholder brackets except explicitly `<PENDING BENCHMARK>` items.

## Verification commands
```bash
cd dbt && ../.venv/bin/dbt build --profiles-dir . && cd ..
make check
```

## Report back
Status; files changed; verbatim verification tails; list of any `<PENDING BENCHMARK>` placeholders left for the orchestrator.
