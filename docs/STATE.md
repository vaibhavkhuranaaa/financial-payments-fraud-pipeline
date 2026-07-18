# Build State — single source of truth for handoff

> **Resuming this build?** Read `CLAUDE.md` (project spec) + the approved plan summary below, then continue
> from **Next Step**. Every phase's spec lives in `docs/tickets/`. Decisions live in `docs/adr/`.
> Never push without `make check` passing.

## Approved plan (summary)
Streaming fraud-detection pipeline on IBM TabFormer (24M txns):
CSV replay producer (contract-validated, PAN-tokenized, DLQ) → Kafka (Redpanda local / Azure Event Hubs cloud)
→ Spark Structured Streaming windowed features → Redis (online) + Delta Lake (offline)
→ XGBoost training (Databricks Asset Bundle-packaged, run locally) → Flask `/score` with measured p50/p95/p99
→ dbt (duckdb) fraud-ops marts → Terraform-provisioned Azure (Event Hubs Standard, ACR, Container Apps), tested teardown.
Full plan: `~/.claude/plans/review-the-repository-plan-frolicking-gem.md` (local to author's machine; key content mirrored in tickets/ADRs).

## Phase status
| Phase | Scope | Status |
|---|---|---|
| 0 | Tooling (Colima/Java/uv venv), discipline scaffold, data sample, data contract | **done** |
| 1 | `src/pipeline/`: producer+DLQ, Spark streaming features, XGBoost training | pending (ticket 01) |
| 2 | `src/app.py`: /score, /healthz, /metrics + latency benchmark | **done** (commit c898f15; cold-path p99 9.9ms @ 1884 req/s; warm-Redis bench pending Phase 5) |
| 3 | Docker compose, Terraform (Event Hubs/ACR/Container Apps), CI | **done** (commit 040253d; validated, NOT applied — images not yet built since src in flight) |
| 4 | dbt marts, governance docs, README | **done** (README metrics filled from v2 full-data run; diagrams + governance docs updated to 1h/1d/7d/30d windows) |
| 5 | E2E verify, Azure deploy + teardown test, tag v1.0 | **done** (2026-07-17: compose E2E verified — 114 Redis feature hashes, warm /score, quarantine; warm bench p99 10.8ms @ 1237 req/s; Azure deployed, live-verified, destroyed; tagged v1.0) |

## Done & verified
- Repo scaffold (commit e0ccd29)
- Plan approved 2026-07-17; user decisions: real Azure deploy w/ teardown, Colima for Docker, full 24M-row training data, Terraform, dbt, Databricks-compatible-local, governance-as-artifacts
- Phase 0 complete: Colima running (docker 29.5.2), `.venv` Python 3.11 with pinned deps, full TabFormer at `data/raw/card_transaction.v1.csv` (24M rows, 2.2GB, out of git), committed sample `data/sample/transactions_sample.csv` (76,989 rows / 7.1MB / 0.22% fraud; 100 users × most-recent ≤1000 txns each, per-card sequences intact, seed=42), contract v1, discipline scaffold

## In flight
- **v1.0 COMPLETE (2026-07-17). Nothing in flight.** All Definition-of-Done boxes checked; local stack still runs via compose (see README). Streaming sink was rewritten during Phase 5 E2E (v2 windows broke the Spark sliding-window design — see commit "v2-compatible streaming sink" and features.py module docstring). Known accepted gaps are in README "What I'd Improve Next".
- **v2 model iteration COMPLETE (2026-07-17):** `wip/model-iteration-v2` merged to main (merge commit 02f2fe4). API `/score` now builds its vector via the shared `build_feature_row` (skew rule); all 45 tests + `make check` green. README Key Results filled from the v2 full-data run (PR-AUC 0.0227, ROC-AUC 0.768, precision@top-0.1% 0.045); README/data-dictionary/tokenization-policy updated to 1h/1d/7d/30d windows. AI co-author trailers stripped from all local history (filter-branch + reflog expire + gc; verified zero). Retrain command if ever needed: `.venv/bin/python -m src.pipeline.train --input data/raw/card_transaction.v1.csv --since-year 2013 --until-year 2019` (~12 min).

## Next step (exact resume sequence) — v1.1 build approved 2026-07-17
Approved plan: recruiter-ready demo — Azure SQL Edge system-of-record + live scorer loop + Plotly Dash fraud-ops dashboard + `make demo`. Full plan mirrored in tickets; costs: local $0, optional Azure demo-day ~$1.50–2.50/day (ask user before any `terraform apply`, ~$2–3 one-off).

1. Execute `docs/tickets/06-bank-db.md` → `07-scorer-loop.md` + `08-dashboard.md` (parallelizable after 06) → `09-demo-infra-ci.md` → `10-docs-tests.md`, each via a **Sonnet subagent** (self-contained specs; orchestrator reviews diffs + runs gates, fixes seams inline).
2. Verify per plan: `make demo` from clean state (dashboard :8050, alerts flowing, zero manual steps), kill-redis degradation visible, `make check` green.
3. Tag `v1.1`, push (history is clean — no AI trailers, keep it that way).
4. Orchestration rules: auto-edit /goal mode, don't stop until done; at 95% session usage write `docs/HANDOFF.md` + update this file + exact follow-up prompt, commit green first.

Fresh-session follow prompt:
> Continue the fraud pipeline v1.1 build. Read docs/STATE.md and docs/tickets/06–10, execute in order with Sonnet subagents per the orchestration notes, /goal mode, auto-edit, handoff at 95%.

## Backlog (approved direction, post-v1.1)
- **v1.2 = CDC ingestion** (`docs/tickets/11-cdc-ingestion.md`): Debezium bank-DB→Kafka replaces CSV replay as the headline; do not start before v1.1 tags.
- **v1.3 candidate = dual auth/settlement streams** (`docs/tickets/12-dual-stream.md`): stub only, needs design pass + user approval. Graph/ring features explicitly rejected.

## Environment facts
- macOS (darwin 25.5), system Python 3.14 (too new for PySpark → use uv-managed 3.11 venv at `.venv/`)
- Azure CLI logged in; subscription `278f1d2f-c561-4a57-ae32-0a5062f7e6b9`
- Docker via Colima (not Docker Desktop)

## Azure resources (record after `terraform apply`; needed for teardown)
Provisioned 2026-07-17 (prefix `fraudpl`, eastus2), verified live, then destroyed same day — see below:
- RG `fraudpl-rg`: ACR `fraudplacr`, Event Hubs ns `fraudpl-ehns` (transactions, transactions-dlq), Log Analytics `fraudpl-law`, Container Apps env `fraudpl-cae`, app `fraudpl-api` (FQDN was fraudpl-api.redsmoke-f278091d.eastus2.azurecontainerapps.io)
- One-time subscription fix that had to be applied: `az provider register --namespace Microsoft.App`
- Live check: /healthz ok, /score 200 (server-side ~2.3ms); benchmark p50 147ms / p95 282ms / p99 290ms @ 21.8 req/s cross-internet
- Teardown: `destroy.sh` run + `az group exists fraudpl-rg` = false (verify current state before assuming anything is running)

## Known issues
_None yet._
