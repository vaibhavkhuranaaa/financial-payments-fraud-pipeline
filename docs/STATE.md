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
| 2 | `src/app.py`: /score, /healthz, /metrics + latency benchmark | pending (ticket 02) |
| 3 | Docker compose, Terraform (Event Hubs/ACR/Container Apps), CI | pending (ticket 03) |
| 4 | dbt marts, governance docs, README | pending (ticket 04) |
| 5 | E2E verify, Azure deploy + teardown test, tag v1.0 | pending |

## Done & verified
- Repo scaffold (commit e0ccd29)
- Plan approved 2026-07-17; user decisions: real Azure deploy w/ teardown, Colima for Docker, full 24M-row training data, Terraform, dbt, Databricks-compatible-local, governance-as-artifacts
- Phase 0 complete: Colima running (docker 29.5.2), `.venv` Python 3.11 with pinned deps, full TabFormer at `data/raw/card_transaction.v1.csv` (24M rows, 2.2GB, out of git), committed sample `data/sample/transactions_sample.csv` (76,989 rows / 7.1MB / 0.22% fraud; 100 users × most-recent ≤1000 txns each, per-card sequences intact, seed=42), contract v1, discipline scaffold

## In flight
- Phase 1 pipeline subagent (ticket `docs/tickets/01-pipeline.md`)

## Next step
Dispatch/complete ticket 01 (pipeline subagent on Sonnet), review its diff, run its verification commands, commit `feat(pipeline)`, tag `phase-1-pipeline`, then write ticket 02 (API) — see plan phases 2–5.

## Environment facts
- macOS (darwin 25.5), system Python 3.14 (too new for PySpark → use uv-managed 3.11 venv at `.venv/`)
- Azure CLI logged in; subscription `278f1d2f-c561-4a57-ae32-0a5062f7e6b9`
- Docker via Colima (not Docker Desktop)

## Azure resources (record after `terraform apply`; needed for teardown)
_None provisioned yet._

## Known issues
_None yet._
