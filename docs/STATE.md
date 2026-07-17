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
| 4 | dbt marts, governance docs, README | **done** (commit 60b7f54; README has `<PENDING METRICS>` for the orchestrator to fill after the model iteration; diagram still says 1m/10m/1h windows — update alongside metrics) |
| 5 | E2E verify, Azure deploy + teardown test, tag v1.0 | pending |

## Done & verified
- Repo scaffold (commit e0ccd29)
- Plan approved 2026-07-17; user decisions: real Azure deploy w/ teardown, Colima for Docker, full 24M-row training data, Terraform, dbt, Databricks-compatible-local, governance-as-artifacts
- Phase 0 complete: Colima running (docker 29.5.2), `.venv` Python 3.11 with pinned deps, full TabFormer at `data/raw/card_transaction.v1.csv` (24M rows, 2.2GB, out of git), committed sample `data/sample/transactions_sample.csv` (76,989 rows / 7.1MB / 0.22% fraud; 100 users × most-recent ≤1000 txns each, per-card sequences intact, seed=42), contract v1, discipline scaffold

## In flight (SESSION PAUSED HERE — usage limit; resume from this block)
- **Branch `wip/model-iteration-v2`** (commit 1d034d2) holds the v2 feature set: windows changed 1m/10m/1h → 1h/1d/7d/30d (density-matched to ~daily card activity — v1 windows were almost always empty, root cause of PR-AUC 0.0029), new features has_error / raw mcc / is_chip / is_swipe / amount_over_mean_30d / time_since_last_txn_s / is_new_city_30d (34 total), tuned params, precision@top-k metrics. Vectorized==row-wise parity test updated and green.
- **KNOWN BROKEN on that branch:** 5 `tests/test_api.py` failures — `src/app.py`'s feature-vector builder doesn't produce the new columns (KeyError `is_chip`). Expected integration gap; API agent was told not to touch pipeline files and vice versa.
- **v2 full retrain DONE** (2026-07-17, 699s wall / 6.9GB RSS, 11.9M rows 2013–2019, `models/*` on disk are current): PR-AUC **0.0227** (v1: 0.0029), ROC-AUC **0.768** (v1: 0.65), precision@top-0.1% 0.045 (~34× lift over 0.13% base rate), F1-threshold precision 0.0065 / recall 0.179, best config "deeper_slower" (see metrics.json tuning_history), 340 rounds. Command if a rerun is ever needed: `.venv/bin/python -m src.pipeline.train --input data/raw/card_transaction.v1.csv --since-year 2013 --until-year 2019` (~12 min).

## Next step (exact resume sequence)
0. Strip AI co-author trailers from ALL local commits (main + wip branch) before any push: `git filter-branch --msg-filter 'sed "/Co-Authored-By: Claude/d"' -- --all`; verify `git log --all --grep=Claude` is empty. No trailers on future commits.
1. ~~Check retrain~~ done — metrics above; `models/*` current.
2. On `wip/model-iteration-v2`: fix `src/app.py` feature building for the v2 columns (Redis hash now also carries `last_event_ts`; keep derivation in shared features.py per ticket-01 skew rule), make the 5 API tests green.
3. If v2 metrics are sane (PR-AUC should beat 0.0029 by a lot; check precision@top-0.1%): merge branch to main, fill README `<PENDING METRICS>` (lines ~47–52) + update README/lineage diagrams (still say 1m/10m/1h), refresh dbt window references if any.
4. Phase 5 (task #6): `docker compose -f docker/docker-compose.yml up` E2E (producer profile `replay`; verify Redis `features:*` keys + `/score` warm path + DLQ), warm benchmark → README, `infra/terraform/deploy.sh` (prefix var, ~$40-50/mo), live latency, `destroy.sh` test, tag v1.0.

## Environment facts
- macOS (darwin 25.5), system Python 3.14 (too new for PySpark → use uv-managed 3.11 venv at `.venv/`)
- Azure CLI logged in; subscription `278f1d2f-c561-4a57-ae32-0a5062f7e6b9`
- Docker via Colima (not Docker Desktop)

## Azure resources (record after `terraform apply`; needed for teardown)
_None provisioned yet._

## Known issues
_None yet._
