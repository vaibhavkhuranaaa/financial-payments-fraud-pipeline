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
| 6 | Bank system-of-record: `bank-db` (Azure SQL Edge), `src/bank/schema.sql`+`seed.py`+`db.py`, `init-bank` compose service (ticket 06) | **done** (commit addf3f2; seed fingerprint `ff724846752b` — 100 customers / 100 accounts / 130 cards, deterministic Faker seed=42; bank-db healthcheck is a TCP-only `/dev/tcp` probe, no `sqlcmd` in the azure-sql-edge image) |
| 7 | Live scorer loop: `src/pipeline/scorer.py`, `scorer` compose service (ticket 07) | **done** (commit 9256e02; Kafka `transactions` consumer → same `/score` endpoint the API benchmark measures → `bank.scored_transactions`/`bank.fraud_alerts`, idempotent on `event_id`) |
| 7 | Fraud-ops dashboard: `src/dashboard/`, `docker/Dockerfile.dashboard`, `dashboard` compose service (ticket 08) | **done** (commit b985836; Plotly Dash on :8050 — stat tiles, live feed, alerts queue with Confirm/Dismiss write-back, score/throughput/alert-mix/cold-card charts) |
| 8 | One-command demo (`make demo`/`scripts/demo.sh`), optional Azure demo Terraform module (`infra/terraform/demo.tf`), CI gate; ADR 0002 + README + governance docs; tag v1.1 | **done** (commit ff914c6 for demo/infra/CI; docs finished this ticket — 2026-07-18: `make demo` clean-state verified ~4:38 on a warm image cache, alerts flowing (`fraud_alerts` 1,147→1,582 over 10s); terraform demo module validate/plan-only, never applied, ~$1.50–2.50/day estimate if run) |

## Done & verified
- Repo scaffold (commit e0ccd29)
- Plan approved 2026-07-17; user decisions: real Azure deploy w/ teardown, Colima for Docker, full 24M-row training data, Terraform, dbt, Databricks-compatible-local, governance-as-artifacts
- Phase 0 complete: Colima running (docker 29.5.2), `.venv` Python 3.11 with pinned deps, full TabFormer at `data/raw/card_transaction.v1.csv` (24M rows, 2.2GB, out of git), committed sample `data/sample/transactions_sample.csv` (76,989 rows / 7.1MB / 0.22% fraud; 100 users × most-recent ≤1000 txns each, per-card sequences intact, seed=42), contract v1, discipline scaffold

## In flight
- **v1.2 (CDC ingestion, ticket 11) IN PROGRESS (2026-07-18):**
  - DONE (pushed): commit a4e1a96 — scorer delivery semantics: auto-commit off, offsets committed synchronously only after the bank-DB flush (hardening roadmap item 1 folded into v1.2); fake-consumer test pins flush→commit ordering.
  - DONE (pushed): commit 87a982d — DB side: bank DB moved master→`frauddemo` (CDC refused on system DBs; `bootstrap_database()` provisions idempotently, seed fingerprint unchanged ff724846752b), new `bank.card_transactions` OLTP table, `src/bank/txn_writer.py` (CSV→SQL replay), `src/bank/cdc.py` (--enable idempotent, --scan pump), compose profile `cdc` (init-cdc, cdc-scan, txn-writer). Verified live: SQL Edge CDC works but ONLY via manual `sp_cdc_scan` (no Agent in Edge — the pump replaces the capture job); 25 writer inserts appeared in `cdc.bank_card_transactions_CT`.
  - DONE (pushed): commit 4b3909a — Kafka side: connector config as code + registrar, `cdc_transformer.py` (Debezium envelope→contract-v1, commit-after-flush), `make demo-cdc`. Topic is `bankdb.frauddemo.bank.card_transactions` (Debezium 2.x = prefix.database.schema.table).
  - DONE (this commit): **Debezium cannot stream from SQL Edge** — found live: streaming loop needs CLR-backed `sys.fn_cdc_increment_lsn`, CLR is hard-disabled on Edge (error 15392; snapshot works, streaming errors forever). Built the ticket's pre-approved fallback: `src/pipeline/cdc_streamer.py` reads the real CDC change tables LSN-windowed via `fn_cdc_get_all_changes`, does the LSN increment in Python, emits byte-compatible Debezium envelopes (round-trip test through the transformer), persists LSN offset to `bank.cdc_offsets` strictly after producer flush. Debezium Connect + config moved to opt-in compose profile `debezium` (drop-in vs full SQL Server). Clean-state E2E verified: `make demo-cdc` up 5:21, insert→scored ~1.2s end-to-end at ~180 events/s; kill/restart cdc-streamer → catch-up with zero duplicate event_ids.
  - REMAINING: final drain check (scored == 76,989), teardown, tag v1.2.
- **v1.1 COMPLETE (2026-07-18).** Tickets 06–10 all done (commits addf3f2, 9256e02, b985836, ff914c6 + docs commit). Final orchestrator E2E verify 2026-07-18: `make demo` up, dashboard :8050 200, scoring ~200 events/s (`scored_transactions` +1600/8s, 14.7k alerts); kill-redis → dashboard stayed 200, cold-card share hit 100%; teardown clean (`docker ps` empty). Nuance: after restarting Redis, cold-card share does NOT recover in seconds — Redis is in-memory, features repopulate only as cards recur (~1 min+); demo talk track's "recover" beat needs that pause. Tagged `v1.1`.
- **v1.0 COMPLETE (2026-07-17).** All Definition-of-Done boxes checked; local stack still runs via compose (see README). Streaming sink was rewritten during Phase 5 E2E (v2 windows broke the Spark sliding-window design — see commit "v2-compatible streaming sink" and features.py module docstring). Known accepted gaps are in README "What I'd Improve Next".
- **v2 model iteration COMPLETE (2026-07-17):** `wip/model-iteration-v2` merged to main (merge commit 02f2fe4). API `/score` now builds its vector via the shared `build_feature_row` (skew rule); all 45 tests + `make check` green. README Key Results filled from the v2 full-data run (PR-AUC 0.0227, ROC-AUC 0.768, precision@top-0.1% 0.045); README/data-dictionary/tokenization-policy updated to 1h/1d/7d/30d windows. AI co-author trailers stripped from all local history (filter-branch + reflog expire + gc; verified zero). Retrain command if ever needed: `.venv/bin/python -m src.pipeline.train --input data/raw/card_transaction.v1.csv --since-year 2013 --until-year 2019` (~12 min).

## Next step (exact resume sequence) — v1.2 CDC ingestion (user approved 2026-07-18: v1.1 first, v1.2 next session)
v1.1 is tagged; v1.2 = Debezium CDC bank-DB→Kafka replacing CSV replay as the headline ingest (`docs/tickets/11-cdc-ingestion.md`).

1. Read `docs/tickets/11-cdc-ingestion.md`; if it needs a design pass, do it before code.
2. Execute via ONE Sonnet subagent per self-contained chunk (point at ticket file, don't paste content); orchestrator (strongest model) reviews diffs, runs gates (`make check` + ticket acceptance), fixes seams inline — never respawn for small fixes.
3. Token rules: no exploratory subagents; read only what a decision needs; batch independent tool calls.
4. Cost rule: terraform validate/plan only — ask the user before any `apply`.
5. Verify E2E, update this file, tag `v1.2`, push (history stays free of AI co-author trailers).
6. At 95% session usage: STOP feature work, commit green work only, write `docs/HANDOFF.md` (done / in-flight / exact next step / gotchas / container state), update this file, print the fresh-session follow-up prompt.

Fresh-session follow prompt:
> /goal Continue fraud-pipeline v1.2 (CDC ingestion). Read docs/STATE.md first — source of truth — then docs/tickets/11-cdc-ingestion.md. Sonnet subagents per the orchestration notes in STATE.md Next step, auto-edit, handoff at 95%.

## Backlog (approved direction, post-v1.1)
- **v1.2 = CDC ingestion** (`docs/tickets/11-cdc-ingestion.md`): now unblocked (v1.1 tagged) — see Next step above.
- **Industry-grade hardening roadmap** (2026-07-18, 7 items in impact order): `docs/HANDOFF.md` — delivery semantics, schema registry, observability (Grafana/OTel/consumer lag), secrets hardening, load testing, model ops, CI/CD depth.
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
