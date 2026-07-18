# Handoff — post-v1.1 (2026-07-18)

> Read `docs/STATE.md` first (source of truth). This file adds the v1.1 session summary and the
> approved industry-grade hardening roadmap.

## Session summary (v1.1, shipped 2026-07-18)
Built and tagged `v1.1` in one session via one Sonnet subagent per ticket (06–10), orchestrator gating each:

| Commit | Ticket | What |
|---|---|---|
| addf3f2 | 06 | Azure SQL Edge system-of-record: `bank` schema, deterministic seed (fingerprint `ff724846752b`), `db.get_engine()` |
| 9256e02 | 07 | Live scorer loop: Kafka → `/score` → `bank.scored_transactions`/`fraud_alerts`, batched, idempotent |
| b985836 | 08 | Plotly Dash fraud-ops dashboard :8050 — tiles, live feed, alert queue w/ SQL write-back, charts |
| ff914c6 | 09 | `make demo`/`demo-down`, terraform demo module (validate-only, $0 spent), CI dashboard build |
| de38e13 | 10 | ADR 0002, README demo section + 3-min recruiter talk track, governance docs, STATE |

Verified: `make check` green (93 tests); `make demo` clean-state ~4:38; live demo ~6,150 scores/min,
p99 8.7ms; kill-redis → dashboard stays up, cold-card 100% (recovery takes ~1 min+ as cards recur —
pause before the "recovery" beat in the talk track). Everything pushed; local stack DOWN; zero Azure spend.

## Industry-grade roadmap (recommended order)
Already planned: **v1.2 = CDC ingestion** (`docs/tickets/11-cdc-ingestion.md`, Debezium bank-DB→Kafka
replacing CSV replay) — this is the single biggest "production-real" upgrade; do it first.

Then, in impact order (each is a candidate release; write a ticket before building):
1. **Delivery semantics (v1.2 scope or v1.2.1)** — today the scorer is at-least-once + PK dedupe. Make it
   explicit: commit Kafka offsets only after the SQL flush succeeds; document the exactly-once boundary.
2. **Schema registry + typed events** — Avro/Protobuf with a registry (Redpanda has one built in) instead
   of loose JSON; contract evolution story replaces the hand-rolled JSON-schema check.
3. **Observability stack** — Prometheus + Grafana in compose (scrape the existing `/metrics`), structured
   JSON logging everywhere, OpenTelemetry traces producer→scorer→API; alert rules (lag, error rate, p99).
   Consumer-lag metric is the flagship DE signal — export it.
4. **Secrets & security hardening** — no default passwords in `.env.example` fallbacks (fail loud instead),
   Azure Key Vault wiring in terraform, TLS notes, image vulnerability scan (trivy) in CI, pin base images
   by digest.
5. **Load/soak testing** — k6 or locust profile against `/score` + end-to-end lag under sustained load;
   backpressure behavior documented with numbers (this upgrades the README's differentiator).
6. **Model ops** — MLflow (or plain artifact versioning) for the model, drift monitoring job comparing live
   score distribution vs training, scheduled retrain recipe.
7. **CI/CD depth** — compose-based E2E smoke in CI (spin stack, assert alerts flow), pre-commit hooks,
   terraform remote state + plan-on-PR.

Explicitly rejected earlier (don't revisit without user): graph/ring features. v1.3 dual-stream
(`docs/tickets/12-dual-stream.md`) stays a stub pending design + user approval.

## Fresh-session follow-up prompt
> /goal Continue fraud-pipeline v1.2 (CDC ingestion). Read docs/STATE.md then docs/HANDOFF.md, then
> docs/tickets/11-cdc-ingestion.md. Fold roadmap item 1 (offset-commit-after-flush delivery semantics)
> into the ticket if it fits, else ticket it separately. One Sonnet subagent per self-contained chunk
> (point at ticket files, don't paste); orchestrator reviews diffs, runs gates, fixes seams inline.
> Terraform validate/plan only — ask before any apply. Verify E2E, update STATE.md, tag v1.2, push
> (no AI co-author trailers). At 95% session usage: stop, commit green, update HANDOFF.md + STATE.md,
> print the next follow-up prompt.
