# Handoff — post-v1.2 (2026-07-18, second session)

> Read `docs/STATE.md` first (source of truth: phase table, environment facts, exact next step).
> This file adds the v1.2 session summary, what failed along the way, the updated roadmap, and a
> candid credibility assessment. Repo state at handoff: everything pushed, `git status` clean,
> `make check` green (133 tests), no containers running, $0 cloud spend, no AI co-author trailers.

## Session summary (v1.2 CDC ingestion — shipped, tagged `v1.2`)

| Commit | What |
|---|---|
| a4e1a96 | Scorer delivery semantics: auto-commit off, Kafka offsets committed synchronously only AFTER the bank-DB flush (roadmap item 1 folded in); fake-consumer test pins flush→commit ordering |
| 87a982d | DB side: bank DB moved `master`→`frauddemo` (CDC refused on system DBs; `bootstrap_database()` provisions idempotently, seed fingerprint unchanged `ff724846752b`), new `bank.card_transactions` OLTP table, `txn_writer.py` (CSV→SQL replay), `cdc.py` (`--enable` + `--scan` pump — SQL Edge has no Agent), compose profile `cdc` |
| 4b3909a | Kafka side: Debezium connector config-as-code + idempotent registrar (PUT), `cdc_transformer.py` (envelope→contract-v1, same commit-after-flush boundary), `make demo-cdc`. Topic is `bankdb.frauddemo.bank.card_transactions` (Debezium 2.x naming: prefix.database.schema.table) |
| 8a2f869 | **The pivot** (see "What failed"): `cdc_streamer.py` replaces Debezium at runtime — reads the real CDC change tables LSN-windowed, does the LSN increment in Python, emits byte-compatible Debezium envelopes (round-trip test through the transformer), persists LSN offset to `bank.cdc_offsets` strictly after producer flush. Debezium Connect moved to opt-in profile `debezium` (drop-in vs full SQL Server). ADR 0003 + README + lineage + data dictionary updated |
| 3159591 | fix(scorer): flush sub-batch tail on idle polls (found because CDC drain stalled 38 rows short) |
| 0bb46d3 | test(bank): mock `bootstrap_database` in seed CLI test (hermeticity regression — only passed while bank-db happened to be up) |
| eb685bc | docs: ticket 13 (v1.3 observability) written, ready to build |

**Final verification (all live, clean-state):** `make demo-cdc` up in 5:21; insert→scored **~1.2s** end-to-end at ~180 events/s; full drain **77,089 inserted / 77,089 scored / 0 duplicate event_ids**; cdc-streamer killed mid-run → resumed from SQL-stored LSN, full catch-up; scorer SIGTERM-killed holding a 38-row uncommitted tail → rows re-delivered after restart and landed exactly once (commit-after-flush proven under a real crash). Replay mode re-verified green (+2,400 scored/12s, dashboard 200). Both modes torn down cleanly.

## What failed (read before trusting any Debezium-on-Edge assumption)
1. **Debezium cannot stream from Azure SQL Edge — permanent, not a config issue.** The SQL Server connector's streaming loop calls CLR-backed `sys.fn_cdc_increment_lsn` every iteration; Edge has CLR hard-disabled (`sp_configure 'clr enabled',1` → error 15392 "not supported by this edition"). Snapshot works, then streaming errors forever in a retriable loop. Everything else in Edge's CDC surface works (`fn_cdc_get_min/max_lsn`, `fn_cdc_get_all_changes`, change tables via the `sp_cdc_scan` pump). Resolution: `cdc_streamer.py` (ticket 11's pre-approved fallback, upgraded — it reads the real log-derived change tables, not a polled timestamp column). Full story in ADR 0003.
2. **Debezium 2.x topic naming surprise:** `prefix.database.schema.table`, so the first live run streamed to `bankdb.frauddemo.bank.card_transactions` while the transformer listened on `bankdb.bank.card_transactions` — caught live, fixed everywhere (4b3909a).
3. **Scorer idle-tail bug (mine, introduced in a4e1a96):** with manual commits, a tail smaller than BATCH_SIZE never flushed on an idle stream — surfaced as a permanent 38-row gap during drain verification. Fixed + regression-tested (3159591).
4. **`v1.2` tag placement miss:** a `make check` failure was masked by piping through `tail` (pipeline exit code), so the tag was pushed pointing at 1fced83, one commit before the hermeticity test fix 0bb46d3. The tagged code is correct; the tagged tree's test suite fails only when no local bank-db is up. Moving the tag = deleting a pushed tag = user-approval item; **left as-is, user informed.** Lesson: check `$?` of the command, not the pipe.
5. **Ticket 13 implementation was started and deliberately discarded** at the user's 95%-usage stop order — a subagent had produced three partial untested files (lag exporter, tests, prometheus/grafana configs); they were deleted for a clean handoff. Nothing of it is committed; the ticket (`docs/tickets/13-observability.md`, commit eb685bc) contains the full validated spec including the verified fact that `/metrics` is already Prometheus text format.

## Roadmap — done vs remaining (impact order)
**Done:** v1.0 (streaming pipeline + scored API + Azure deploy/teardown) · v1.1 (bank system-of-record, live scorer loop, fraud-ops dashboard, `make demo`) · v1.2 (CDC ingestion + explicit end-to-end delivery semantics = original hardening item 1).

**Remaining:**
1. **v1.3 = Observability** (`docs/tickets/13-observability.md`, ready to build): Prometheus + Grafana-as-code + consumer-lag exporter (`kafka_consumergroup_lag` is the flagship DE signal), staleness metric, alert rules, `OBS=1` demo modes.
2. **Schema registry + typed events** — Avro/Protobuf with Redpanda's built-in registry replacing loose JSON + hand-rolled JSON-Schema checks (no ticket yet).
3. **Secrets & security hardening** — fail-loud on missing passwords instead of `LocalDev!Passw0rd` compose defaults, Key Vault wiring in terraform, trivy scan in CI, digest-pinned base images (no ticket yet).
4. **Load/soak testing** — k6/locust against `/score` + end-to-end lag under sustained load; backpressure numbers for the README (no ticket yet).
5. **Model ops** — model artifact versioning (MLflow or plain), drift monitoring vs training distribution, scheduled retrain recipe (no ticket yet).
6. **CI/CD depth** — compose-based E2E smoke in CI (assert alerts flow), pre-commit hooks, terraform remote state + plan-on-PR (no ticket yet).

Explicitly rejected (don't revisit without the user): graph/ring features. `docs/tickets/12-dual-stream.md` stays a stub pending design + user approval.

## Candid assessment: big-bank resume credibility
**Verdict: credible to list, and above the portfolio-project bar in three specific areas — but not yet "industry-grade" as a whole. Say "production-shaped demo with real delivery guarantees," not "production system."**

Judged against real bars:
- **Data delivery guarantees — genuinely strong now.** At-least-once with explicit commit-after-flush at every consumer, effectively-once in SQL via PK dedupe, *proven under live kill tests with exact row accounting* (77,089/77,089, 0 dups), and honestly documented where duplicates can still occur (ADR 0003). Most portfolio projects claim semantics; this one demonstrates and falsifies them. The CDC story (OLTP system of record → log-derived change feed → contract topic) is the architecture a payments platform team actually runs, and the ADR documenting *why Debezium itself couldn't run and what replaced it* reads like a real engineering incident, which is worth more in an interview than a clean Debezium deploy would have been.
- **Testing/CI rigor — decent, not bank-grade.** 133 hermetic unit tests, contract validation at three boundaries, CI runs lint+tests+dbt+terraform validate. Missing: an automated E2E smoke in CI (today E2E verification is manual/session-driven), integration tests against a real broker/DB in CI, coverage gates, pre-commit hooks. A bank platform team would also expect mutation/property testing on the money-adjacent paths.
- **Observability — the biggest gap, and the next ticket.** There are `/metrics` and measured latency numbers, but no time series, no lag visibility, no alerting, no structured logs, no traces. "How far behind is scoring right now?" currently requires SQL. Until v1.3 lands, ops maturity is demo-level.
- **Security posture — demo-level by design.** Default SA password fallbacks in compose, sa-as-app-user, no TLS locally, tokenization salt in `.env.example`, no vuln scanning, no Key Vault. Fine for a $0 local demo, would fail any bank security review; roadmap item 3 exists for exactly this. The honest parts (PAN tokenization policy doc, synthetic-data-only constraint, secrets never committed) are real positives to talk about.
- **Ops maturity — partial.** One-command up/down, idempotent init, kill/restart resilience verified, Azure deploy+teardown tested once with real numbers. Missing: HA anything (single broker/partition/instance), backpressure story under real load, runbooks, capacity numbers beyond a laptop.

What makes it land on a resume today: measured numbers everywhere (p99s, events/s, insert→scored latency, exact-count delivery proofs), decisions-as-ADRs including a documented failure pivot, and a 3-minute live demo (`make demo-cdc`) that shows the whole loop. What would make it "industry-grade": v1.3 observability + CI E2E smoke + the security pass — items 1, 3, 6 above. With those three done, the delivery-semantics + CDC + observability triad genuinely resembles what a mid-level platform DE ships.

## Follow-up prompt for a fresh agent (any model/vendor)
> Continue the fraud-pipeline project autonomously. Read `docs/STATE.md` first (source of truth), then `docs/HANDOFF.md`, then `docs/tickets/13-observability.md`, and build v1.3 (observability: Prometheus + Grafana provisioned as code + consumer-lag exporter + alert rules + `OBS=1` demo wiring) per that ticket's acceptance criteria. Work in self-contained chunks; if your environment supports cheaper delegate agents, use one per chunk (point them at ticket/files, don't paste content) and review their diffs yourself. Auto-apply edits/commits/pushes when gates pass (`make check` + ticket acceptance verified live); keep commits atomic with no AI co-author trailers; update `docs/STATE.md` as you go; tag `v1.3` after live E2E verification (kill the scorer → watch lag climb and recover on the Grafana board). Hard stops requiring user confirmation: any infra provisioning/teardown (`terraform apply`/`destroy`), `git push --force`, deleting data/branches/tags, anything touching secrets/credentials/IAM. At ~95% session usage: stop new work, run a full verification pass, get the repo fully clean and pushed, rewrite `docs/HANDOFF.md` (session summary, what failed, roadmap, candid big-bank credibility assessment), and print this same style of follow-up prompt for the next session.
