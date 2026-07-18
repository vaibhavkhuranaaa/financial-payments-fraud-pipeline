# ADR 0002: Bank system-of-record, scorer loop, and dashboard for v1.1

- **Status:** accepted (2026-07-18, approved by project owner)
- **Context:** v1.0 proved the streaming pipeline end-to-end but had no "watch it happen" surface — a recruiter screen-share needs a live, visual demo, and the pipeline needs a system-of-record to score *against* and write results *into*, closing the loop from replayed transaction to analyst-facing alert. v1.1 adds three pieces per `docs/tickets/06-08`: a core-banking database, a scorer consumer loop, and a fraud-ops dashboard.

## Decisions

1. **Bank system-of-record: Azure SQL Edge, not full SQL Server / Postgres / Azure SQL.**
   - Same SQL Server T-SQL surface and wire protocol as production Azure SQL, so the schema/queries are directly portable to a managed database later (see ADR decision below on the optional Terraform demo module, which does exactly that).
   - **ARM-native** — runs natively on Apple Silicon under Colima without emulation, unlike the full `mssql-server` Linux image which requires `--platform linux/amd64` and pays a Rosetta/QEMU tax on every query.
   - **$0 local cost** — a container, not a managed service; matches the "local dev must be free" constraint that already shaped ADR 0001 (Redpanda over cloud Event Hubs for local dev).
   - Rejected full SQL Server: heavier image, x86-only performance tax on ARM dev machines, no functional gain for this project's schema (five tables, no SQL Server-specific features used).
   - Rejected Postgres: would be lighter and faster, but the whole point of this piece is to demonstrate a *bank-realistic* system-of-record — card-issuer core banking systems are overwhelmingly SQL Server/Oracle/DB2 shops, and staying in the SQL Server family keeps the schema/migration story honest for a portfolio aimed at that industry.
   - Rejected Azure SQL (managed) for local dev: no local/offline equivalent, and it would reintroduce a live-cloud dependency for a `make demo` that must work from a clean clone with only Docker installed. Azure SQL serverless *is* used for the optional cloud demo module (below) — the local/cloud split mirrors the Redpanda/Event Hubs split in ADR 0001.
   - Known trade-off, documented at the source: the Edge image ships no `sqlcmd`/`mssql-tools`, so the compose healthcheck is a plain TCP probe using bash's `/dev/tcp` pseudo-device (`bash -c 'exec 3<>/dev/tcp/127.0.0.1/1433'`), not a real query-level readiness check — see the `bank-db` service comment in `docker/docker-compose.yml`. This means "container is healthy" means "port 1433 is accepting TCP," not "schema is applied"; `init-bank`/`src/bank/seed.py`'s own SQLAlchemy `pool_pre_ping` retry (not the healthcheck) is what actually waits for the engine to be query-ready.

2. **A separate scorer consumer (`src/pipeline/scorer.py`), not scoring inside the Spark streaming job.**
   - **Latency isolation.** The Spark Structured Streaming job's job is windowed feature computation on a microbatch cadence; bolting a per-event HTTP call to the scoring API onto that job would couple the API's latency/availability directly to streaming throughput, and a slow/down API would back up the *feature* pipeline, not just scoring.
   - **The API stays the single scoring path — no skew.** If Spark scored transactions directly (e.g. loading the model in-process), that would be a *second* code path computing `fraud_probability`, independent of `src/app.py::/score`, with its own opportunity to drift from the measured-latency, Redis-joined, threshold-versioned path that's the actual portfolio artifact (ADR 0001 decision 4, train/serve skew). Routing every scored transaction through `/score` — the same endpoint the benchmark measures and the same endpoint an external caller would hit — means the dashboard's numbers and the README's benchmark numbers are provably the same code.
   - **Decoupled failure domains.** The scorer is a plain Kafka consumer (`group=scorer`) with its own retry/backoff on the HTTP call and idempotent SQL writes (event_id PK; duplicate-key on replay/rebalance is swallowed, not fatal) — it can fall behind or restart without taking Spark or the API down with it, and vice versa.
   - Rejected: scoring inside `features.py`'s `foreachBatch` — rejected for the skew and coupling reasons above. Rejected: making the API itself a Kafka consumer — rejected because the API's contract is a synchronous HTTP request/response used for both the live demo and the benchmark; turning it into a consumer would change what's being measured.

3. **Plotly Dash for the fraud-ops dashboard, not a JS frontend (React/Vue) or a BI tool (Grafana/Metabase/Superset).**
   - **Pure Python, code-only.** This is a DE portfolio project — every other surface (ingestion, features, training, API) is Python; a Dash app keeps the entire stack in one language and one review surface, with no separate frontend build step or Node toolchain.
   - **No CDN.** Dash bundles its JS/CSS assets and serves them from the Flask-based dev server itself, so the dashboard has no runtime dependency on an external CDN — it works fully offline/air-gapped, matching the "clean clone + Docker only" demo constraint.
   - Rejected Grafana/Metabase/Superset: would require standing up and configuring a second service with its own datasource config, dashboards-as-JSON, and auth model — heavier to run and less demonstrative of *this project's* code (the interesting logic here, like the alert Confirm/Dismiss actions writing back to `bank.fraud_alerts`, is application logic a BI tool doesn't model well).
   - Rejected a React/Vue SPA + separate API layer: correct choice for a real production fraud-ops console, but disproportionate engineering for a 3-minute recruiter demo; Dash's `dcc.Interval` polling is a fine substitute for a websocket-pushed UI at this scale (single demo viewer, ~2s refresh).

## Cost

| Component | Local (Docker Compose) | Azure (optional demo module) |
|---|---|---|
| Bank DB | Azure SQL Edge container — $0 | Azure SQL serverless, `GP_S_Gen5_1`, auto-pause after 60 min idle — ~$0.50–1.00/day while active, ~$0 while paused |
| Dashboard | Container — $0 | Container App, 0.5 vCPU / 1Gi, always-on (no scale-to-zero in this module) — ~$1.00–1.50/day |
| Scorer loop | Container — $0 | Runs alongside the existing pipeline image; no separate resource | 
| **Total** | **$0** | **~$1.50–2.50/day while the demo module is applied** |

The Azure demo module (`infra/terraform/demo.tf`) is gated on `var.enable_demo` (default `false`); a plain `terraform apply` never touches it. It is `validate`-only as of this ticket — nobody has run `terraform apply -var enable_demo=true` yet, and the orchestrator asks the user first before doing so (per `docs/STATE.md`'s cost rule), same teardown-tested discipline as the v1.0 Event Hubs/Container Apps deployment in ADR 0001.

## Consequences

- The bank DB's TCP-only healthcheck means compose's "healthy" status is necessary but not sufficient for query-readiness; `src/bank/seed.py` and the scorer's DB calls must (and do) retry on connection errors rather than assuming healthcheck-green implies schema-applied.
- Every scored transaction now costs one extra network hop (Kafka → scorer → HTTP → API → SQL) versus scoring in-line in Spark — acceptable because the demo's point is the live loop and analyst workflow, not raw throughput; the throughput/latency numbers that matter for the "DE differentiator" claim (README Key Results) are still measured directly against `/score`, unaffected by the scorer's presence.
- The dashboard's Prometheus-text parsing (`API_METRICS_URL`) is a histogram-bucket quantile *estimate*, not an exact quantile — acceptable for a live "is it healthy" tile, not a substitute for the benchmark script's real quantiles in the README.
- Card identity across the stream and the bank DB is joined *only* on `card_token` — no PAN, no raw `User`/`Card` index, ever reaches `bank.*`; see `docs/governance/tokenization-policy.md` for the join-key rule and `docs/governance/data-dictionary.md` for the `bank.*` field-level docs this ticket added.
