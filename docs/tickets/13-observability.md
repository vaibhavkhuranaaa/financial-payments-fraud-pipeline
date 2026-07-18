# Ticket 13 — v1.3: Observability stack (Prometheus + Grafana + consumer lag)

**Owner:** Pipeline/Infra subagents (v1.3). **Scope:** `docker/` (prometheus, grafana, lag-exporter services + provisioning-as-code), `src/pipeline/lag_exporter.py`, docs. Roadmap item 3 in `docs/HANDOFF.md` (items 1–2 of the original list: delivery semantics shipped in v1.2; schema registry deliberately deferred — lag/latency visibility is the stronger DE signal and builds on `/metrics` that already exists).

## Why (the v1.3 headline)
The pipeline already *measures* (api `/metrics` latency quantiles, README benchmarks) but nothing *watches*: no time series, no consumer-lag visibility, no alert thresholds. Consumer lag is **the** flagship data-engineering operational signal — "how far behind is scoring right now, and is it recovering?" — and a Grafana board over Prometheus is the lingua franca every platform/DE team expects. Interview arc: "v1.2 made delivery semantics explicit; v1.3 made them observable — here's the lag graph recovering after I kill the scorer live."

## Design sketch
1. **Prometheus** (`prom/prometheus` image, compose profile `obs`): config-as-code at `docker/observability/prometheus.yml`; scrapes (a) the existing Flask `/metrics` (already Prometheus text format — `src/app.py` uses `prometheus_client.generate_latest()`, verified), (b) the lag exporter.
2. **Lag exporter** (`src/pipeline/lag_exporter.py`, pipeline image): tiny HTTP server exposing Prometheus text format; polls Kafka admin API (confluent_kafka AdminClient/Consumer committed-vs-end offsets) for groups `scorer`, `cdc-transformer` × their topics, and the bank DB for `scored_transactions`/`fraud_alerts`/`card_transactions` counts + max `scored_at` staleness. Metrics: `kafka_consumergroup_lag{group,topic}`, `bank_rows_total{table}`, `scoring_staleness_seconds`. No third-party exporter image — the point is showing the mechanics, and Redpanda's own exporter doesn't know about the bank DB.
3. **Grafana** (`grafana/grafana-oss`, profile `obs`): fully provisioned-as-code (`docker/observability/grafana/provisioning/…` datasource + dashboard JSON) — zero clickops; anonymous viewer enabled for the demo. One board: scoring throughput, consumer lag per group, p50/p95/p99 score latency, alert rate, staleness.
4. **Alert rules as code**: Prometheus rule file (`docker/observability/alerts.yml`) with at least: lag > N for 2m, staleness > 60s, api down (scrape failure). No Alertmanager in v1.3 (rules visible/firing in Prometheus UI is enough for the demo; note the gap).
5. **Wiring**: `make demo OBS=1` / `make demo-cdc OBS=1` (or `make demo-obs`) adds the `obs` profile to either mode; `demo-down` tears it down. README + lineage note; ADR 0004 (why hand-rolled exporter, why no Alertmanager/OTel yet).

## Acceptance
- `make demo-cdc OBS=1` (and replay equivalent): Grafana on :3000 shows live throughput/lag/latency with zero manual steps; kill the scorer → lag climbs on the board; restart → visibly recovers.
- Prometheus targets page: all scrapes green; alert rules load; the lag alert fires during the kill test.
- Everything as code (no manual Grafana setup survives `demo-down-volumes`); `make check` green; new unit tests for the exporter's metric rendering + lag computation (mocked Kafka/DB).
