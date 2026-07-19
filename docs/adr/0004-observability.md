# ADR 0004 — v1.3 observability: Prometheus + Grafana-as-code + hand-rolled lag exporter

**Status:** accepted (2026-07-19). **Ticket:** `docs/tickets/13-observability.md`.

## Context

Through v1.2 the pipeline *measures* (the API's `/metrics` latency histogram, README
benchmark numbers, exact-count delivery verifications) but nothing *watches*: no time
series, no consumer-lag visibility, no alert thresholds. "How far behind is scoring
right now, and is it recovering?" required SQL against the bank DB. Consumer lag is the
flagship operational signal for a streaming data platform, and v1.2's delivery-semantics
work (commit-after-flush everywhere) is exactly the thing lag makes visible: kill the
scorer and committed offsets freeze while the high watermark keeps moving.

## Decision

1. **Prometheus + Grafana, fully provisioned as code** (compose profile `obs`, layered
   onto either demo mode via `make demo OBS=1` / `make demo-cdc OBS=1`). Datasource and
   dashboard are provisioning files + JSON in `docker/observability/`; `allowUiUpdates`
   is off, anonymous viewer is on. Nothing survives only in a Grafana volume — there is
   no Grafana volume. Scrape/eval interval is 5s so the kill-the-scorer demo beat is
   visible within one refresh.

2. **Hand-rolled lag exporter** (`src/pipeline/lag_exporter.py`) instead of a stock
   kafka-lag-exporter image, for two reasons:
   - The mechanics are the point of this project: lag is committed-offset vs
     high-watermark, two client API calls (`committed()` on a non-subscribing consumer
     constructed with the observed `group.id` — a read-only fetch that never joins the
     group — and `get_watermark_offsets()`). ~100 lines including the HTTP server, all
     unit-tested with mocks.
   - No stock exporter knows about the *other* half of this pipeline's freshness story:
     the bank DB. The same exporter emits `bank_rows_total{table}` and
     `scoring_staleness_seconds` (seconds since the newest `scored_at`), so one scrape
     target covers "is Kafka backing up" and "is anything actually landing in SQL".
   - Failure isolation: each backend (Kafka, bank DB) is polled independently;
     `lag_exporter_target_up{target}` says which side is dark instead of the whole
     exporter dying.

3. **Alert rules as code, no Alertmanager** (`docker/observability/alerts.yml`):
   `ConsumerLagHigh` (sum by group > 500 for 2m), `ScoringStale` (> 60s; NaN before
   first data never fires), `TargetDown`, `ExporterBackendDown`. Rules loading and
   firing in the Prometheus UI is enough for a local demo; **routing/paging
   (Alertmanager, OpsGenie/PagerDuty) is a deliberate, documented gap**, as is
   tracing/OTel and structured log shipping — this ADR covers metrics only.

## Consequences

- The demo talk track gains its strongest beat: `docker kill scorer` → the
  `kafka_consumergroup_lag{group="scorer"}` line climbs on the Grafana board while
  `scoring_staleness_seconds` rises; restart → lag drains visibly back to zero.
- The exporter's committed-offset read depends on consumer groups actually committing —
  true since v1.2, where every consumer commits synchronously after its flush.
- If the stack ever moves to Azure Event Hubs, `kafka_config()` already carries the
  SASL settings; the exporter inherits them unchanged. Grafana/Prometheus would move to
  Azure Managed Grafana / Monitor rather than these containers — the dashboard JSON and
  rules port, the compose wiring does not.
