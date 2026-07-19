# Ticket 16 — v1.5a: Load/soak testing (roadmap item 4)

**Scope:** `scripts/loadtest.py` (or k6 script — decide by what's installable without global installs; a Python/locust-free asyncio+httpx closed-loop generator is acceptable and dependency-light), README Key Results additions. Local only, $0.

## Deliverables
1. **`/score` load test**: sustained closed-loop load at fixed concurrency levels (e.g. 4/16/64 workers, 60s each) against the compose API using contract-v1 events sampled from `data/sample/transactions_sample.csv`. Report req/s + p50/p95/p99 per level (client-side), plus server-side p99 from `/metrics` for the same window. Reuse/extend `scripts/benchmark.py` if it's already close — read it first; don't build a second benchmarker if one flag suffices.
2. **End-to-end lag under load**: while the replay producer runs at elevated `PRODUCER_EVENTS_PER_SEC` (e.g. 500, 1000), record from the lag exporter: peak `kafka_consumergroup_lag{group="scorer"}`, time-to-drain after producer stops, and `scoring_staleness_seconds` behavior. This gives the README a backpressure story: the events/s at which the scorer stops keeping up on a laptop, and what the recovery curve looks like.
3. **README "Key Results"**: new subsection with a small table (concurrency → req/s, p50/p95/p99) and a sentence on the saturation point + drain rate. Numbers must be real measured output, honestly caveated as laptop-local.
4. **Makefile:** `make loadtest` (requires demo up; fail with a clear message if API unreachable).

## Acceptance
- `make loadtest` runs end-to-end against a live `make demo OBS=1` stack and prints the table.
- Numbers land in README; `make check` green; no new heavyweight deps (httpx or stdlib only if scripting in Python).
- Everything torn down afterward.
