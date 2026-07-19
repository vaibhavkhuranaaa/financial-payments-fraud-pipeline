# Ticket 15 — v1.4b: CI/CD depth — compose E2E smoke + pre-commit (roadmap item 6)

**Scope:** `.github/workflows/ci.yml`, `scripts/`, `.pre-commit-config.yaml`. Terraform remote state is OUT (needs Azure provisioning — hard stop); plan-on-PR stays out for the same reason.

## Deliverables
1. **`scripts/smoke.sh`** — a bounded, assertion-driven E2E: brings up the replay demo (`bash scripts/demo.sh` with `SCORER_MAX_EVENTS` bounded or a timeout), then asserts with real exit codes:
   - `/healthz` 200 and `/score` 200 with a valid contract-v1 event;
   - `bank.scored_transactions` count strictly increasing across a ~20s window (query via a one-off `docker compose run` python, not host DB deps);
   - dashboard HTTP 200;
   - with `OBS=1` (flag `SMOKE_OBS=1`): all Prometheus targets healthy and `kafka_consumergroup_lag` present in the exporter output.
   Must `make demo-down-volumes` in a trap on exit (pass or fail) and print a clear PASS/FAIL last line.
2. **CI job `e2e-smoke`** running it on ubuntu-latest (docker + compose are preinstalled on GH runners; ARM-vs-x86: azure-sql-edge has no healthy amd64 CI story if it fails — if the image won't run under amd64 emulation-free CI, gate the job on a `workflow_dispatch`/nightly schedule instead of every push, and document that in the workflow comments; try plain first). Model artifacts: training in CI is too heavy — smoke must work with the committed model files if present, else skip scoring-dependent asserts with an explicit notice (check what `models/` contains in-repo before deciding; if no model is committed, have the API's cold path still return a valid response or mark those asserts skipped).
3. **`.pre-commit-config.yaml`**: ruff (lint+format), trailing-whitespace/EOF fixers, `check-yaml`, `check-json`, and a local hook running `pytest -q -x tests` on push stage only. README one-liner: `pre-commit install`.
4. **Makefile:** `make smoke` target.

## Acceptance
- `make smoke` passes locally end-to-end on a clean state (this is the gate — run it live).
- `make check` green; workflow YAML well-formed.
- Smoke leaves nothing running afterward (`docker ps` empty) even when an assert fails.
- Honest CI: if e2e-smoke cannot run on GH-hosted runners (SQL Edge/amd64), the workflow says so in comments and runs where it can; no green-but-fake jobs.
