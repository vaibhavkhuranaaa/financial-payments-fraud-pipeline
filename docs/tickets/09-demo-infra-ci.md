# Ticket 09 — One-command demo, optional Azure module, CI gate

**Owner:** Infra subagent. **Scope:** `Makefile`, `scripts/demo.sh`, `docker/docker-compose.yml` (wiring/profiles only — services owned by tickets 06–08), `infra/terraform/`, `.github/workflows/ci.yml`, `scripts/check.sh`. Do not touch `src/`.

## Context
- Read `CLAUDE.md`, `docs/STATE.md`, tickets 06–08 (what exists by now), current `Makefile`/`scripts/check.sh`, `infra/terraform/*.tf`, `docker/docker-compose.yml`.
- Gotchas already learned (STATE.md): Redpanda rpk has no `--set` flag — topics must be created explicitly post-start; `az containerapp update` needs `--container-name`; subscription already registered for `Microsoft.App`.
- COST RULE: terraform work is `validate`-only. NO `apply` in this ticket — the orchestrator asks the user first (~$2–3 for a deploy+teardown test).

## Deliverables
1. **`make demo`** → `scripts/demo.sh`: idempotent single command: compose up core + `demo` profile (build as needed) → wait on healthchecks → `rpk topic create transactions transactions.dlq` (idempotent, ignore exists) → init-bank seed → start replay producer + scorer → print dashboard/API URLs and a stop hint. `make demo-down` tears it all down (incl. volumes flag documented). Works from a clean clone with only Docker + data sample present.
2. **Terraform optional module** (e.g. `infra/terraform/demo.tf` behind `var.enable_demo` default false): Azure SQL serverless (auto-pause 60 min, minimum vCore) + dashboard Container App on the existing environment/ACR + firewall allowing the Container Apps env. Outputs: dashboard FQDN, SQL FQDN. `terraform validate` must pass with the flag both false and true (`-var enable_demo=true` plan-level validation only).
3. **CI**: extend the unified gate to cover new tests (they're plain pytest — should be automatic), add dashboard/scorer/bank modules to lint scope, keep `terraform validate` step covering the new module, compose config validation still green with new services/profiles.
4. Update `scripts/check.sh` if module scoping is explicit anywhere.

## Acceptance
- Fresh clone + `make demo`: dashboard live with flowing alerts, zero manual steps, within ~3 min on a warm image cache.
- `make demo` re-run while up: no errors (idempotent). `make demo-down` leaves nothing running.
- `make check` green including `terraform validate` for the demo module.
