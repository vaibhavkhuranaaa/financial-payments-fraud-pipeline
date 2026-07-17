# Ticket 03 — Infra: Docker Compose, Terraform (Azure), CI

**Owner:** Infra subagent. **Scope:** `docker/`, `infra/`, `.github/workflows/ci.yml`, `scripts/check.sh`. Do not touch `src/`, `tests/`, README, STATE.md, dbt/.

## Context
Read first: `CLAUDE.md`, `docs/adr/0001-stack-and-architecture.md`, `.env.example`, `Makefile`, `docs/tickets/01-pipeline.md` + `02-api.md` (they define the service CLIs you are containerizing — implementations may still be in flight; code against those contracts).

Service contracts:
- Producer: `python -m src.pipeline.ingestion --input data/sample/transactions_sample.csv` (env: KAFKA_*, PRODUCER_*, TOKENIZATION_SALT)
- Streaming job: `python -m src.pipeline.features --run-stream` (needs Spark + spark-sql-kafka-0-10 + delta JARs; env: KAFKA_*, REDIS_*, DELTA_ROOT)
- API: `gunicorn -w 2 -b 0.0.0.0:8000 src.app:app` (env: REDIS_*, MODEL_DIR; model artifacts mounted or baked)

## Deliverables

### 1. `docker/`
- `Dockerfile.api` — python:3.11-slim, non-root user, installs only api-needed deps (flask/gunicorn/xgboost/redis/jsonschema/pandas/prometheus-client), copies `src/` + `contracts/` + `models/`, HEALTHCHECK on /healthz, EXPOSE 8000.
- `Dockerfile.pipeline` — python:3.11-slim + OpenJDK 17 (temurin via apt eclipse-temurin or default-jre-headless), full requirements.txt; used by both producer and streaming job (different commands).
- `docker-compose.yml` (in `docker/`, paths relative to repo root via `context: ..`): services `redpanda` (redpandadata/redpanda:latest, single node, ports 19092 external/9092 internal, auto-create topics), `redis` (redis:7-alpine), `spark-features` (Dockerfile.pipeline, command run-stream, depends_on redpanda+redis, KAFKA_BOOTSTRAP_SERVERS=redpanda:9092), `producer` (Dockerfile.pipeline, depends_on redpanda, profiles: ["replay"] so it's opt-in), `api` (Dockerfile.api, port 8000, depends_on redis). Shared env via compose `environment:` mapping matching `.env.example` names; internal bootstrap `redpanda:9092`.
- Volume mounts: `../data:/app/data`, `../models:/app/models` so artifacts persist on host.

### 2. `infra/terraform/`
- `providers.tf` (azurerm ~> 3.100, required_version), `variables.tf` (prefix, location default eastus2, tags), `main.tf`:
  - resource group `${var.prefix}-rg`
  - Event Hubs namespace Standard (kafka enabled by default on Standard), event hub `transactions` (4 partitions, 1 day retention) + `transactions-dlq` (1 partition), authorization rule (send+listen) 
  - ACR (Basic), Log Analytics workspace
  - Container Apps environment + one container app for the API (ingress external 8000, min 1 max 2 replicas, ACR pull via managed identity)
- `outputs.tf`: eventhubs bootstrap server string, connection string (sensitive), ACR login server, API FQDN.
- `deploy.sh`: az acr build both images, terraform apply, then update container app image; `destroy.sh`: terraform destroy -auto-approve with confirmation prompt. Both `set -euo pipefail`, documented usage headers.
- `terraform fmt` + `terraform validate` must pass (init with `-backend=false`).

### 3. CI + check script
- `scripts/check.sh`: mirrors `make check` (ruff, pytest, dbt build if dbt/ exists, terraform fmt/validate if infra/terraform exists, compose config -q). Bash, set -euo pipefail, runnable both locally (uses .venv if present) and in CI (uses PATH).
- `.github/workflows/ci.yml`: on push/PR — setup python 3.11, pip install -r requirements.txt -r requirements-dev.txt, hashicorp/setup-terraform, run `scripts/check.sh`, then `docker build` both Dockerfiles (no push). Remove the old `|| true` lint job entirely.

## Acceptance criteria
- `docker compose -f docker/docker-compose.yml config -q` clean.
- `terraform -chdir=infra/terraform init -backend=false && terraform validate` clean, `fmt -check` clean.
- `bash scripts/check.sh` passes locally (skip-gracefully behavior for not-yet-present dirs preserved).
- Images NOT built in this ticket if src implementations are incomplete — compose/config validation is the gate; note it in the report.

## Verification commands
```bash
docker compose -f docker/docker-compose.yml config -q && echo compose-ok
terraform -chdir=infra/terraform init -backend=false -input=false && terraform -chdir=infra/terraform validate && terraform -chdir=infra/terraform fmt -check -recursive
bash scripts/check.sh || true   # report actual output
```

## Report back
Status; files changed; verbatim verification tails; est. monthly Azure cost of the terraform plan; anything skipped.
