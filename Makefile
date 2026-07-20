.PHONY: check lint test dbt-build tf-validate compose-validate avro-schema-check fmt demo demo-cdc demo-down demo-down-volumes smoke loadtest

VENV := .venv/bin
COMPOSE_FILE := docker/docker-compose.yml
# Ticket 14: compose has no inline secret fallbacks left, so every invocation
# needs demo.env's values for interpolation (`${VAR:?msg}`), not just for the
# containers' own env.
COMPOSE_ENV_FILE := docker/demo.env

check: lint test dbt-build tf-validate compose-validate avro-schema-check
	@echo "✅ all checks passed — safe to push"

lint:
	$(VENV)/ruff check src tests scripts

fmt:
	$(VENV)/ruff format src tests scripts

test:
	$(VENV)/pytest tests -q

dbt-build:
	@if [ -d dbt ]; then cd dbt && ../$(VENV)/dbt build --profiles-dir . ; else echo "dbt/ not present yet — skipping"; fi

tf-validate:
	@if [ -d infra/terraform ]; then terraform -chdir=infra/terraform fmt -check -recursive && terraform -chdir=infra/terraform validate ; else echo "infra/terraform not present yet — skipping"; fi

compose-validate:
	@if [ -f docker/docker-compose.yml ] && grep -q "services:" docker/docker-compose.yml; then docker compose -f docker/docker-compose.yml --env-file $(COMPOSE_ENV_FILE) config -q ; else echo "compose not present yet — skipping"; fi

# Ticket 18 (ADR 0006): contracts/transaction.avsc is generated from
# contracts/transaction.schema.json — this fails if they've drifted.
avro-schema-check:
	$(VENV)/python scripts/gen_avro_schema.py --check

# Observability overlay toggle (ticket 13): `make demo OBS=1` /
# `make demo-cdc OBS=1` adds Prometheus + Grafana + the lag exporter to
# either mode. Passed explicitly because make command-line vars are not
# auto-exported to recipe environments.
OBS ?= 0

# One-command recruiter demo: core stack + bank DB + scorer loop + dashboard
# + replay producer, topics created idempotently. See scripts/demo.sh.
demo:
	OBS=$(OBS) bash scripts/demo.sh

# v1.2 CDC-mode demo: bank.card_transactions is the system of record,
# Debezium streams its change feed onto Kafka, cdc-transformer maps it back
# onto contract-v1 — see scripts/demo.sh.
demo-cdc:
	CDC=1 OBS=$(OBS) bash scripts/demo.sh

# Tear down the demo stack (containers only — named volumes, e.g. the bank
# DB's data, persist so re-running `make demo` is fast on a warm cache).
demo-down:
	docker compose -f $(COMPOSE_FILE) --env-file $(COMPOSE_ENV_FILE) --profile demo --profile replay --profile cdc --profile debezium --profile obs down

# Same as demo-down but also drops named volumes (bank-db-data) for a fully
# clean-state re-test.
demo-down-volumes:
	docker compose -f $(COMPOSE_FILE) --env-file $(COMPOSE_ENV_FILE) --profile demo --profile replay --profile cdc --profile debezium --profile obs down -v

# Ticket 15: bounded, assertion-driven E2E smoke test. Brings up the replay
# demo, asserts against the live stack (health, scoring, DB growth,
# dashboard, and — with OBS=1 — Prometheus/lag-exporter), then always tears
# down via demo-down-volumes (trap on EXIT, pass or fail). See
# scripts/smoke.sh. This is the merge gate for ticket 15 — run it live.
smoke:
	SMOKE_OBS=$(OBS) bash scripts/smoke.sh

# Ticket 16: closed-loop load ladder against a LIVE demo stack's /score
# (bring one up first: `make demo` / `make demo OBS=1`). Results land in
# README "Load & backpressure". Fails fast if the API isn't reachable.
loadtest:
	@curl -sf -o /dev/null http://localhost:8000/healthz || { echo "!! api not reachable on :8000 — run 'make demo' first"; exit 1; }
	@for c in 4 16 64; do \
		$(VENV)/python scripts/benchmark.py --n $$((c*500)) --concurrency $$c --url http://localhost:8000/score; \
	done
