.PHONY: check lint test dbt-build tf-validate compose-validate fmt demo demo-down demo-down-volumes

VENV := .venv/bin
COMPOSE_FILE := docker/docker-compose.yml

check: lint test dbt-build tf-validate compose-validate
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
	@if [ -f docker/docker-compose.yml ] && grep -q "services:" docker/docker-compose.yml; then docker compose -f docker/docker-compose.yml config -q ; else echo "compose not present yet — skipping"; fi

# One-command recruiter demo: core stack + bank DB + scorer loop + dashboard
# + replay producer, topics created idempotently. See scripts/demo.sh.
demo:
	bash scripts/demo.sh

# Tear down the demo stack (containers only — named volumes, e.g. the bank
# DB's data, persist so re-running `make demo` is fast on a warm cache).
demo-down:
	docker compose -f $(COMPOSE_FILE) --profile demo --profile replay down

# Same as demo-down but also drops named volumes (bank-db-data) for a fully
# clean-state re-test.
demo-down-volumes:
	docker compose -f $(COMPOSE_FILE) --profile demo --profile replay down -v
