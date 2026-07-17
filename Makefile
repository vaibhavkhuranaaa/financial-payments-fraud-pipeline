.PHONY: check lint test dbt-build tf-validate compose-validate fmt

VENV := .venv/bin

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
