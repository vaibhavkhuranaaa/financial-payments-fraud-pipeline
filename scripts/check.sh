#!/usr/bin/env bash
#
# check.sh — mirrors `make check`: lint, tests, dbt build (if present),
# terraform validate (if present), compose config validation (if present).
#
# Runs both locally (prefers .venv/bin if it exists) and in CI (falls back
# to PATH — setup-python/pip install puts ruff/pytest there).
#
# Usage: bash scripts/check.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ -x "$REPO_ROOT/.venv/bin/ruff" ]]; then
  RUFF="$REPO_ROOT/.venv/bin/ruff"
else
  RUFF="ruff"
fi

if [[ -x "$REPO_ROOT/.venv/bin/pytest" ]]; then
  PYTEST="$REPO_ROOT/.venv/bin/pytest"
else
  PYTEST="pytest"
fi

if [[ -x "$REPO_ROOT/.venv/bin/dbt" ]]; then
  DBT="$REPO_ROOT/.venv/bin/dbt"
else
  DBT="dbt"
fi

echo "==> lint (ruff check)"
"$RUFF" check src tests scripts

echo "==> test (pytest)"
"$PYTEST" tests -q

echo "==> dbt build"
if [[ -d "$REPO_ROOT/dbt" ]]; then
  (cd "$REPO_ROOT/dbt" && "$DBT" build --profiles-dir .)
else
  echo "dbt/ not present yet — skipping"
fi

echo "==> terraform validate"
if [[ -d "$REPO_ROOT/infra/terraform" ]]; then
  terraform -chdir="$REPO_ROOT/infra/terraform" fmt -check -recursive
  terraform -chdir="$REPO_ROOT/infra/terraform" init -backend=false -input=false >/dev/null
  terraform -chdir="$REPO_ROOT/infra/terraform" validate
else
  echo "infra/terraform not present yet — skipping"
fi

echo "==> compose config"
if [[ -f "$REPO_ROOT/docker/docker-compose.yml" ]] && grep -q "services:" "$REPO_ROOT/docker/docker-compose.yml"; then
  docker compose -f "$REPO_ROOT/docker/docker-compose.yml" config -q
else
  echo "compose not present yet — skipping"
fi

echo "all checks passed — safe to push"
