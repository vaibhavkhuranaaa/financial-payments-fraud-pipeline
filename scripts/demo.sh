#!/usr/bin/env bash
#
# demo.sh — one-command recruiter demo: brings up the full local stack
# (core streaming path + bank DB + scorer loop + dashboard), makes sure the
# Kafka topics exist, seeds the bank DB, and starts the replay producer so
# the dashboard goes live with zero manual steps.
#
# Idempotent: safe to re-run while the stack is already up (docker compose
# no-ops on unchanged services; topic creation ignores "already exists";
# init-bank's seed is itself idempotent — see src/bank/seed.py).
#
# Usage: bash scripts/demo.sh   (or `make demo`)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

COMPOSE_FILE="docker/docker-compose.yml"
COMPOSE=(docker compose -f "$COMPOSE_FILE")
WAIT_TIMEOUT="${DEMO_WAIT_TIMEOUT:-180}"

echo "==> [1/4] starting core services + bank DB (build as needed)"
# Named explicitly (not via --profile) so this works regardless of which
# profiles are active; compose still resolves each service's own
# depends_on graph (e.g. api waits on redis, spark-features waits on
# redpanda+redis).
"${COMPOSE[@]}" up -d --build --wait --wait-timeout "$WAIT_TIMEOUT" \
  redpanda redis bank-db api spark-features

echo "==> [2/4] ensuring Kafka topics exist (idempotent)"
# rpk has no --set/--if-not-exists flag, and exits 1 with
# TOPIC_ALREADY_EXISTS on a re-run — treat that one failure mode as
# success, anything else is a real error.
for topic in transactions transactions.dlq; do
  out="$(docker exec redpanda rpk topic create "$topic" 2>&1)" && rc=0 || rc=$?
  if [[ "$rc" -ne 0 ]] && ! grep -q "ALREADY_EXISTS" <<<"$out"; then
    echo "$out" >&2
    echo "!! failed to create topic '$topic'" >&2
    exit 1
  fi
done
echo "    topics ready: transactions, transactions.dlq"

echo "==> [3/4] seeding bank DB + starting scorer, dashboard, replay producer"
# init-bank is one-shot (depends_on bank-db healthy) and exits 0 once the
# schema/seed apply — re-running it is a documented no-op (src/bank/seed.py
# upserts deterministically). scorer depends_on init-bank completing
# successfully, so compose sequences it automatically within this one `up`.
"${COMPOSE[@]}" up -d --build --wait --wait-timeout "$WAIT_TIMEOUT" \
  init-bank scorer dashboard producer

echo "==> [4/4] demo is live"
DASHBOARD_PORT="${DASHBOARD_PORT:-8050}"
cat <<EOF

  Dashboard : http://localhost:${DASHBOARD_PORT}
  API       : http://localhost:8000/healthz
  Metrics   : http://localhost:8000/metrics

Transactions are replaying onto Kafka and being scored continuously;
give the dashboard ~10-20s to show its first live data.

Stop everything with:  make demo-down
EOF
