#!/usr/bin/env bash
#
# demo.sh — one-command recruiter demo: brings up the full local stack
# (core streaming path + bank DB + scorer loop + dashboard), makes sure the
# Kafka topics exist, seeds the bank DB, and starts ingestion so the
# dashboard goes live with zero manual steps.
#
# Two ingest modes, selected by CDC=0|1 (default 0):
#   CDC=0 (default): v1.0 replay — the TabFormer CSV is replayed straight
#     onto the `transactions` Kafka topic by the `producer` service.
#   CDC=1 (`make demo-cdc`): v1.2 CDC — the CSV is instead written INTO
#     bank.card_transactions (system of record), Debezium captures the
#     change feed onto `bankdb.frauddemo.bank.card_transactions`, and
#     `cdc-transformer` maps it back onto contract-v1 `transactions` so the
#     scorer/dashboard/API downstream are unchanged between modes.
#
# Idempotent: safe to re-run while the stack is already up (docker compose
# no-ops on unchanged services; topic creation ignores "already exists";
# init-bank's seed is itself idempotent — see src/bank/seed.py).
#
# Usage: bash scripts/demo.sh            (replay mode, or `make demo`)
#        CDC=1 bash scripts/demo.sh      (CDC mode, or `make demo-cdc`)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

COMPOSE_FILE="docker/docker-compose.yml"
COMPOSE=(docker compose -f "$COMPOSE_FILE")
WAIT_TIMEOUT="${DEMO_WAIT_TIMEOUT:-180}"
CDC="${CDC:-0}"
OBS="${OBS:-0}"

# OBS=1 layers the observability profile (ticket 13) onto either mode:
# Prometheus + Grafana + lag exporter. Naming the services explicitly
# activates their "obs" profile automatically (same mechanism the demo
# already relies on for bank-db's "demo" profile).
OBS_SERVICES=()
if [[ "$OBS" == "1" ]]; then
  OBS_SERVICES=(lag-exporter prometheus grafana)
fi

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
topics=(transactions transactions.dlq)
if [[ "$CDC" == "1" ]]; then
  # schema-history.bankdb and Connect's internal _connect_* topics are
  # created by Kafka Connect / Debezium themselves — leave those alone.
  topics+=(bankdb.frauddemo.bank.card_transactions)
fi
for topic in "${topics[@]}"; do
  out="$(docker exec redpanda rpk topic create "$topic" 2>&1)" && rc=0 || rc=$?
  if [[ "$rc" -ne 0 ]] && ! grep -q "ALREADY_EXISTS" <<<"$out"; then
    echo "$out" >&2
    echo "!! failed to create topic '$topic'" >&2
    exit 1
  fi
done
echo "    topics ready: ${topics[*]}"

if [[ "$CDC" == "1" ]]; then
  echo "==> [3/4] seeding bank DB + starting CDC pipeline + scorer, dashboard"
  # init-bank/init-cdc are one-shot and sequenced by their own depends_on
  # graph within this one `up`. No `producer` here — the CSV never touches
  # Kafka directly in CDC mode: txn-writer INSERTs into
  # bank.card_transactions and cdc-streamer emits the change feed
  # (Debezium-shaped; see ADR 0003 for why not Debezium itself on SQL Edge).
  # ${arr[@]+...} guard: macOS ships bash 3.2, where expanding an empty
  # array under `set -u` is an unbound-variable error.
  "${COMPOSE[@]}" up -d --build --wait --wait-timeout "$WAIT_TIMEOUT" \
    init-bank init-cdc cdc-scan cdc-streamer txn-writer cdc-transformer scorer dashboard \
    ${OBS_SERVICES[@]+"${OBS_SERVICES[@]}"}
else
  echo "==> [3/4] seeding bank DB + starting scorer, dashboard, replay producer"
  # init-bank is one-shot (depends_on bank-db healthy) and exits 0 once the
  # schema/seed apply — re-running it is a documented no-op (src/bank/seed.py
  # upserts deterministically). scorer depends_on init-bank completing
  # successfully, so compose sequences it automatically within this one `up`.
  "${COMPOSE[@]}" up -d --build --wait --wait-timeout "$WAIT_TIMEOUT" \
    init-bank scorer dashboard producer \
    ${OBS_SERVICES[@]+"${OBS_SERVICES[@]}"}
fi

# `compose up --wait` has been observed to exit 0 even when a one-shot
# dependency (init-bank) failed and its dependents were left in Created —
# so assert the long-running services are genuinely running before
# declaring victory (found live 2026-07-19).
critical=(scorer dashboard)
if [[ "$CDC" == "1" ]]; then
  critical+=(cdc-streamer cdc-transformer cdc-scan)
fi
if [[ "$OBS" == "1" ]]; then
  critical+=(lag-exporter prometheus grafana)
fi
for svc in "${critical[@]}"; do
  state="$(docker inspect -f '{{.State.Status}}' "$svc" 2>/dev/null || echo missing)"
  if [[ "$state" != "running" ]]; then
    echo "!! service '$svc' is '$state' (expected running) — demo did NOT come up cleanly" >&2
    echo "   check: docker logs $svc  (and docker logs init-bank for seed failures)" >&2
    exit 1
  fi
done

echo "==> [4/4] demo is live"
DASHBOARD_PORT="${DASHBOARD_PORT:-8050}"
if [[ "$CDC" == "1" ]]; then
  cat <<EOF

  Mode      : CDC (bank.card_transactions change feed -> Kafka -> scorer)
  Dashboard : http://localhost:${DASHBOARD_PORT}
  API       : http://localhost:8000/healthz
  Metrics   : http://localhost:8000/metrics

Transactions are being written into bank.card_transactions, streamed off
its CDC change table, and scored continuously; give the dashboard ~10-20s
to show its first live data.

Stop everything with:  make demo-down
EOF
else
  cat <<EOF

  Dashboard : http://localhost:${DASHBOARD_PORT}
  API       : http://localhost:8000/healthz
  Metrics   : http://localhost:8000/metrics

Transactions are replaying onto Kafka and being scored continuously;
give the dashboard ~10-20s to show its first live data.

Stop everything with:  make demo-down
EOF
fi

if [[ "$OBS" == "1" ]]; then
  cat <<EOF
Observability (OBS=1):
  Grafana    : http://localhost:3000/d/fraud-ops  (anonymous viewer, provisioned as code)
  Prometheus : http://localhost:9090  (/targets for scrape health, /alerts for rules)
  Exporter   : http://localhost:9105/metrics  (consumer lag + bank freshness)
EOF
fi
