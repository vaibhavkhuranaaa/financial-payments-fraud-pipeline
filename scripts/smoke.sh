#!/usr/bin/env bash
#
# smoke.sh — bounded, assertion-driven E2E smoke test (ticket 15) for the
# replay demo (`scripts/demo.sh`). Brings the stack up, asserts real exit
# codes against the live services, then ALWAYS tears the stack down (trap on
# EXIT, pass or fail) via `make demo-down-volumes` so nothing is left running.
#
# Asserts:
#   1. GET /healthz — 200 if a model is committed/present at models/model.json,
#      else 503 is the documented (not a failure) cold-start response — see
#      "model artifact" notice below.
#   2. POST /score with one valid contract-v1 event — 200 iff a model is
#      present; skipped with an explicit notice otherwise (training in CI is
#      out of scope for this smoke test — see docs/tickets/15-ci-e2e-smoke.md).
#   3. bank.scored_transactions row count strictly increasing over a ~20s
#      window (queried via a one-off `docker compose run` against the
#      pipeline image — it has sqlalchemy/pymssql; host python does not).
#      Skipped (with notice) under the no-model condition above, since the
#      scorer can't write rows if /score is 503ing every event.
#   4. GET / on the dashboard — 200.
#   5. SMOKE_OBS=1 only: every Prometheus target healthy, and
#      `kafka_consumergroup_lag` present in the lag-exporter's own
#      /metrics output.
#
# Bounded throughout: every wait loop below has an explicit deadline; nothing
# can hang forever. scripts/demo.sh itself is bounded by DEMO_WAIT_TIMEOUT.
#
# Usage: bash scripts/smoke.sh              (replay mode, or `make smoke`)
#        SMOKE_OBS=1 bash scripts/smoke.sh  (+ observability asserts)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

COMPOSE_FILE="docker/docker-compose.yml"
COMPOSE=(docker compose -f "$COMPOSE_FILE" --env-file docker/demo.env)

SMOKE_OBS="${SMOKE_OBS:-0}"
API_URL="${SMOKE_API_URL:-http://localhost:8000}"
DASHBOARD_PORT="${DASHBOARD_PORT:-8050}"
DASHBOARD_URL="${SMOKE_DASHBOARD_URL:-http://localhost:${DASHBOARD_PORT}}"
PROMETHEUS_URL="${SMOKE_PROMETHEUS_URL:-http://localhost:9090}"
LAG_EXPORTER_URL="${SMOKE_LAG_EXPORTER_URL:-http://localhost:9105}"

POLL_INTERVAL_S="${SMOKE_POLL_INTERVAL_S:-2}"
HTTP_DEADLINE_S="${SMOKE_HTTP_DEADLINE_S:-90}"
DB_GROWTH_WINDOW_S="${SMOKE_DB_GROWTH_WINDOW_S:-20}"
DB_QUERY_TIMEOUT_S="${SMOKE_DB_QUERY_TIMEOUT_S:-30}"

# --- teardown, always runs (pass or fail) -----------------------------------

RESULT="FAIL"
teardown() {
  local ec=$?
  echo "==> tearing down (make demo-down-volumes)"
  make demo-down-volumes || echo "!! teardown itself reported a non-zero exit" >&2
  if [[ "$RESULT" == "PASS" ]]; then
    echo "PASS: e2e smoke"
    exit 0
  fi
  echo "FAIL: e2e smoke"
  exit "$([[ "$ec" -ne 0 ]] && echo "$ec" || echo 1)"
}
trap teardown EXIT

fail() {
  echo "!! FAIL: $*" >&2
  exit 1
}

# --- helpers -----------------------------------------------------------------

# with_timeout SECONDS CMD... — GNU `timeout` when available (Linux/CI, or
# brew coreutils' gtimeout); stock macOS ships neither, so fall back to
# running unwrapped there (the wrapped commands are one-off `compose run`
# queries that terminate on their own; the timeout is belt-and-braces for CI).
with_timeout() {
  local secs="$1"; shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "$secs" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$secs" "$@"
  else
    "$@"
  fi
}

# wait_for_http URL EXPECTED_CODE DEADLINE_S
wait_for_http() {
  local url="$1" expected="$2" deadline="$3" waited=0 code
  while true; do
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$url" 2>/dev/null || echo "000")"
    if [[ "$code" == "$expected" ]]; then
      return 0
    fi
    if (( waited >= deadline )); then
      echo "!! $url never returned $expected (last: $code) within ${deadline}s" >&2
      return 1
    fi
    sleep "$POLL_INTERVAL_S"
    waited=$((waited + POLL_INTERVAL_S))
  done
}

# scored_txn_count: one-off python against the pipeline image (has
# sqlalchemy/pymssql) using the scorer service's own env (BANK_DB_* +
# --no-deps so it doesn't try to restart bank-db/api). Never host python —
# the host has no SQL Server driver installed. --entrypoint python is
# required: the image's ENTRYPOINT is `python -m` (module runner), which
# would mangle an inline `-c` script.
scored_txn_count() {
  with_timeout "${DB_QUERY_TIMEOUT_S}" "${COMPOSE[@]}" run --rm --no-deps --entrypoint python scorer -c '
from sqlalchemy import text
from src.bank.db import get_engine
with get_engine().connect() as conn:
    print(conn.execute(text("SELECT COUNT(*) FROM bank.scored_transactions")).scalar())
' 2>/dev/null | tr -d "\r" | tail -n 1
}

echo "==> [1/5] bringing up the demo stack (scripts/demo.sh)"
if ! OBS="$SMOKE_OBS" bash scripts/demo.sh; then
  fail "scripts/demo.sh exited non-zero — demo did not come up cleanly"
fi

echo "==> [2/5] model artifact check"
MODEL_PRESENT=0
if [[ -f "$REPO_ROOT/models/model.json" ]]; then
  MODEL_PRESENT=1
  echo "    models/model.json present — full scoring asserts will run"
else
  echo "    NOTICE: no models/model.json on disk (nothing committed to models/"
  echo "    in this repo — training in CI is out of scope, see ticket 15)."
  echo "    /healthz is expected to report 503/model_loaded=false (a valid"
  echo "    cold-start response, not a crash); /score and the DB-growth"
  echo "    assert are SKIPPED below rather than failed."
fi

echo "==> [3/5] asserting /healthz + /score"
if [[ "$MODEL_PRESENT" == "1" ]]; then
  wait_for_http "${API_URL}/healthz" 200 "$HTTP_DEADLINE_S" || fail "/healthz did not return 200 within ${HTTP_DEADLINE_S}s"
  echo "    /healthz 200 OK"

  EVENT_JSON="$(python3 -c '
import json, uuid
from datetime import datetime, timezone
print(json.dumps({
    "schema_version": "1.0.0",
    "event_id": str(uuid.uuid4()),
    "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "card_token": "a" * 64,
    "user_id": "19",
    "amount": 42.50,
    "currency": "USD",
    "channel": "chip",
    "merchant_name": "smoke-test-merchant",
    "merchant_city": "Tucson",
    "merchant_state": "AZ",
    "merchant_country": "US",
    "zip": "85719",
    "mcc": 5411,
    "errors": None,
}))
')"
  SCORE_CODE="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -X POST \
    -H "Content-Type: application/json" -d "$EVENT_JSON" "${API_URL}/score" 2>/dev/null || echo "000")"
  [[ "$SCORE_CODE" == "200" ]] || fail "POST /score returned $SCORE_CODE, expected 200"
  echo "    POST /score 200 OK"
else
  wait_for_http "${API_URL}/healthz" 503 "$HTTP_DEADLINE_S" || fail "/healthz did not report the expected cold-start 503 within ${HTTP_DEADLINE_S}s"
  echo "    /healthz 503 (model_loaded=false) OK — cold-start path is a valid response"
  echo "    SKIP: POST /score (no model artifact — see notice above)"
fi

echo "==> [4/5] asserting bank.scored_transactions is growing"
if [[ "$MODEL_PRESENT" == "1" ]]; then
  COUNT_BEFORE="$(scored_txn_count)"
  [[ "$COUNT_BEFORE" =~ ^[0-9]+$ ]] || fail "could not read bank.scored_transactions count (got: '$COUNT_BEFORE')"
  echo "    scored_transactions count at t=0: $COUNT_BEFORE"
  sleep "$DB_GROWTH_WINDOW_S"
  COUNT_AFTER="$(scored_txn_count)"
  [[ "$COUNT_AFTER" =~ ^[0-9]+$ ]] || fail "could not read bank.scored_transactions count on second read (got: '$COUNT_AFTER')"
  echo "    scored_transactions count at t=${DB_GROWTH_WINDOW_S}s: $COUNT_AFTER"
  (( COUNT_AFTER > COUNT_BEFORE )) || fail "bank.scored_transactions did not strictly increase ($COUNT_BEFORE -> $COUNT_AFTER) over ${DB_GROWTH_WINDOW_S}s"
  echo "    scored_transactions strictly increasing OK"
else
  echo "    SKIP: bank.scored_transactions growth (no model artifact — scorer can't write rows while /score 503s)"
fi

echo "==> [5/5] asserting dashboard is live"
wait_for_http "${DASHBOARD_URL}/" 200 "$HTTP_DEADLINE_S" || fail "dashboard did not return 200 within ${HTTP_DEADLINE_S}s"
echo "    dashboard 200 OK"

if [[ "$SMOKE_OBS" == "1" ]]; then
  echo "==> [obs] asserting Prometheus targets + kafka_consumergroup_lag"
  wait_for_http "${PROMETHEUS_URL}/-/ready" 200 "$HTTP_DEADLINE_S" || fail "prometheus did not become ready within ${HTTP_DEADLINE_S}s"

  waited=0
  while true; do
    UNHEALTHY="$(curl -s --max-time 5 "${PROMETHEUS_URL}/api/v1/targets" 2>/dev/null | python3 -c '
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    print("PARSE_ERROR")
    sys.exit(0)
active = data.get("data", {}).get("activeTargets", [])
if not active:
    print("NO_TARGETS")
    sys.exit(0)
bad = [t.get("scrapeUrl") for t in active if t.get("health") != "up"]
print(",".join(bad))
' 2>/dev/null || echo "PARSE_ERROR")"
    if [[ -z "$UNHEALTHY" ]]; then
      break
    fi
    if (( waited >= HTTP_DEADLINE_S )); then
      fail "prometheus targets not all healthy within ${HTTP_DEADLINE_S}s (unhealthy/pending: $UNHEALTHY)"
    fi
    sleep "$POLL_INTERVAL_S"
    waited=$((waited + POLL_INTERVAL_S))
  done
  echo "    all prometheus targets healthy"

  wait_for_http "${LAG_EXPORTER_URL}/metrics" 200 "$HTTP_DEADLINE_S" || fail "lag-exporter /metrics did not return 200 within ${HTTP_DEADLINE_S}s"
  curl -s --max-time 5 "${LAG_EXPORTER_URL}/metrics" | grep -q "^kafka_consumergroup_lag" \
    || fail "kafka_consumergroup_lag not present in lag-exporter /metrics output"
  echo "    kafka_consumergroup_lag present OK"
fi

RESULT="PASS"
