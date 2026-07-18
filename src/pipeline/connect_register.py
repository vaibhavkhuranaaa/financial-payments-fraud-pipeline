"""Idempotent registrar for the Debezium SQL Server source connector (ticket 11, v1.2).

Data flow
---------
1. Read the checked-in connector config template
   (``docker/connect/bankdb-source.json`` by default) — a flat JSON object
   that is the exact body Kafka Connect's REST API expects for
   ``PUT /connectors/bankdb-source/config``.
2. ``substitute_env`` replaces any ``${ENV_VAR}`` placeholder value (e.g.
   ``database.password``) with the corresponding environment variable,
   failing loud if a referenced variable is unset — secrets never live in
   the checked-in file.
3. Wait for Kafka Connect's REST API (``CONNECT_URL``, default
   ``http://connect:8083``) to answer, then ``PUT`` the substituted config to
   ``/connectors/bankdb-source/config``. PUT is create-or-update, so re-running
   this registrar (e.g. on every ``connect-init`` restart) is idempotent.
4. Poll ``/connectors/bankdb-source/status`` briefly and print the
   connector/task state so a broken connector is visible in compose logs
   immediately, without requiring a separate `curl`.

Testability
-----------
``substitute_env`` and ``register`` are pure/injectable: `register` takes a
``requests``-module-shaped object (or a real `requests` session) so tests can
mock the HTTP calls with no live Kafka Connect required.

CLI: ``python -m src.pipeline.connect_register [--config PATH]``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from collections.abc import Mapping
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_DEFAULT_CONFIG_PATH = os.path.join(_REPO_ROOT, "docker", "connect", "bankdb-source.json")

CONNECTOR_NAME = "bankdb-source"

# --- Env-driven configuration (defaults mirror .env.example) ---------------

CONNECT_URL = os.environ.get("CONNECT_URL", "http://connect:8083").rstrip("/")

CONNECT_WAIT_TIMEOUT_S = 120.0
CONNECT_WAIT_POLL_INTERVAL_S = 3.0
STATUS_POLL_ATTEMPTS = 5
STATUS_POLL_INTERVAL_S = 2.0

_ENV_VAR_PATTERN = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def substitute_env(config: dict[str, Any], env: Mapping[str, str]) -> dict[str, Any]:
    """Replace ``${ENV_VAR}`` string values in `config` with `env[ENV_VAR]`.

    Only exact-match placeholder values (e.g. ``"${BANK_DB_PASSWORD}"``) are
    substituted — non-placeholder strings pass through unchanged. Raises
    ``KeyError`` if a referenced env var is unset, so a missing secret fails
    the registrar loudly instead of registering a connector with a literal
    ``${...}`` password.
    """
    resolved: dict[str, Any] = {}
    for key, value in config.items():
        if isinstance(value, str):
            match = _ENV_VAR_PATTERN.match(value)
            if match:
                var_name = match.group(1)
                if var_name not in env:
                    raise KeyError(f"env var '{var_name}' referenced by '{key}' is not set")
                resolved[key] = env[var_name]
                continue
        resolved[key] = value
    return resolved


def _wait_for_connect(session: requests.Session, base_url: str, timeout_s: float) -> None:
    """Poll `base_url` (Kafka Connect's REST root) until it answers, or raise
    TimeoutError after `timeout_s` seconds."""
    deadline = time.monotonic() + timeout_s
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            response = session.get(base_url, timeout=5.0)
            if response.status_code == 200:
                return
            last_error = f"HTTP {response.status_code}"
        except requests.exceptions.RequestException as exc:
            last_error = str(exc)
        time.sleep(CONNECT_WAIT_POLL_INTERVAL_S)
    raise TimeoutError(f"Kafka Connect at {base_url} did not become ready within {timeout_s}s: {last_error}")


def _log_status(session: requests.Session, base_url: str, connector_name: str) -> None:
    """Best-effort: poll the connector's status a few times and print it.
    Never raises — a status-check failure shouldn't fail a successful PUT."""
    status_url = f"{base_url}/connectors/{connector_name}/status"
    for _ in range(STATUS_POLL_ATTEMPTS):
        try:
            response = session.get(status_url, timeout=5.0)
            if response.status_code == 200:
                body = response.json()
                connector_state = body.get("connector", {}).get("state", "UNKNOWN")
                task_states = [t.get("state", "UNKNOWN") for t in body.get("tasks", [])]
                print(f"connector '{connector_name}' state={connector_state} tasks={task_states}")
                return
        except requests.exceptions.RequestException as exc:
            logger.debug("status poll failed: %s", exc)
        time.sleep(STATUS_POLL_INTERVAL_S)
    print(f"connector '{connector_name}' status unavailable after {STATUS_POLL_ATTEMPTS} attempts")


def register(
    config: dict[str, Any],
    connect_url: str,
    session: requests.Session,
    connector_name: str = CONNECTOR_NAME,
    wait_timeout_s: float = CONNECT_WAIT_TIMEOUT_S,
) -> None:
    """Wait for Connect to be reachable, then PUT `config` to
    `/connectors/{connector_name}/config` (create-or-update, idempotent).
    Raises on any non-2xx response so the caller (main) can exit non-zero."""
    _wait_for_connect(session, connect_url, wait_timeout_s)

    put_url = f"{connect_url}/connectors/{connector_name}/config"
    response = session.put(put_url, json=config, timeout=30.0)
    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"failed to register connector '{connector_name}': HTTP {response.status_code}: {response.text[:500]}"
        )
    print(f"registered connector '{connector_name}' (HTTP {response.status_code})")

    _log_status(session, connect_url, connector_name)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Idempotently register the Debezium SQL Server source connector."
    )
    parser.add_argument(
        "--config", default=_DEFAULT_CONFIG_PATH, help="Path to the connector config JSON template."
    )
    args = parser.parse_args(argv)

    with open(args.config, encoding="utf-8") as f:
        template = json.load(f)

    try:
        config = substitute_env(template, os.environ)
    except KeyError as exc:
        print(f"connector config error: {exc}", file=sys.stderr)
        sys.exit(1)

    session = requests.Session()
    try:
        register(config, CONNECT_URL, session)
    except (TimeoutError, RuntimeError) as exc:
        print(f"connector registration failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
