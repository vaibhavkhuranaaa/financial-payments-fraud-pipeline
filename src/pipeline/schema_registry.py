"""Shared schema-registry helpers for the `transactions` topic (ADR 0006).

Both producers of `transactions` (``ingestion.py``, ``cdc_transformer.py``)
and the consumer that follows (``scorer.py``) build their Avro
serializer/deserializer through this module instead of each wiring
``confluent_kafka.schema_registry`` directly, so the subject name, the
checked-in schema path, and registry-readiness/compat behavior stay in one
place.

Wire format is Confluent framing (magic byte + 4-byte big-endian schema id)
via ``confluent_kafka.schema_registry.avro.AvroSerializer`` — see ADR 0006
decision 2. The schema is the checked-in, CI-synced
``contracts/transaction.avsc`` (generated from the JSON-Schema contract);
this module never talks to the registry at import time.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_AVRO_SCHEMA_PATH = os.path.join(_REPO_ROOT, "contracts", "transaction.avsc")

# ADR 0006 decision 3: subject follows the default topic-name strategy
# ("{topic}-value") for the `transactions` topic.
TRANSACTIONS_SUBJECT = "transactions-value"
_COMPATIBILITY_LEVEL = "BACKWARD"

REGISTRY_WAIT_TIMEOUT_S = 60.0
REGISTRY_WAIT_POLL_INTERVAL_S = 3.0


def registry_url() -> str:
    """Read SCHEMA_REGISTRY_URL from env, failing loud if unset."""
    url = os.environ.get("SCHEMA_REGISTRY_URL", "").strip()
    if not url:
        raise RuntimeError(
            "SCHEMA_REGISTRY_URL is not set — required to build an Avro producer for "
            "the `transactions` topic (ADR 0006)"
        )
    return url


def registry_client(url: str | None = None) -> Any:
    """Build a SchemaRegistryClient pointed at `url` (or SCHEMA_REGISTRY_URL)."""
    from confluent_kafka.schema_registry import SchemaRegistryClient

    return SchemaRegistryClient({"url": url or registry_url()})


def wait_for_registry(client: Any, timeout_s: float = REGISTRY_WAIT_TIMEOUT_S) -> None:
    """Poll `client` until the registry answers, or raise TimeoutError after
    `timeout_s` seconds. Mirrors connect_register._wait_for_connect's
    poll/backoff/logging style for the broker's own readiness dependency."""
    deadline = time.monotonic() + timeout_s
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            client.get_subjects()
            return
        except Exception as exc:  # registry unreachable / not-yet-ready
            last_error = str(exc)
        time.sleep(REGISTRY_WAIT_POLL_INTERVAL_S)
    raise TimeoutError(f"schema registry did not become ready within {timeout_s}s: {last_error}")


def _load_avro_schema_str() -> str:
    with open(_AVRO_SCHEMA_PATH, encoding="utf-8") as f:
        return f.read()


def build_avro_serializer(client: Any) -> Any:
    """Build an AvroSerializer for contract-v1 events from the checked-in
    `contracts/transaction.avsc`, the same way `ingestion.py` loads the JSON
    contract from a repo-relative path (works both locally and inside the
    pipeline image, which copies `contracts/` — see docker/Dockerfile.pipeline)."""
    from confluent_kafka.schema_registry.avro import AvroSerializer

    schema_str = _load_avro_schema_str()
    return AvroSerializer(client, schema_str)


def build_avro_deserializer(client: Any) -> Any:
    """Build an AvroDeserializer for contract-v1 events using the checked-in
    `contracts/transaction.avsc` as the reader schema (ADR 0006 decision 2:
    Spark reads registry-framed messages against this same reader schema, so
    the Python consumer path — scorer.py — does too, rather than trusting
    the per-message writer schema alone)."""
    from confluent_kafka.schema_registry.avro import AvroDeserializer

    schema_str = _load_avro_schema_str()
    return AvroDeserializer(client, schema_str)


def ensure_backward_compatibility(client: Any, subject: str = TRANSACTIONS_SUBJECT) -> None:
    """Idempotently set/assert BACKWARD compatibility on `subject` (ADR 0006
    decision 3). Called once at producer startup; this is what later makes
    the ticket's "incompatible register -> 409" acceptance provable."""
    from confluent_kafka.schema_registry.error import SchemaRegistryError

    try:
        current = client.get_compatibility(subject)
    except SchemaRegistryError:
        # No subject-level override yet (registry 404s on /config/{subject}
        # until one is set) — fall through and set it explicitly.
        current = None

    if current == _COMPATIBILITY_LEVEL:
        return

    client.set_compatibility(subject, level=_COMPATIBILITY_LEVEL)
    logger.info("schema registry: set %s compatibility to %s (was %s)", subject, _COMPATIBILITY_LEVEL, current)
