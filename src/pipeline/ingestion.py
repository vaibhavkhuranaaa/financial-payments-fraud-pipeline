"""Event Hubs / Kafka replay producer for TabFormer card transactions.

Data flow
---------
1. Read the TabFormer CSV row-by-row (``data/sample/transactions_sample.csv`` for
   local dev, or the full ``data/raw/card_transaction.v1.csv`` in production replay).
2. ``to_event`` maps each raw row onto contract-v1
   (``contracts/transaction.schema.json``): timestamps are normalized to UTC
   ISO-8601, ``Amount`` strings ("$123.45") are parsed to floats, the raw
   ``User``/``Card`` pair is salted-SHA-256 tokenized into ``card_token`` (the raw
   PAN/card identifiers never leave this module), and ``Merchant State`` is
   resolved to an ISO-3166 alpha-2 ``merchant_country``.
3. Every event is validated against the compiled JSON-Schema contract. Valid
   events are produced, keyed by ``card_token`` (so Kafka partitioning gives
   per-card ordering), to ``KAFKA_TOPIC_TRANSACTIONS``. Invalid events are
   produced (original row + failure reason) to ``KAFKA_TOPIC_DLQ`` instead of
   being dropped, so no data is silently lost.
4. A simple sleep-based rate limiter throttles replay to ``PRODUCER_EVENTS_PER_SEC``
   to simulate a live transaction feed instead of a burst dump.

This module is broker-agnostic: ``kafka_config`` builds a PLAINTEXT config for a
local Redpanda broker by default, or a SASL_SSL/PLAIN config (unchanged client
code) when ``KAFKA_SASL_PASSWORD`` is set, which is exactly what Azure Event
Hubs' Kafka-compatible endpoint requires.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import time
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from jsonschema import Draft202012Validator

load_dotenv()

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_SCHEMA_PATH = os.path.join(_REPO_ROOT, "contracts", "transaction.schema.json")

# --- Env-driven configuration (defaults mirror .env.example) ---------------

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.environ.get("KAFKA_SASL_MECHANISM", "")
KAFKA_SASL_USERNAME = os.environ.get("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.environ.get("KAFKA_SASL_PASSWORD", "")
KAFKA_TOPIC_TRANSACTIONS = os.environ.get("KAFKA_TOPIC_TRANSACTIONS", "transactions")
KAFKA_TOPIC_DLQ = os.environ.get("KAFKA_TOPIC_DLQ", "transactions.dlq")

PRODUCER_EVENTS_PER_SEC = float(os.environ.get("PRODUCER_EVENTS_PER_SEC", "200"))
PRODUCER_INPUT_CSV = os.environ.get("PRODUCER_INPUT_CSV", "data/sample/transactions_sample.csv")

TOKENIZATION_SALT = os.environ.get("TOKENIZATION_SALT", "change-me-local-only")

# --- Reference data for merchant_country derivation -------------------------

US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP",
}

# Best-effort country-name -> ISO-3166 alpha-2 map for the countries observed
# in TabFormer plus a handful of common extras; unmapped/unknown -> "XX".
COUNTRY_NAME_TO_ISO2 = {
    "Italy": "IT",
    "Mexico": "MX",
    "France": "FR",
    "Bangladesh": "BD",
    "Norway": "NO",
    "Malaysia": "MY",
    "Greece": "GR",
    "Netherlands": "NL",
    "Japan": "JP",
    "Spain": "ES",
    "Czech Republic": "CZ",
    "Thailand": "TH",
    "China": "CN",
    "Canada": "CA",
    "Germany": "DE",
    "United Kingdom": "GB",
    "Portugal": "PT",
    "Switzerland": "CH",
    "Sweden": "SE",
    "Poland": "PL",
    "Austria": "AT",
    "Belgium": "BE",
    "Ireland": "IE",
    "India": "IN",
    "South Korea": "KR",
    "Vietnam": "VN",
    "Philippines": "PH",
    "Indonesia": "ID",
    "Australia": "AU",
    "Brazil": "BR",
    "Argentina": "AR",
    "South Africa": "ZA",
}

CHANNEL_MAP = {
    "Chip Transaction": "chip",
    "Swipe Transaction": "swipe",
    "Online Transaction": "online",
}


def _load_schema_validator() -> Draft202012Validator:
    """Compile the contract validator once at import time."""
    with open(_SCHEMA_PATH, encoding="utf-8") as f:
        schema = json.load(f)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


_VALIDATOR = _load_schema_validator()


def _card_token(user: str, card: str, salt: str) -> str:
    """Salted-SHA-256 tokenization: raw card identifiers never cross the wire."""
    return hashlib.sha256(f"{salt}:{user}:{card}".encode("utf-8")).hexdigest()


def _parse_amount(raw: str) -> float:
    """Parse TabFormer's '$123.45' / '$-54.00' amount strings to float."""
    return float(raw.replace("$", "").replace(",", "").strip())


def _resolve_country(merchant_state: str | None, channel: str) -> str:
    """Map raw Merchant State to an ISO-3166 alpha-2 merchant_country."""
    if channel == "online":
        return "XX"
    if not merchant_state or not merchant_state.strip():
        return "XX"
    state = merchant_state.strip()
    if state.upper() in US_STATE_CODES:
        return "US"
    return COUNTRY_NAME_TO_ISO2.get(state, "XX")


def to_event(row: dict[str, Any], salt: str) -> dict[str, Any]:
    """Map one raw TabFormer CSV row (as a dict) to a contract-v1 event.

    Raises KeyError/ValueError for rows so malformed that mapping itself is
    impossible (e.g. unparseable amount/time); the caller catches this and
    routes the raw row to the DLQ rather than crashing the replay.
    """
    use_chip = (row.get("Use Chip") or "").strip()
    channel = CHANNEL_MAP.get(use_chip, "online")

    year = int(row["Year"])
    month = int(row["Month"])
    day = int(row["Day"])
    hour_str, minute_str = row["Time"].strip().split(":")
    event_dt = datetime(year, month, day, int(hour_str), int(minute_str), tzinfo=timezone.utc)
    event_time = event_dt.isoformat().replace("+00:00", "Z")

    amount = _parse_amount(row["Amount"])

    user = str(row["User"]).strip()
    card = str(row["Card"]).strip()
    card_token = _card_token(user, card, salt)

    merchant_state_raw = (row.get("Merchant State") or "").strip()
    if channel == "online":
        merchant_state = "ONLINE"
    elif merchant_state_raw:
        merchant_state = merchant_state_raw
    else:
        merchant_state = "XX"
    merchant_country = _resolve_country(merchant_state_raw, channel)

    merchant_city = (row.get("Merchant City") or "").strip()

    zip_raw = row.get("Zip")
    zip_val: str | None
    if zip_raw is None or str(zip_raw).strip() in ("", "nan"):
        zip_val = None
    else:
        # Zip arrives as a float string (e.g. "85719.0") in the source CSV.
        try:
            zip_val = str(int(float(zip_raw)))
        except (TypeError, ValueError):
            zip_val = str(zip_raw).strip()

    errors_raw = (row.get("Errors?") or "").strip()
    errors = errors_raw.rstrip(",") if errors_raw else None

    is_fraud_raw = (row.get("Is Fraud?") or "").strip()
    is_fraud = is_fraud_raw == "Yes" if is_fraud_raw else None

    mcc = int(row["MCC"])

    return {
        "schema_version": "1.0.0",
        "event_id": str(uuid.uuid4()),
        "event_time": event_time,
        "card_token": card_token,
        "user_id": user,
        "amount": amount,
        "currency": "USD",
        "channel": channel,
        "merchant_name": str(row.get("Merchant Name") or "").strip(),
        "merchant_city": merchant_city,
        "merchant_state": merchant_state,
        "merchant_country": merchant_country,
        "zip": zip_val,
        "mcc": mcc,
        "errors": errors,
        "is_fraud": is_fraud,
    }


def validate_event(event: dict[str, Any]) -> str | None:
    """Return None if valid, else a human-readable validation error reason."""
    errors = sorted(_VALIDATOR.iter_errors(event), key=lambda e: e.path)
    if not errors:
        return None
    first = errors[0]
    path = "/".join(str(p) for p in first.path) or "<root>"
    return f"{path}: {first.message}"


def kafka_config() -> dict[str, Any]:
    """Build a confluent_kafka producer config from env vars.

    PLAINTEXT locally (Redpanda); SASL_SSL/PLAIN unchanged when
    KAFKA_SASL_PASSWORD is set, which is what Azure Event Hubs' Kafka endpoint
    requires (username is literally the string '$ConnectionString').
    """
    config: dict[str, Any] = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
    }
    if KAFKA_SASL_PASSWORD:
        config["security.protocol"] = "SASL_SSL"
        config["sasl.mechanism"] = KAFKA_SASL_MECHANISM or "PLAIN"
        config["sasl.username"] = KAFKA_SASL_USERNAME
        config["sasl.password"] = KAFKA_SASL_PASSWORD
    return config


def _iter_csv_rows(path: str) -> Iterator[dict[str, Any]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        yield from reader


class _RateLimiter:
    """Sleep-based token bucket limiting replay to N events/sec."""

    def __init__(self, events_per_sec: float) -> None:
        self._interval = 1.0 / events_per_sec if events_per_sec > 0 else 0.0
        self._next_time = time.monotonic()

    def wait(self) -> None:
        if self._interval <= 0:
            return
        now = time.monotonic()
        if now < self._next_time:
            time.sleep(self._next_time - now)
        self._next_time = max(now, self._next_time) + self._interval


def replay(
    input_path: str,
    salt: str,
    eps: float,
    max_events: int | None,
    dry_run: bool,
) -> tuple[int, int]:
    """Replay `input_path` to Kafka (or count-only in dry-run). Returns (valid, invalid)."""
    producer = None
    if not dry_run:
        from confluent_kafka import Producer

        producer = Producer(kafka_config())

    limiter = _RateLimiter(eps)
    valid_count = 0
    invalid_count = 0

    for i, row in enumerate(_iter_csv_rows(input_path)):
        if max_events is not None and i >= max_events:
            break
        limiter.wait()
        try:
            event = to_event(row, salt)
            reason = validate_event(event)
        except (KeyError, ValueError) as exc:
            event = None
            reason = f"mapping error: {exc}"

        if reason is None:
            valid_count += 1
            if producer is not None:
                producer.produce(
                    KAFKA_TOPIC_TRANSACTIONS,
                    key=event["card_token"],
                    value=json.dumps(event),
                )
        else:
            invalid_count += 1
            if producer is not None:
                dlq_payload = {"raw_row": row, "error": reason}
                producer.produce(KAFKA_TOPIC_DLQ, value=json.dumps(dlq_payload))

    if producer is not None:
        producer.flush(10.0)

    return valid_count, invalid_count


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Replay TabFormer CSV rows as contract-v1 events onto Kafka."
    )
    parser.add_argument("--input", default=PRODUCER_INPUT_CSV, help="Path to source CSV.")
    parser.add_argument(
        "--eps", type=float, default=PRODUCER_EVENTS_PER_SEC, help="Target events/sec."
    )
    parser.add_argument(
        "--max-events", type=int, default=None, help="Stop after N rows (for tests/smoke)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate/map only; print counts instead of producing to Kafka.",
    )
    args = parser.parse_args(argv)

    valid, invalid = replay(
        input_path=args.input,
        salt=TOKENIZATION_SALT,
        eps=args.eps,
        max_events=args.max_events,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        print(f"dry-run: {valid} valid events, {invalid} invalid events (routed to DLQ)")
    else:
        print(f"produced {valid} valid events, {invalid} invalid events to DLQ")


if __name__ == "__main__":
    main(sys.argv[1:])
