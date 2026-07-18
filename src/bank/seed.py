"""Deterministic seed loader for the bank-db core-banking system-of-record.

Data flow
---------
Derives `bank.customers` / `bank.accounts` / `bank.cards` directly from the
TabFormer sample CSV (`data/sample/transactions_sample.csv`) — the same file
`src.pipeline.ingestion` replays onto Kafka. This keeps the "core banking"
system-of-record consistent with the transaction stream: every `card_token`
the streaming producer emits already exists in `bank.cards` before a single
event is scored.

- One `bank.customers` row per unique TabFormer `User`.
- One `bank.accounts` row per customer (single account per customer, v1).
- One `bank.cards` row per unique (`User`, `Card`) pair, with `card_token`
  computed by the EXACT salted-SHA256 tokenization the streaming producer
  uses — `src.pipeline.ingestion._card_token` is imported, never
  reimplemented, so pipeline events always resolve to a real seeded card.

Determinism: unique users/cards are processed in sorted order and Faker is
seeded once (seed=42) before any name/email is drawn, so repeated runs over
the same input CSV derive byte-identical customers/accounts/cards — no real
PII, Faker output only.

Idempotent: applies `schema.sql` (itself guarded with `IF NOT EXISTS`), then
replaces the derived dimension rows inside one transaction (delete + bulk
insert, respecting FK order) — safe to run any number of times.

CLI: `python -m src.bank.seed [--input data/sample/transactions_sample.csv]`.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from dotenv import load_dotenv
from faker import Faker
from sqlalchemy import Engine, text

from src.bank.db import get_engine, run_script
from src.pipeline.ingestion import TOKENIZATION_SALT, _card_token

load_dotenv()

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")

SEED_INPUT_CSV = os.environ.get("SEED_INPUT_CSV", "data/sample/transactions_sample.csv")

FAKER_SEED = 42
CARD_TYPES = ("visa", "mastercard", "amex")
# Fixed reference instant so opened_at/issued_at are deterministic across runs.
_EPOCH = datetime(2018, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class SeedData:
    customers: list[dict[str, Any]]
    accounts: list[dict[str, Any]]
    cards: list[dict[str, Any]]


def _resolve_path(path: str) -> str:
    return path if os.path.isabs(path) else os.path.join(_REPO_ROOT, path)


def load_unique_users_and_cards(csv_path: str) -> tuple[list[str], list[tuple[str, str]]]:
    """Scan the source CSV once; return sorted-unique Users and (User, Card) pairs."""
    users: set[str] = set()
    pairs: set[tuple[str, str]] = set()
    with open(_resolve_path(csv_path), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user = str(row["User"]).strip()
            card = str(row["Card"]).strip()
            users.add(user)
            pairs.add((user, card))
    return sorted(users, key=lambda u: (len(u), u)), sorted(pairs, key=lambda p: (len(p[0]), p[0], len(p[1]), p[1]))


def _account_id(user: str) -> str:
    return f"acct-{user}"


def _credit_limit(user: str) -> float:
    """Deterministic credit limit derived from the user id: $1,000-$20,000 in $1,000 steps."""
    try:
        n = int(user)
    except ValueError:
        n = int(hashlib.sha256(user.encode("utf-8")).hexdigest(), 16)
    return float(1000 * (1 + (n % 20)))


def build_customers(users: list[str]) -> list[dict[str, Any]]:
    """One customer per unique User, with deterministic synthetic name/email.

    Faker is (re-)seeded immediately before drawing, and `users` is processed
    in the caller-provided (sorted) order, so this is reproducible run to run.
    """
    fake = Faker()
    Faker.seed(FAKER_SEED)
    customers = []
    for user in users:
        customers.append(
            {
                "customer_id": user,
                "name": fake.name(),
                "email": fake.email(),
                "created_at": _EPOCH,
                "risk_tier": "standard",
            }
        )
    return customers


def build_accounts(customers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    accounts = []
    for customer in customers:
        user = customer["customer_id"]
        accounts.append(
            {
                "account_id": _account_id(user),
                "customer_id": user,
                "opened_at": _EPOCH,
                "credit_limit": _credit_limit(user),
                "status": "active",
            }
        )
    return accounts


def build_cards(
    user_card_pairs: list[tuple[str, str]], salt: str
) -> list[dict[str, Any]]:
    cards = []
    for user, card in user_card_pairs:
        card_index = int(card) if card.isdigit() else abs(hash(card)) % len(CARD_TYPES)
        cards.append(
            {
                "card_token": _card_token(user, card, salt),
                "account_id": _account_id(user),
                "card_type": CARD_TYPES[card_index % len(CARD_TYPES)],
                "issued_at": _EPOCH + timedelta(days=card_index),
            }
        )
    return cards


def derive_seed_data(csv_path: str = SEED_INPUT_CSV, salt: str = TOKENIZATION_SALT) -> SeedData:
    """Pure derivation: CSV -> deterministic (customers, accounts, cards). No DB I/O."""
    users, pairs = load_unique_users_and_cards(csv_path)
    customers = build_customers(users)
    accounts = build_accounts(customers)
    cards = build_cards(pairs, salt)
    return SeedData(customers=customers, accounts=accounts, cards=cards)


def fingerprint(data: SeedData) -> str:
    """Stable hash of the derived dimension rows, for determinism assertions."""
    payload = json.dumps(
        {
            "customers": data.customers,
            "accounts": data.accounts,
            "cards": data.cards,
        },
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def apply_schema(engine: Engine) -> None:
    with open(_SCHEMA_PATH, encoding="utf-8") as f:
        run_script(engine, f.read())


def write_dims(engine: Engine, data: SeedData) -> None:
    """Replace derived dimension rows inside one transaction (delete + insert, FK order)."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM bank.cards"))
        conn.execute(text("DELETE FROM bank.accounts"))
        conn.execute(text("DELETE FROM bank.customers"))

        if data.customers:
            conn.execute(
                text(
                    "INSERT INTO bank.customers "
                    "(customer_id, name, email, created_at, risk_tier) "
                    "VALUES (:customer_id, :name, :email, :created_at, :risk_tier)"
                ),
                data.customers,
            )
        if data.accounts:
            conn.execute(
                text(
                    "INSERT INTO bank.accounts "
                    "(account_id, customer_id, opened_at, credit_limit, status) "
                    "VALUES (:account_id, :customer_id, :opened_at, :credit_limit, :status)"
                ),
                data.accounts,
            )
        if data.cards:
            conn.execute(
                text(
                    "INSERT INTO bank.cards "
                    "(card_token, account_id, card_type, issued_at) "
                    "VALUES (:card_token, :account_id, :card_type, :issued_at)"
                ),
                data.cards,
            )


def seed(csv_path: str = SEED_INPUT_CSV, salt: str = TOKENIZATION_SALT) -> SeedData:
    """Apply schema.sql, then derive + (re)write dimension rows. Idempotent."""
    engine = get_engine()
    apply_schema(engine)
    data = derive_seed_data(csv_path, salt)
    write_dims(engine, data)
    return data


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Seed bank.customers/accounts/cards from the TabFormer sample CSV."
    )
    parser.add_argument("--input", default=SEED_INPUT_CSV, help="Path to source CSV.")
    args = parser.parse_args(argv)

    data = seed(csv_path=args.input)
    print(
        f"seeded {len(data.customers)} customers, {len(data.accounts)} accounts, "
        f"{len(data.cards)} cards (fingerprint {fingerprint(data)[:12]})"
    )


if __name__ == "__main__":
    main()
