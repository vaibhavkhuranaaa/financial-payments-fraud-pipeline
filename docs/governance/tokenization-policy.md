# Card Tokenization Policy

## Why raw PANs/card identifiers never enter the pipeline

TabFormer's source CSV carries a `User`/`Card` pair (small integer indices — TabFormer does not include real PANs), which stands in for what would be a real card/account identifier in a production system. Even though TabFormer's synthetic identifiers carry no real PII, the pipeline is built as if they did, because that's the discipline a real financial-payments system requires: **raw card identifiers must never cross the wire, sit in a topic, or land in a lake.** This is a hard constraint per `CLAUDE.md` ("No real payment data or PII") and ADR 0001 decision 9.

## Mechanism: salted SHA-256, tokenized at the producer

`src/pipeline/ingestion.py::_card_token`:

```python
def _card_token(user: str, card: str, salt: str) -> str:
    return hashlib.sha256(f"{salt}:{user}:{card}".encode("utf-8")).hexdigest()
```

- Tokenization happens **at the producer**, before the event is validated against the contract or published to Kafka. The raw `User`/`Card` values exist only transiently inside `to_event()`; they are never serialized, logged, or included in the event payload.
- The output, `card_token`, is a 64-character lowercase hex string (`^[a-f0-9]{64}$` in `contracts/transaction.schema.json`) — this is the *only* card identity that flows through Kafka, Redis, Delta, the model, and the API.
- Tokenization is deterministic per `(salt, user, card)`: the same real-world card always maps to the same token (needed so Redis's `features:{card_token}` windowed aggregates and the model's per-card features are coherent) — but the token itself reveals nothing about the underlying identifier without the salt.
- The dbt staging model (`dbt/models/staging/stg_transactions.sql`) reproduces the identical construction in SQL (`sha256(salt || ':' || user_id || ':' || card_id)`) over the same sample CSV, so analytical `card_token` values are byte-for-byte identical to what the live producer would emit for the same salt/rows — verified during this ticket's build (DuckDB's `sha256()` output matches Python's `hashlib.sha256().hexdigest()` exactly).
- The bank system-of-record (`bank.cards`, ADR 0002) does the same thing a third way: `src/bank/seed.py` imports `src.pipeline.ingestion._card_token` directly (never reimplements it) to derive `card_token` for every `(User, Card)` pair in the sample CSV. **`card_token` is the *only* join key between the live transaction stream/scoring path and the bank DB's dimensional tables** — there is no other cross-reference. `bank.customers`/`bank.accounts`/`bank.cards` are entirely Faker-synthetic (seed=42: names, emails, deterministic credit limits/card types) derived from TabFormer's synthetic `User`/`Card` indices; no PAN, no raw card identifier, and no real personal data exists anywhere in `bank.*`, `scored_transactions`, or `fraud_alerts` — those tables carry `card_token` and the same contract fields already covered above, nothing more.

## Salt handling

| Environment | Salt source | Notes |
|---|---|---|
| Local dev / docker compose | `TOKENIZATION_SALT` env var, default `change-me-local-only` (`.env.example`) | Intentionally a placeholder — never used outside a local machine. |
| CI (dbt build) | `dbt/dbt_project.yml` var `tokenization_salt`, same default | Only needs to be internally consistent for the sample CSV; not a security boundary in CI. |
| Cloud (Azure) | Azure Key Vault secret, injected as `TOKENIZATION_SALT` into the Container App environment | Never committed, never logged, never passed as a CLI arg (avoids process-list/shell-history exposure). |

## Rotation consequences

Rotating the salt is a **breaking change to card identity continuity**, not a routine key-rotation:

- Every card produces a *new* `card_token` after rotation, because the token is `SHA256(salt || user:card)`.
- All Redis online-feature state (`features:{card_token}`) for the old token space becomes orphaned/unreachable — the windowed aggregates effectively reset to cold-start for every card.
- Any offline Delta/dbt history keyed by the old `card_token` can no longer be joined to newly-ingested events without also retaining the old salt (to recompute the old token for historical joins) or a token-to-token mapping table.
- The model itself is not directly affected (it never sees `card_token` as a feature — only the *aggregates* keyed by it), but a rotation effectively **cold-starts every card's windowed features** the moment it takes effect, which briefly degrades feature quality (all `_1h`/`_1d`/`_7d`/`_30d` aggregates read as zero/empty until enough post-rotation history accumulates).

**Recommendation:** rotate the salt only for a genuine security event (suspected salt leak), not on a routine schedule, and treat it as a coordinated deploy: update the Key Vault secret, restart the streaming job and API together, and accept the transient cold-start window rather than trying to preserve continuity across the rotation boundary.
