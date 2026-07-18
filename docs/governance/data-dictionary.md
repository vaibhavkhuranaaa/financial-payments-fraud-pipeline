# Data Dictionary

Two layers of fields flow through this pipeline:

1. **Contract fields** — what's on the wire (`contracts/transaction.schema.json`), produced by `src/pipeline/ingestion.py` and re-derived analytically by `dbt/models/staging/stg_transactions.sql`.
2. **Feature fields** — what the model actually trains/scores on (`src/pipeline/features.py::FEATURE_COLUMNS`), computed by `enrich()` (event-level) and the windowed aggregates (`CARD_WINDOWS`).

## 1. Contract fields (`transactions` topic, data contract v1)

| Field | Type | Source | Meaning |
|---|---|---|---|
| `schema_version` | string (const `"1.0.0"`) | producer-set | Data contract version this event conforms to. |
| `event_id` | string (uuid) | producer-generated (`uuid4()`) | Unique event identifier; no natural key in the TabFormer source. |
| `event_time` | string (ISO-8601 UTC) | derived from source `Year`/`Month`/`Day`/`Time` | Transaction timestamp; watermarked 10 min downstream for windowed aggregation. |
| `card_token` | string (`^[a-f0-9]{64}$`) | `SHA256(salt \|\| user_id:card_id)` | Tokenized card identity — see `tokenization-policy.md`. Raw `User`/`Card` never leave `ingestion.py`. |
| `user_id` | string | source `User` column, stringified | Cardholder identifier (TabFormer synthetic user index — not a real identity). |
| `amount` | number | source `Amount` (`"$123.45"` parsed to float) | Transaction amount in `currency` units; negative = reversal/credit. |
| `currency` | string (ISO-4217) | constant `"USD"` | TabFormer source is USD-only. |
| `channel` | enum `chip` \| `swipe` \| `online` | source `Use Chip` (`Chip Transaction`/`Swipe Transaction`/`Online Transaction`) | Card-present (chip/swipe) vs. card-not-present (online). |
| `merchant_name` | string | source `Merchant Name` | Opaque merchant identifier (TabFormer emits an anonymized numeric string, not a real business name). |
| `merchant_city` | string | source `Merchant City` | Merchant city, or blank for online. |
| `merchant_state` | string | source `Merchant State`, or `"ONLINE"` for online txns, or `"XX"` if missing | US state code or foreign country name. |
| `merchant_country` | string (ISO-3166 alpha-2) | derived: US state → `US`; known foreign country name → mapped code; online/unrecognized → `XX` | Used for the `is_cross_border` feature and `fct_channel_mix.cross_border_share`. |
| `zip` | string \| null | source `Zip` (float-string cleaned to int-string) | Merchant ZIP; null for online/foreign/missing. |
| `mcc` | integer | source `MCC` | Merchant category code; feeds `mcc_group`/`mcc_group_id`. |
| `errors` | string \| null | source `Errors?` | Authorization error code(s), comma-separated; null if none. Presence drives the `decline_rate_*` window features. |
| `is_fraud` | boolean \| null | source `Is Fraud?` (`Yes`/`No` → bool) | Ground-truth label. Present only in replay/training flows — a live production authorization stream would never carry this. |

## 2. Feature fields (`FEATURE_COLUMNS`, `src/pipeline/features.py`)

### Event-level enrichment (`enrich()` — stateless, no history required)

| Feature | Type | Derivation | Meaning |
|---|---|---|---|
| `amount` | float | passthrough of contract `amount` | Raw transaction amount. |
| `amount_log` | float | `log1p(abs(amount))` | Log-scaled amount; compresses the long right tail of transaction sizes. |
| `is_cnp` | int (0/1) | `channel == "online"` | Card-not-present flag. |
| `is_chip` | int (0/1) | `channel == "chip"` | Chip (card-present) transaction flag. |
| `is_swipe` | int (0/1) | `channel == "swipe"` | Magstripe-swipe transaction flag. |
| `is_cross_border` | int (0/1) | `merchant_country not in ("US", "XX")` | Cross-border transaction flag. |
| `mcc_group_id` | int | `MCC_GROUP_IDS[_mcc_group(mcc)]` | Ordinal id for the coarse spend category (`travel`=0, `grocery`=1, `cash`=2, `online_retail`=3, `other`=4). See `_MCC_EXACT_GROUPS`/range rules in `features.py`. |
| `mcc` | int | passthrough of contract `mcc` | Raw merchant category code (lets the trees split finer than the coarse groups). |
| `has_error` | int (0/1) | `bool(event["errors"])` | Current event carries a non-null auth-error code. |
| `hour_of_day` | int (0–23) | `event_time.hour` (UTC) | Time-of-day signal. |
| `day_of_week` | int (0–6) | `event_time.weekday()` | Day-of-week signal (0=Monday). |

### Point-in-time history features (sequential, not window aggregates)

Computed from the card's history strictly before the scored event (`build_feature_row` derives them; online, `last_event_ts` and the trailing-city set live in Redis):

| Feature | Type | Meaning |
|---|---|---|
| `time_since_last_txn_s` | float | Seconds since the card's previous transaction, capped at the 30d window (a card's first-ever event gets the cap). |
| `is_new_city_30d` | int (0/1) | Current `merchant_city` not seen for this card in the trailing 30 days. |
| `amount_over_mean_30d` | float | Current amount divided by the card's trailing-30d mean amount (denominator 1.0 when no history). |

### Windowed per-card aggregates (`CARD_WINDOWS = {1h, 1d, 7d, 30d}`)

Computed **strictly before** the event being scored (point-in-time correctness — the scored event never contributes to its own features). Windows are density-matched to TabFormer's roughly-daily card activity (sub-hour windows were almost always empty). For each window key `w` in `{1h, 1d, 7d, 30d}`:

| Feature (`{metric}_{w}`) | Type | Meaning |
|---|---|---|
| `txn_count_{w}` | float | Count of this card's transactions in the trailing window. |
| `amount_sum_{w}` | float | Sum of amounts in the trailing window. |
| `amount_mean_{w}` | float | Mean amount in the trailing window (0 if no prior activity). |
| `distinct_merchant_city_{w}` | float | Distinct merchant cities visited in the trailing window — a velocity/geographic-spread signal. |
| `decline_rate_{w}` | float | Fraction of trailing-window transactions carrying a non-null `errors` code — a proxy for "this card is currently being probed/declined." |

A card with no prior history in a window gets all-zero aggregates for that window (`_empty_window_features`).

## 3. Bank system-of-record fields (`bank.*`, Azure SQL Edge, `src/bank/schema.sql`)

The system-of-record the pipeline hangs off (ADR 0002). Dimensional tables (`customers`/`accounts`/`cards`) are seeded once, deterministically, by `src/bank/seed.py` from `data/sample/transactions_sample.csv`; the fact tables (`scored_transactions`/`fraud_alerts`) are written continuously by the live scorer loop (`src/pipeline/scorer.py`, ticket 07) and read (plus, for `fraud_alerts.status`, written back to) by the dashboard (`src/dashboard/`, ticket 08). **`card_token` is the only join key between this schema and the streaming/scoring path — no PAN and no raw TabFormer `User`/`Card` index ever appears in `bank.*`; see `tokenization-policy.md`. All rows are Faker-synthetic (seed=42), not real customer data.**

### `bank.customers` — one row per unique TabFormer `User`

| Field | Type | Source | Meaning |
|---|---|---|---|
| `customer_id` | `NVARCHAR(64)` PK | TabFormer `User` (stringified) | Same identifier space as the contract's `user_id` — lets `bank.customers` and the streaming events line up on the same user, without ever sharing a raw PAN. |
| `name` | `NVARCHAR(200)` | `Faker(seed=42).name()` | Synthetic display name — never a real person. |
| `email` | `NVARCHAR(200)` | `Faker(seed=42).email()` | Synthetic email — never a real address. |
| `created_at` | `DATETIME2` | fixed epoch (`2018-01-01T00:00:00Z`) | Deterministic placeholder "account creation" instant, same for every seed run. |
| `risk_tier` | `NVARCHAR(20)` | constant `"standard"` (v1) | Reserved for future risk-tiering logic; every seeded customer is `standard` today. |

### `bank.accounts` — one row per customer (single account per customer, v1)

| Field | Type | Source | Meaning |
|---|---|---|---|
| `account_id` | `NVARCHAR(64)` PK | `f"acct-{customer_id}"` | Deterministic, derived — not a random identifier. |
| `customer_id` | `NVARCHAR(64)` FK → `bank.customers` | passthrough | Owning customer. |
| `opened_at` | `DATETIME2` | fixed epoch (`2018-01-01T00:00:00Z`) | Same deterministic placeholder as `customers.created_at`. |
| `credit_limit` | `DECIMAL(12,2)` | `1000 * (1 + (int(user_id) % 20))` | Deterministic $1,000–$20,000 limit in $1,000 steps, derived from the user id (falls back to a SHA-256 hash of the id for non-numeric ids). |
| `status` | `NVARCHAR(20)` | constant `"active"` (v1) | Reserved for future account-status logic. |

### `bank.cards` — one row per unique (`User`, `Card`) pair

| Field | Type | Source | Meaning |
|---|---|---|---|
| `card_token` | `CHAR(64)` PK | `src.pipeline.ingestion._card_token(user, card, salt)` — **imported, not reimplemented** | MUST byte-for-byte match the token the streaming producer emits for the same `(User, Card)` — this is what lets the scorer resolve a live event's `card_token` to a real seeded card with zero manual mapping. |
| `account_id` | `NVARCHAR(64)` FK → `bank.accounts` | derived from `User` | Owning account. |
| `card_type` | `NVARCHAR(20)` | `["visa", "mastercard", "amex"][card_index % 3]` | Deterministic pseudo-network assignment; not sourced from TabFormer (which has no network field). |
| `issued_at` | `DATETIME2` | fixed epoch `+ card_index` days | Deterministic placeholder issue date, staggered per card index so multiple cards on one user don't collide. |

### `bank.scored_transactions` — insert-heavy audit log, one row per scored event

Written by `src/pipeline/scorer.py` for every event it consumes off the `transactions` topic and successfully scores; idempotent on `event_id` (duplicate inserts from replays/consumer-group rebalances are swallowed, not fatal). Indexed on `(scored_at)` and `(card_token)`.

| Field | Type | Source | Meaning |
|---|---|---|---|
| `event_id` | `NVARCHAR(64)` PK | contract `event_id` | Same event identity as the streaming path — lets this table be joined back to Delta `events` if ever needed. |
| `card_token` | `CHAR(64)` | contract `card_token` | Joins to `bank.cards.card_token`. |
| `event_time` | `DATETIME2` | contract `event_time` | Original transaction timestamp (not the scoring instant). |
| `amount` | `DECIMAL(12,2)` | contract `amount` | Transaction amount. |
| `merchant_name` / `merchant_city` / `merchant_state` | `NVARCHAR` | contract fields | Passthrough merchant descriptors. |
| `mcc` | `INT` | contract `mcc` | Merchant category code. |
| `channel` | `NVARCHAR(20)` | contract `channel` | `chip` / `swipe` / `online`. |
| `fraud_probability` | `FLOAT` | `/score` response | Model output for this event. |
| `decision` | `NVARCHAR(20)` | `/score` response | e.g. `approve` / `review`, per the response threshold. |
| `cold_card` | `BIT` | `/score` response | Whether the API fell back to zero-history features (Redis miss) for this score — the same signal the dashboard's cold-card-share tile aggregates. |
| `latency_ms` | `FLOAT` | `/score` response | Server-side scoring latency reported by the API for this request. |
| `scored_at` | `DATETIME2` | scorer insert time (`SYSUTCDATETIME()`) | When the scorer wrote the row — distinct from `event_time`. |

### `bank.fraud_alerts` — analyst-facing alert queue

Written (insert) by `src/pipeline/scorer.py` when `fraud_probability` clears `ALERT_THRESHOLD` (or the response's own model threshold); updated (`status`, `reviewed_at`) only by the dashboard's Confirm fraud / Dismiss actions. Indexed on `(status, created_at)`.

| Field | Type | Source | Meaning |
|---|---|---|---|
| `alert_id` | `INT IDENTITY` PK | auto-increment | Surrogate key. |
| `event_id` | `NVARCHAR(64)` | contract `event_id` | Links back to the triggering `bank.scored_transactions` row. |
| `card_token` | `CHAR(64)` | contract `card_token` | Joins to `bank.cards → accounts → customers` for the dashboard's alert-queue customer context. |
| `fraud_probability` | `FLOAT` | `/score` response | Score that triggered the alert. |
| `amount` | `DECIMAL(12,2)` | contract `amount` | Transaction amount, shown in the alert card. |
| `merchant_name` | `NVARCHAR(200)` | contract `merchant_name` | Shown in the alert card. |
| `created_at` | `DATETIME2` | insert time (`SYSUTCDATETIME()`) | When the alert was raised. |
| `status` | `NVARCHAR(20)` | dashboard action, default `'open'` | One of `open` / `confirmed_fraud` / `dismissed` (`CHECK` constraint). |
| `reviewed_at` | `DATETIME2` \| null | dashboard action | Set when an analyst clicks Confirm fraud or Dismiss; null while `open`. |

## 4. dbt mart fields

See `dbt/models/marts/_marts.yml` for full column-level docs/tests. Summary:

- **`fct_daily_fraud_rate`** — `txn_date`, `txn_count`, `fraud_count`, `fraud_rate` (`fraud_count / txn_count`), `gross_amount` (sum of positive amounts only — negative amounts are reversals/credits per the contract and would understate gross spend if netted in).
- **`fct_merchant_risk`** — `mcc_group` (mirrors the model's `mcc_group_id` grouping via `dbt/macros/mcc_group.sql`) × `merchant_state`: `txn_count`, `fraud_count`, `fraud_rate`, `avg_amount`.
- **`fct_channel_mix`** — `channel`: `txn_count`, `fraud_count`, `fraud_rate`, `cross_border_share` (`merchant_country not in (US, XX)` divided by `txn_count`).
