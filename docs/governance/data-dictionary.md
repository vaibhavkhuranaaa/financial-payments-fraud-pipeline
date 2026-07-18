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

## 3. dbt mart fields

See `dbt/models/marts/_marts.yml` for full column-level docs/tests. Summary:

- **`fct_daily_fraud_rate`** — `txn_date`, `txn_count`, `fraud_count`, `fraud_rate` (`fraud_count / txn_count`), `gross_amount` (sum of positive amounts only — negative amounts are reversals/credits per the contract and would understate gross spend if netted in).
- **`fct_merchant_risk`** — `mcc_group` (mirrors the model's `mcc_group_id` grouping via `dbt/macros/mcc_group.sql`) × `merchant_state`: `txn_count`, `fraud_count`, `fraud_rate`, `avg_amount`.
- **`fct_channel_mix`** — `channel`: `txn_count`, `fraud_count`, `fraud_rate`, `cross_border_share` (`merchant_country not in (US, XX)` divided by `txn_count`).
