# Changelog

All notable changes to this project. Format: [Keep a Changelog](https://keepachangelog.com), versioning: tags per phase, `v1.0` at Definition of Done.

## [Unreleased]
### Added
- Engineering-discipline scaffold: `docs/STATE.md` handoff doc, `docs/SETUP.md`, ADRs, delegation tickets, `make check` pre-push gate, `.env.example`
- Transaction data contract v1 (`contracts/transaction.schema.json`)
- `scripts/get_data.py` — TabFormer download + stratified local sample
- dbt-duckdb analytics project (`dbt/`): `stg_transactions` (analytical mirror of the ingestion contract), `fct_daily_fraud_rate`, `fct_merchant_risk`, `fct_channel_mix` marts, plus not_null/unique/accepted_values tests and a custom `fraud_rate_between_0_and_1` generic test — builds standalone against the committed sample CSV, no live services required
- Governance docs (`docs/governance/`): `data-dictionary.md`, `lineage.md` (mermaid end-to-end + dbt lineage), `tokenization-policy.md`
- README rewritten from the project template: architecture diagram, Key Results (model metrics pending full-data retrain; cold-path API latency baseline from `benchmarks/latest.json`), run/deploy instructions, honest "What I'd Improve Next"
- Additional unit tests for `src/pipeline/features.py`'s MCC travel-range boundary and `MCC_GROUP_IDS` ordinal stability (both load-bearing for the dbt `mcc_group` mart mirror, previously untested)
