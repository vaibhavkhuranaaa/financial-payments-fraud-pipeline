# ADR 0001: Stack and architecture for the fraud pipeline

- **Status:** accepted (2026-07-17, approved by project owner)
- **Context:** Portfolio project must demonstrate industry-grade streaming DE for financial/global-payments hiring (Capital One-style), per CLAUDE.md. Must be resumable by any agent/model mid-build.

## Decisions
1. **Broker:** Kafka protocol everywhere. Local = Redpanda (single container); cloud = Azure Event Hubs **Standard** (Kafka endpoint requires Standard tier). One client codebase, env-driven SASL config.
2. **Stream processing:** Spark Structured Streaming (PySpark 3.5) over Flink — better Python story, Databricks-portable.
3. **Storage:** Delta Lake for all offline data (raw events, features, scored events) instead of plain Parquet — ACID, time travel, Databricks/Unity-Catalog-ready. Online features in Redis.
4. **Train/serve skew prevention:** one shared feature-definition module (`src/pipeline/features.py`) used by both the streaming job and offline training builder.
5. **IaC:** Terraform (azurerm), not Bicep — industry-portable, plan/apply/destroy workflow. Full real deployment with tested `terraform destroy`.
6. **Batch packaging:** Databricks Asset Bundle (`databricks.yml`) wrapping feature-build + training jobs; executed locally for v1 (no live workspace — cost decision), deployment path documented.
7. **Analytics:** dbt with DuckDB target locally over Delta/Parquet exports; models portable to Databricks SQL.
8. **Model:** XGBoost binary classifier; `scale_pos_weight` for the ~0.1% fraud rate; strict time-based split; threshold chosen from the precision-recall curve and versioned alongside the model.
9. **Governance as artifacts:** JSON-Schema data contract (`contracts/`), producer-side validation + in-stream re-validation with DLQ topic, salted-SHA-256 PAN tokenization at the producer (raw card IDs never enter the pipeline), dbt tests as quality gates, lineage/data-dictionary docs.
10. **Python 3.11 pinned venv (uv)** — system Python 3.14 is unsupported by PySpark 3.5.

## Consequences
- Event Hubs Standard costs ~$25–30/month while provisioned → teardown script is part of Definition of Done.
- Delta requires delta-spark JARs in the streaming container; compose image build handles it.
- dbt reads Delta via DuckDB's parquet reader over exported snapshots (documented limitation; Databricks SQL removes it).
