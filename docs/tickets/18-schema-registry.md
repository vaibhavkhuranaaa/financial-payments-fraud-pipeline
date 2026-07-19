# Ticket 18 — v1.6: Schema registry + typed events (roadmap item 2 — LARGEST, do last)

**Scope:** contract-v1 JSON → Avro on the `transactions` topic via Redpanda's built-in schema registry (`:8081`, already in the redpanda image — enable/advertise it in compose). Touches producer (`ingestion.py`), Spark job (`features.py` deserializer), scorer, cdc-transformer, contract docs. This is the highest-blast-radius change in the backlog: every consumer changes. Do NOT start it unless there is budget to finish and E2E-verify both demo modes; a half-migrated wire format is worse than JSON.

## Design constraints (decide in an ADR before coding)
1. **Avro over Protobuf** (TabFormer-shaped flat records, Python-first stack, registry-native).
2. **Wire format**: Confluent framing (magic byte + schema id) via `confluent_kafka.schema_registry` serializers — Spark side needs matching deserialization (`from_avro` needs the schema; the registry's Confluent framing must be stripped — check what the pinned PySpark version supports before committing to an approach; a plain `fastavro` UDF path is the honest fallback).
3. **Migration story**: topic stays `transactions`; schema registered under subject `transactions-value` with compatibility BACKWARD. The JSON-Schema contract file remains the human-readable source; generate the Avro schema from it (one script, checked-in output, CI check that they're in sync).
4. **DLQ unchanged** (invalid records fail serialization at the producer boundary — count them; DLQ keeps JSON for debuggability).

## Acceptance
- Both demo modes E2E green with Avro on the wire (replay + CDC), dashboard live, `make check` green.
- Registry survives `demo-down`/up; schema id stable; BACKWARD compatibility enforced (prove by attempting an incompatible register — expect 409).
- README/lineage/data-dictionary updated; ADR 0006 records the format/framing/migration decisions.
