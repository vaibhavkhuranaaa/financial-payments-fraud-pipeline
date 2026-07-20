# ADR 0006 — v1.6 typed events: Avro on `transactions` via Redpanda schema registry

**Status:** accepted (2026-07-20). **Ticket:** `docs/tickets/18-schema-registry.md`.

## Context

Every hop of the pipeline ships contract-v1 JSON: `ingestion.py` and
`cdc_transformer.py` produce `json.dumps(event)` to the `transactions` topic,
`features.py` parses it with `from_json` against a hand-mirrored Spark schema, and
`scorer.py` does `json.loads`. Validation is real (JSON-Schema at three boundaries)
but the wire itself is untyped: nothing stops a producer that skips validation, the
Spark schema is a second copy of the contract that can silently diverge, and every
message pays JSON's size/parse tax. This is the last roadmap item and the
highest-blast-radius one — all four serialization boundaries change at once, which
is exactly why the ticket forbids a partial migration.

Facts that constrain the design, verified against the pinned stack:
- `confluent-kafka==2.5.0` is already the only Kafka client in `requirements.txt`;
  its `schema_registry` subpackage (serializers + registry client) additionally needs
  `fastavro` and `requests`, which are not yet pinned.
- PySpark 3.5.1 (OSS) `from_avro` takes a JSON-format schema string only — it has
  **no** schema-registry integration and does not understand Confluent wire framing
  (that variant is Databricks-runtime-only). Decoding registry-framed messages in
  open-source Spark means stripping the 5-byte header first, then `from_avro`
  against a reader schema you supply. `spark-avro` ships as a separate artifact that
  must be added to `spark.jars.packages`.
- The `redpanda` image (v24.2.4, digest-pinned) has the schema registry built in on
  `:8081`; the compose service just doesn't enable/advertise/expose it yet.
- Demo state is ephemeral: `make demo-down` destroys the broker with its topics, so
  there is never a topic holding a mix of old-JSON and new-Avro messages to migrate.
- The Flask API image has no Kafka client at all — `/score` takes HTTP JSON and is
  untouched by this change.

## Decisions

1. **Avro, not Protobuf.** The records are TabFormer-shaped flat rows — no nesting,
   no repeated fields, nothing Protobuf is better at. The stack is Python-first with
   a JVM Spark consumer, and both sides have first-class Avro paths
   (`confluent_kafka.schema_registry.avro`, `spark-avro`). Redpanda's registry is
   Confluent-API-compatible and Avro is its most-exercised subject format. Protobuf
   would add a codegen step (`protoc`) to a pipeline that currently has none, for no
   modelling benefit on flat records.

2. **Confluent wire framing (magic byte `0x0` + 4-byte big-endian schema id) via
   `confluent_kafka.schema_registry` serializers on the Python side; manual
   frame-strip + `from_avro` on the Spark side.** Producers (`ingestion.py`,
   `cdc_transformer.py`) use `AvroSerializer` — schema auto-registered under the
   subject on first produce, id embedded per message. `scorer.py` uses
   `AvroDeserializer`. `features.py` cannot use the registry (see Context), so it
   adds `org.apache.spark:spark-avro_2.12:3.5.1` to `spark.jars.packages`, strips
   the framing with `substring(value, 6, length(value)-5)`, and decodes with
   `from_avro` against the **checked-in** `contracts/transaction.avsc` in PERMISSIVE
   mode (corrupt records become nulls, filtered and counted — a streaming job must
   not die on one bad message when the producer boundary already validates).
   Trade-off accepted: Spark reads with the repo's reader schema rather than the
   per-message writer schema, so a writer-schema change Spark doesn't know about
   yet must be backward-compatible — which is exactly what the registry's BACKWARD
   subject compatibility (decision 3) enforces at registration time. The honest
   fallback if `spark-avro` misbehaves — a `fastavro` Python UDF (fastavro is now a
   pinned dep anyway) — is documented here but not built speculatively.

3. **Topic stays `transactions`; subject `transactions-value`, compatibility
   BACKWARD; atomic cutover; JSON-Schema stays the human source of truth.** The
   Avro schema is *generated* from `contracts/transaction.schema.json` by a
   checked-in script (`scripts/gen_avro_schema.py`) whose output
   (`contracts/transaction.avsc`) is also committed; a CI step regenerates and
   diffs, so the two can never drift silently. All producers and consumers switch
   in the same release — safe precisely because demo state is ephemeral (no mixed
   topic history exists; a real production migration would need a dual-read window
   this demo deliberately doesn't build, and this ADR says so rather than
   pretending the problem doesn't exist). BACKWARD compatibility is set on the
   subject at demo bootstrap and proven in acceptance by attempting an incompatible
   registration and expecting HTTP 409.

4. **Type mapping keeps the contract's shapes — no opportunistic re-typing.**
   `event_time` stays an ISO-8601 **string**, not `timestamp-millis`: every
   downstream consumer (Spark enrichment, scorer, bank SQL) already parses the
   string form, and re-typing it mid-migration would smuggle a semantic change into
   what must be a pure transport change. `channel` maps to a **string**, not an
   Avro enum: Avro enum evolution is the format's best-known trap (an unknown
   symbol breaks old readers unless defaults are wired exactly right), and the
   chip/swipe/online domain is already enforced where it belongs — the JSON-Schema
   validation producers run before serializing. `amount` → `double`, `mcc` → `int`,
   the three nullable fields (`zip`, `errors`, `is_fraud`) → `["null", T]` with
   `default: null`. Field order in the generated schema is the contract file's
   order, so regeneration is deterministic.

5. **DLQ stays JSON, and serialization failures join it.** The DLQ exists for
   humans debugging bad rows; Avro-encoding rejects would be actively hostile
   there. Rows that fail contract validation go to the DLQ as today; a record that
   passes validation but fails Avro serialization (shouldn't happen — the schema is
   generated from the same contract — but "shouldn't" is not "can't") is sent to
   the DLQ with its error reason and counted separately, so a schema-generator bug
   surfaces as a visible counter, not silent loss.

## Consequences

- The wire format is now enforced by construction: a message that isn't
  registry-framed Avro can't be produced by the pipeline's own code path, and the
  Spark schema is no longer a hand-maintained second copy of the contract.
- New moving part: the registry endpoint (`:8081`) becomes a startup dependency of
  every producer/consumer of `transactions`. Compose gains a healthcheck on it and
  the Python services retry registry connection at startup like they already do for
  the broker; `SCHEMA_REGISTRY_URL` joins `docker/demo.env`.
- New pinned deps: `fastavro`, `requests` (serializer prerequisites); new Spark
  package: `spark-avro_2.12:3.5.1`. First `spark-features` cold start pays one
  extra Ivy fetch, cached in the image/volume thereafter.
- Schema evolution now has a gate (BACKWARD on `transactions-value`) and a proof
  (the 409 test) — but Spark reads with the repo's reader schema, so "registered
  and backward-compatible" is the invariant that keeps Spark correct, not
  per-message writer-schema resolution. That asymmetry is the honest cost of OSS
  Spark + Confluent framing, and it's contained by decision 3.
- DLQ debuggability is unchanged (still JSON), at the cost that DLQ messages are
  not typed — accepted: the DLQ's consumer is a human.
