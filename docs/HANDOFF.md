# Handoff — post-v1.6 (2026-07-20)

> Read `docs/STATE.md` first (source of truth: phase table, environment facts, exact next step).
> Repo state at handoff: everything pushed to `main` locally (push to remote not yet run — see
> below), `git status` clean, `make check` green (242 tests), tag `v1.6` created, no containers
> running, $0 cloud spend, no AI co-author trailers anywhere in history.

## Session summary (2026-07-20 — v1.6 shipped)

Execution model: **fable-foreman orchestration.** The lead model wrote ADR 0006 directly
(judgment work), then dispatched three Sonnet workers for the implementation chunks — each ran
in the working tree, was reviewed diff-by-diff by the orchestrator, and was blind-verified by a
fresh-context `foreman-verifier` (no access to the worker's reasoning, only the original task and
the diff) before commit. All three verifiers returned `PASS`. Live E2E verification — the part
no unit test or blind verifier could substitute for — was run directly by the orchestrator against
a real Docker stack, per this project's own standing rule that live gates are never delegated.

| Commit | What shipped |
|---|---|
| cfcf6b0 | ADR 0006 (Avro, Confluent framing, BACKWARD compat, migration story, type mapping) |
| a23fc16 | Schema foundation: `scripts/gen_avro_schema.py` (JSON-Schema → Avro, deterministic, `--check` CI gate), `contracts/transaction.avsc`, Redpanda schema registry enabled in compose (`:8081`/`:18081`), `fastavro` pinned |
| a635e5a | Producers: `src/pipeline/schema_registry.py` helper, `ingestion.py` + `cdc_transformer.py` now emit registry-framed Avro; DLQ stays JSON; Avro-serialization failures counted separately from validation failures |
| 9c1cb0c | Consumers: `scorer.py` Avro-decode (poison messages logged/counted/skipped, commit-after-flush untouched), `features.py` Spark `from_avro` + frame-strip, dead `_event_schema()` StructType removed |
| b151edd | **Live-caught fix:** `F.substring()`'s `pos`/`len` must be plain ints in PySpark 3.5 — crashed `spark-features` on every startup; fixed with `Column.substr()` |
| f825836, 122a70d | Docs: README/data-dictionary/lineage/CHANGELOG/STATE updated with live-verified numbers |

**Live verifications this session (all real, none simulated):**
- `make smoke` (replay mode): PASS, `bank.scored_transactions` 231,567 → 236,567 over 20s with Avro on the wire.
- Targeted Spark check: real Confluent-framed bytes confirmed on `transactions` (`rpk topic consume`, magic byte + schema id); 791,471 events correctly `from_avro`-decoded into Delta `events`, 1 row in `_quarantine` (a business-rule reject, not a decode failure — proves decode succeeded).
- BACKWARD-compatibility acceptance criterion: `GET /config/transactions-value` = `BACKWARD`; a schema with a new required field and no default → registry returns **HTTP 409** with `READER_FIELD_MISSING_DEFAULT_VALUE`.
- Full CDC-mode E2E with API + dashboard live: all services healthy, `/healthz`+`/score` 200, dashboard 200, `bank.scored_transactions` 12,900 → 16,550 and `bank.fraud_alerts` 1,233 over 20s, `cdc_transformer.py` producing Avro under the same schema id as the replay producer, `transactions.dlq` at 0.

## What failed this session (read before trusting anything)

1. **Real bug, caught only by live verification, not by 242 green unit tests:**
   `features.py`'s Confluent-frame-strip built `avro_payload = F.substring(F.col("value"), 6,
   F.length(F.col("value")) - 5)` — `F.substring()`'s `pos`/`len` args must be plain Python ints
   in PySpark 3.5.1; a `Column`-typed length raised `PySparkTypeError` and crash-looped
   `spark-features` on every startup. Unit tests are hermetic by design (no JVM in the test env)
   so this was structurally invisible to them — this is exactly why ticket 18's own spec
   forbade tagging `v1.6` without a live E2E pass. Fixed with `Column.substr()`, which does
   accept `Column` arguments for a data-dependent length; re-verified live post-fix
   (791,471 correctly decoded rows).
2. **Two local Docker/Kafka gotchas, not repo bugs, worth remembering:**
   - Bypassing `scripts/demo.sh` and bringing up CDC services manually skips its topic
     pre-creation step; `cdc_transformer.py` then logs transient `UNKNOWN_TOPIC_OR_PARTITION`
     for ~30–45s until Redpanda's single-broker metadata converges after the topic is created.
     Self-resolves; not a defect. Always let `demo.sh` create topics, or pre-create them
     yourself before starting consumers if bringing services up piecemeal.
   - Docker Compose (v2.24+) merges a service's `ports:` list across `-f` files by
     **appending**, not replacing — a naive override file adds a second port mapping instead of
     changing the first, which still collides. Use the `!override` YAML tag on the key to force
     a full replace (see the local-port-conflict workaround below).
3. **Host port 8000 was held by an unrelated project (`docker-app-1`,
   `healthcare-sepsis-prediction`) plus an active SSH tunnel during this session.** Rather than
   stop someone else's running container without asking, the orchestrator surfaced the conflict
   to the user, who said "use another port." Fixed with a **local, uncommitted** compose override
   (`docker/docker-compose.local-override.yml`, deleted after use, never staged/committed):
   ```yaml
   services:
     api:
       ports: !override
         - "18000:8000"
   ```
   Brought the full stack up with `-f docker/docker-compose.yml -f docker/docker-compose.local-override.yml`,
   ran every check against `localhost:18000` instead of `:8000`, then deleted the override file.
   Repo/Makefile/CI are unaffected — this was a session-local workaround, not a code change.

## Roadmap — done vs remaining

**Done:** v1.0 pipeline+API+Azure · v1.1 bank/scorer/dashboard · v1.2 CDC + delivery semantics ·
v1.3 observability · v1.4 security hardening + CI E2E smoke + pre-commit · v1.5 model ops + load
testing · **v1.6 typed events (Avro + schema registry).** **All 7 items in the original
industry-grade hardening roadmap are now shipped and live-verified.**

**Remaining:** no approved backlog item exists. `docs/tickets/12-dual-stream.md` (dual
auth/settlement streams) is a stub that needs a design pass and explicit user approval before
any work starts; graph/ring features are explicitly rejected — don't revisit either without the
user. Small housekeeping items carried from prior sessions, still untouched (all user-approval
items, not urgent): delete the three merged `worktree-agent-*` branches +
`.claude/worktrees/` dirs; consider moving the `v1.2` tag (it points one commit before a
test-hermeticity fix); Key Vault wiring in Terraform remains a documented TODO
(`security-posture.md`); Alertmanager/OTel/traces remain documented gaps (ADR 0004).

## Candid assessment: big-bank resume credibility

**Verdict: yes — list it, and it's stronger than the v1.5 assessment.** As of v1.6 this project
has closed its own most-cited gap: the wire format is no longer "loose JSON, next step Avro" —
it *is* Avro, registry-enforced, with a live-proven compatibility gate. Say "production-shaped,
single-node, fully observable, typed, and tested demo with measured limits."

Against real bars:
- **Schema governance — was the honest gap, now closed.** A generated-and-CI-synced Avro schema,
  a real schema registry enforcing BACKWARD compatibility (proven with a live 409, not asserted
  in prose), and a documented, deliberate asymmetry (Spark reads with the repo's schema, not
  per-message writer-schema resolution — OSS Spark's actual constraint, stated plainly in ADR
  0006 rather than glossed over).
- **Delivery guarantees — unchanged and still strong.** Commit-after-flush ordering was a hard
  constraint on this ticket and was verified byte-for-byte unchanged in both producers and both
  consumers; the migration touched serialization only.
- **Engineering process — the ticket's own gate did its job.** The spec said "don't tag without
  live E2E of both modes," and that gate caught a real crash-on-every-startup bug that 242 green
  hermetic tests structurally could not see (no JVM in the test environment). That's the ticket's
  design working exactly as intended, not a near-miss to gloss over.
- **Judgment under a real external constraint.** A local port conflict with a genuinely unrelated
  project was surfaced to the user instead of unilaterally killing someone else's container — and
  once the user said "use another port," it was solved without touching the compose files that
  ship in the repo (a local, uncommitted override, deleted after use).
- Everything else from the v1.5 assessment (delivery semantics, observability, testing/CI,
  security posture, ops maturity, model ops) is unchanged and still holds.

What would take it further: HA/multi-partition with a rebalance story, per-PR E2E on ARM
runners, and (if ever revisited) a real writer-schema-per-message path on Spark instead of the
reader-schema-only approach OSS Spark's `from_avro` currently forces.

## Follow-up prompt for a fresh agent (any model/vendor)

> Continue the fraud-pipeline project. Read `docs/STATE.md` first (source of truth), then this
> file. v1.6 (typed events — Avro + Redpanda schema registry, ADR 0006) shipped and was fully
> live-verified in both demo modes (`make demo`, `make demo-cdc`) on 2026-07-20, tag `v1.6`. All
> seven items in the original hardening roadmap are now shipped. **No backlog item is currently
> approved** — before starting any new work, confirm direction with the user. If they name a new
> initiative, write a ticket under `docs/tickets/` and an ADR under `docs/adr/` before any code,
> matching this project's established discipline (see prior ADRs 0001–0006 for the expected
> depth). Known environment facts: macOS + Colima (no GNU `timeout`, bash 3.2), compose needs
> `--env-file docker/demo.env` everywhere, pipeline image ENTRYPOINT is `python -m` (use
> `--entrypoint` for one-off commands), `.venv` must never be a symlink, `docker compose` merges
> a service's `ports:` list across `-f` files by *appending* — use the `!override` YAML tag to
> replace it if you ever need a local port remap. Hard stops requiring user confirmation: infra
> provisioning/teardown (`terraform apply`/`destroy`), `git push --force`, deleting data/branches/
> tags, anything touching secrets/credentials/IAM, and killing any container/process this session
> didn't start itself (check `docker ps -a` and `com.docker.compose.project.config_files` labels
> before assuming an unfamiliar container is yours to touch). At ~95% session usage: stop new
> work, run `make check` + `make smoke`, get the repo clean/pushed with correct tags, kill all
> containers (`make demo-down`, verify `docker ps` empty), rewrite this file in the same format
> (session summary, what failed, roadmap, candid credibility assessment), and end by printing this
> same style of self-contained follow-up prompt.
