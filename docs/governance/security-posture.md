# Security Posture

**Threat model, one line:** this is a local demo of a production shape; secrets discipline is structural, not operational. The stack runs on localhost with synthetic data and demo credentials — the point of ticket 14 is that the *code paths* (fail-loud config, no baked-in secrets, scanned dependencies/images, pinned base images) are what a real deployment would use, not that this repo is itself hardened for a real deployment.

## What's hardened

- **Fail-loud secrets in Compose.** `docker/docker-compose.yml` has no inline `:-default` fallback left for `BANK_DB_PASSWORD` / `TOKENIZATION_SALT` — both are `${VAR:?msg}` (required-var syntax), so an unset secret fails `docker compose config`/`up` immediately with a clear message instead of silently defaulting. `docker/demo.env` (committed, clearly marked demo-only) is the one place that supplies the local-dev values, wired in two ways: `--env-file docker/demo.env` (Makefile targets, `scripts/demo.sh`, `scripts/check.sh`) resolves the `${VAR:?msg}` compose-level interpolation, and each secret-using service also carries `env_file: [demo.env]` so the container gets the value directly. Pointing the stack at anything non-local requires exporting real values (or passing a different `--env-file`) — editing `demo.env` is not the intended path for that.
- **Fail-loud secrets in Python.** `src/bank/db.py` and `src/pipeline/ingestion.py` still fall back to the same demo values for local-dev ergonomics (so `import src.bank.db` works without compose/`.env` present, e.g. in tests), but each logs `WARNING: demo credential in use — not for production (...)` at import time whenever the fallback is actually in effect, so a demo credential silently reaching a non-local environment shows up in the logs immediately.
- **Dependency + config/IaC scanning in CI.** `.github/workflows/ci.yml`'s `security` job runs `pip-audit` against `requirements.txt` (non-blocking first pass — see below) and a Trivy filesystem+config scan (`aquasecurity/trivy-action`) covering the repo, Dockerfiles, compose, and Terraform, gated on CRITICAL/HIGH findings. Filesystem mode is used deliberately instead of building and scanning images, so the job doesn't need to build the app images to catch a misconfigured Dockerfile or a vulnerable pinned dependency.
- **Digest-pinned base images.** Every `FROM` line (`docker/Dockerfile.*`) and the infra-critical compose images (`redpanda`, `azure-sql-edge`, `prometheus`, `grafana`) are pinned to `@sha256:...` digests obtained from the locally-cached images (a human-readable tag is kept in an adjacent comment for reference). This makes builds reproducible and removes "the tag moved under us" as a supply-chain vector. `redis` and `quay.io/debezium/connect` are intentionally left on tags — neither is part of the default demo path's trust boundary in the same way (redis holds no secrets/PII; the `debezium` profile is opt-in and not part of `make demo`/`make demo-cdc`, see ADR 0003).
- **Tokenization.** Raw card identifiers never enter the pipeline — see `docs/governance/tokenization-policy.md` for the full mechanism (salted SHA-256, tokenized at the producer, before validation/publish).
- **Synthetic-only data.** TabFormer is synthetic; `bank.*` dimensional data is Faker-generated from TabFormer's synthetic `User`/`Card` indices. No real PANs, no real personal data, anywhere in this repo or the demo stack — see `CLAUDE.md` hard constraints and the tokenization policy above.

## What's deliberately demo-grade

These are conscious scope cuts for a $0, zero-manual-setup local demo — not oversights:

- **`sa` user.** `bank-db` (Azure SQL Edge) is accessed as `sa`, not a least-privilege application login. A real deployment would use a dedicated app login scoped to the `bank` schema.
- **No TLS in-cluster.** Redpanda, SQL Edge, Redis, and the API all talk PLAINTEXT/unencrypted on the local Docker network. Fine for localhost-only traffic that never leaves the machine; not fine across a real network boundary.
- **No Key Vault / managed identity.** `TOKENIZATION_SALT` and `BANK_DB_PASSWORD` are plain env vars sourced from `docker/demo.env` locally. The Terraform-side equivalent — provisioning an Azure Key Vault and wiring Container Apps to pull secrets via managed identity instead of plain app settings — is a **documented TODO**, not implemented: see `infra/terraform/` (no `azurerm_key_vault` resource yet) and `docs/adr/0001-stack-and-architecture.md`. `apply` for any cloud change stays a user-approval hard stop regardless.
- **pip-audit is non-blocking (first pass).** `requirements.txt` pins exact versions but the repo has no patch-cadence/waiver workflow yet, so a newly-disclosed CVE in a pinned dependency would fail every PR until someone manually bumps it — that's not a useful gate on day one. `continue-on-error: true` keeps findings visible (job output, not silently dropped) without blocking merges; the Trivy config/filesystem scan is the gate that does block, on CRITICAL/HIGH.

## Where the pieces live

| Concern | Location |
|---|---|
| Compose fail-loud secrets, demo defaults | `docker/docker-compose.yml`, `docker/demo.env` |
| Python fail-loud secrets, demo-default warning | `src/bank/db.py`, `src/pipeline/ingestion.py` |
| CI dependency/config scanning | `.github/workflows/ci.yml` (`security` job) |
| Digest-pinned images | `docker/Dockerfile.api`, `docker/Dockerfile.pipeline`, `docker/Dockerfile.dashboard`, `docker/docker-compose.yml` |
| Tokenization | `docs/governance/tokenization-policy.md` |
| Cloud secrets TODO | `infra/terraform/` (Key Vault + managed identity wiring not yet built) |
