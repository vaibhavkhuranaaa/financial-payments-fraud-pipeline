# Ticket 14 — v1.4a: Secrets & security hardening (roadmap item 3)

**Scope:** compose/env fail-loud posture, CI vuln scanning, image pinning. NO cloud changes beyond terraform code (validate/plan only — `apply` is a user-approval hard stop). Keep the $0-local-demo property: `make demo` must still work with zero manual setup.

## Deliverables
1. **Fail-loud secrets, demo-friendly defaults preserved via one explicit file:** add `docker/demo.env` (committed, clearly marked demo-only) holding today's compose fallback values (`BANK_DB_PASSWORD=LocalDev!Passw0rd`, `TOKENIZATION_SALT=change-me-local-only`). Compose services drop every inline `:-default` fallback for secrets and instead use `env_file: [demo.env]` + `${VAR:?msg}` required-var syntax where a secret has no safe default. Result: `make demo` unchanged (demo.env supplies values), but pointing the stack at anything non-local requires explicitly overriding, and an unset secret fails the `docker compose config` gate loudly instead of silently defaulting.
2. **Python side fail-loud:** `src/bank/db.py` and `src/pipeline/ingestion.py` (tokenization salt) currently hard-code the same fallbacks. Keep local-dev ergonomics but log a WARNING at startup when a known demo default is in use ("demo credential in use — not for production"). Do not break tests.
3. **CI security jobs** (`.github/workflows/ci.yml`): add a `security` job — `pip-audit` (or `uv pip audit`) on requirements.txt (non-blocking `continue-on-error: true` first pass, note in README why), and Trivy filesystem/config scan of the repo + Dockerfiles (action `aquasecurity/trivy-action`, severity CRIT/HIGH gate on config scan; image scan can be fs-mode to avoid building images in CI).
4. **Digest-pinned base images:** pin the FROM lines in `docker/Dockerfile.*` and the infra-critical images in compose (redpanda, azure-sql-edge, prometheus, grafana) to `@sha256:` digests, with the human-readable tag kept in a comment. Get digests via `docker buildx imagetools inspect` locally (they're already pulled).
5. **Docs:** `docs/governance/security-posture.md` — what's hardened (fail-loud, scanning, pinning, tokenization, synthetic-only data), what's deliberately demo-grade (sa user, no TLS in-cluster, no Key Vault — Key Vault/managed-identity wiring stays a documented terraform TODO), and the threat model line ("local demo of a production shape; secrets discipline is structural, not operational"). README security paragraph links it.

## Acceptance
- `make check` green; `make demo` and `make demo-cdc` come up clean with no behavior change.
- `docker compose config` fails with a clear message when `BANK_DB_PASSWORD` is unset AND demo.env removed (prove once manually, don't automate that negative test).
- CI workflow YAML valid (`act`-free check: just yamllint-level correctness + `gh workflow` syntax if available); security job added.
- No secret value appears anywhere new outside `docker/demo.env` and `.env.example`.
- Grep proves no `:-LocalDev` / `:-change-me` fallback remains in compose.
