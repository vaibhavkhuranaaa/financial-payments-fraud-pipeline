# Ticket 10 — v1.1 docs, governance, demo script

**Owner:** Docs subagent. **Scope:** `README.md`, `docs/adr/0002-*.md`, `docs/governance/*`, `docs/STATE.md` (phase table only), `.env.example` comments. Do not touch `src/`, `docker/`, `infra/` code.

## Context
- Read `CLAUDE.md`, `docs/STATE.md`, tickets 06–09 and the code they produced, `docs/adr/0001-stack-and-architecture.md` (format to follow), `docs/governance/lineage.md` + `data-dictionary.md`.
- v1.1 = SQL Edge system-of-record + scorer loop + Dash fraud-ops dashboard + `make demo`. Costs: local $0; optional Azure demo ~$1.50–2.50/day, teardown-tested pattern from v1.0.

## Deliverables
1. **ADR 0002**: why Azure SQL Edge (SQL-Server family, ARM-native, $0 local) over full SQL Server/Postgres/Azure SQL; why a separate scorer consumer instead of scoring inside Spark (latency isolation, API stays the single scoring path — no skew); why Plotly Dash (pure Python, no CDN, code-only); cost table.
2. **README**: architecture diagram gains bank-db / scorer / dashboard nodes; new "Run the demo" section (`make demo`, URLs, screenshot placeholder); **"3-minute recruiter demo script"** — an exact talk track: what to say per dashboard panel, the suspicious-transaction moment, the kill-redis resilience beat, the teardown/cost line. Update Tech Stack + What I'd Improve Next (add: CDC from the bank DB instead of CSV replay would be the production-real ingest).
3. **Governance**: lineage.md gains the SQL tables + dashboard read/write paths; data-dictionary.md gains `bank.*` tables (field-level); tokenization-policy note: card_token is the ONLY join key between stream and bank dims — no PAN anywhere, dims are Faker-synthetic.
4. **STATE.md**: phase rows 6 (bank DB), 7 (scorer+dashboard), 8 (demo+docs, tag v1.1) with status; keep the handoff discipline intact.

## Acceptance
- A newcomer can run the demo and deliver the talk track from README alone.
- `make check` green (dbt/docs untouched functionally); no stale window/feature references introduced.
- All ADR/lineage/dictionary cross-references resolve.
