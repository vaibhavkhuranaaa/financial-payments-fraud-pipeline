# ADR 0005 — v1.5b model ops: plain-file artifact versioning + PSI drift detection

**Status:** accepted (2026-07-19). **Ticket:** `docs/tickets/17-model-ops.md`.

## Context

Through v1.5, `src/pipeline/train.py` wrote one model in place: `models/model.json` +
`threshold.json` + `feature_columns.json` + `metrics.json`, overwritten on every retrain.
That's fine for a single demo run, but it has no audit trail (which run is actually
serving? what data/params produced it?) and no way to tell, short of eyeballing
`models/metrics.json`, whether the world the model was trained on still looks like the
world it's scoring today. Two gaps, one ticket: artifact versioning, and drift detection.

## Decisions

1. **Plain-file artifact versioning, not MLflow.** `train.py` now writes to
   `models/<run_id>/` (`run_id` = UTC timestamp + git short SHA, e.g.
   `20260719T120000Z-8db7a55` — sortable, collision-resistant per commit) containing
   `model.json`, `threshold.json`, `feature_columns.json`, `metrics.json`, and a new
   `manifest.json` (training data range, row counts, class balance, package versions,
   git SHA, seed, and the PSI reference distributions — see decision 2).
   `models/current.json` — a pointer file (`{"run_id": ...}`), not a symlink: symlinks
   don't survive a Docker `COPY` faithfully and complicate the image build for no
   benefit here — selects the serving/reference version. `src/app.py` and
   `src/pipeline/drift.py` each resolve it independently (see decision 3 for why not
   shared code): a valid pointer resolves the versioned run dir and reports its
   `run_id` as `model_version`; no pointer, an unreadable one, or one naming a run dir
   that no longer exists on disk all fall back to the flat legacy `models/` layout with
   `model_version: null` — so the artifacts already committed to this repo pre-ticket-17
   keep serving unchanged, with zero migration step required.

   An MLflow tracking server was the other option on the table and is explicitly
   rejected: it's a stateful service this demo would then have to run, back up, and
   explain in the compose stack, for the same audit answers ("what trained this,
   with what data, and is it still valid?") a versioned directory + a manifest already
   gives a reader with zero extra infrastructure. If this pipeline needed multi-model
   comparison dashboards or remote model registries across a team, that calculus
   changes — for one person's demo stack it doesn't.

2. **Population Stability Index (PSI) for drift, computed over `amount` / `mcc` /
   `channel`.** These three are the "top features" this ticket monitors, not the full
   `FEATURE_COLUMNS` set, for a concrete reason: most model features (the 1h/1d/7d/30d
   window aggregates, `is_new_city_30d`, `time_since_last_txn_s`, ...) live only in
   Redis's per-card `features:{card_token}` hash — point-in-time state, not a
   population-level distribution queryable at scale without a time-travel feature
   store this project doesn't have. `amount`, `mcc`, and `channel` are exactly the raw
   transaction attributes `bank.scored_transactions` stores verbatim from the scored
   event AND the fields the model conditions its score on (`amount`/`amount_log`,
   `mcc`/`mcc_group_id`, `is_cnp`/`is_chip`/`is_swipe`) — the honest population-drift
   signal available without adding a feature-store dependency.

   PSI over MLflow's own drift plugins or a stats-test grab-bag (KS test, chi-square)
   because PSI is the credit-risk-industry-standard single number with well-known
   thresholds (< 0.1 stable, 0.1–0.2 moderate, > 0.2 significant — Yurdakul 2018
   convention), works uniformly for numeric (quantile-binned) and categorical
   (frequency-binned) features via the same `psi_from_counts`, and needs no reference
   *model* — just reference *counts*, persisted as plain JSON in the manifest.
   `> 0.2` on any of the three top features is `drift.py --check`'s exit-non-zero
   threshold.

3. **No new service: `drift.py` is an importable module, wired into the existing lag
   exporter.** `src/pipeline/drift.py` exposes pure functions
   (`psi_from_counts`/`build_reference_stats`/`check_drift`/...) plus a `--check` CLI
   entrypoint; `src/pipeline/lag_exporter.py` imports `check_drift` directly and
   publishes `model_psi{feature}` gauges from `maybe_update_drift`, called on its own
   cadence (`DRIFT_CHECK_INTERVAL_S`, independent of the Kafka/bank poll interval)
   inside the exporter's existing poll loop — same failure-isolation pattern as the
   Kafka/bank blocks (`lag_exporter_target_up{target="drift"}`, `..._poll_errors_total`).
   `DRIFT_CHECK_INTERVAL_S` defaults to `0` (off): the obs profile's behavior is
   **unchanged unless explicitly opted into** — no new container, no new exposed
   endpoint, and no risk of an always-on DB query nobody asked for showing up in an
   otherwise idle demo stack. `drift.py`'s own pointer/manifest resolution
   (`_resolve_run_dir`/`load_reference_stats`) is deliberately a small, independent
   copy of `src/app.py`'s `_resolve_model_dir` rather than an import from `src.app`:
   importing `src.app` executes its module-level `app = create_app()` (a real Flask
   app that loads a model and opens a Redis connection) as an import side effect,
   which a CLI drift check or the exporter process has no business triggering.

## Consequences

- Every training run is now individually addressable and auditable
  (`models/<run_id>/manifest.json`); `models/current.json` is the one file that
  changes on retrain, and prior runs are never deleted by `train.py` itself (cleanup,
  if ever needed, is a separate concern this ADR doesn't solve).
- `/healthz` and every `/score` response now report `model_version` — `null` under the
  flat legacy layout, the `run_id` once any versioned run has been trained.
- The drift check's honesty is bounded by its inputs: it monitors the raw transaction
  attributes, not the window/behavioral features that actually carry most of the
  model's signal. A drift alert on `amount`/`mcc`/`channel` is a real, useful early
  warning (input population shifted) but a *clean* PSI reading is not proof the
  windowed features haven't drifted — that would need the time-travel feature store
  this ADR explicitly decided not to build for a demo.
- `DRIFT_CHECK_INTERVAL_S=0` by default means a fresh clone's `make demo OBS=1` shows
  no `model_psi` series until someone opts in — documented here and in `.env.example`
  so that's a legible choice, not a silent gap.
