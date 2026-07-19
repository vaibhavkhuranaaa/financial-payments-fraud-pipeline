# Ticket 17 — v1.5b: Model ops — artifact versioning + drift detection (roadmap item 5)

**Scope:** `src/pipeline/train.py` output layout, `src/app.py` model metadata, `src/pipeline/drift.py`, dashboard/exporter surface. No MLflow server — plain-file versioning (the decision: an MLflow dependency + tracking server is operational overhead a demo can't honestly run; a versioned artifact dir + manifest gives the same audit answers. Record this in the ADR).

## Deliverables
1. **Versioned model artifacts**: `train.py` writes to `models/<run_id>/` (run_id = UTC timestamp + git short SHA) containing model, feature_columns, threshold.json, metrics.json, and a new `manifest.json` (training data range, row counts, class balance, package versions, git SHA, seed). `models/current` symlink (or `current.json` pointer file — symlinks are fragile in Docker COPY; prefer the pointer) selects the serving version; API loads via the pointer and exposes `model_version` in `/healthz` and every `/score` response. Existing flat `models/` files stay working as a fallback (backward compat with the committed artifacts, if any).
2. **Drift detection (`src/pipeline/drift.py`)**: at train time, persist per-feature reference stats (mean/std/quantiles for numerics, category frequencies for categoricals) into the manifest. A `--check` mode reads recent rows from `bank.scored_transactions` (+ features from Redis where applicable), computes PSI per feature vs reference, prints a table, exits non-zero above threshold (PSI > 0.2 on any top feature). Unit-test PSI math hermetically.
3. **Exporter integration**: `model_psi{feature}` gauges pushed from a periodic drift check into the lag exporter (simplest honest wiring: exporter shells the same computation on a slow interval, or drift.py exposes a function the exporter imports — pick the cleaner import path, no new service).
4. **Docs**: ADR 0005 (plain-file versioning over MLflow; PSI choice + threshold), README "Model ops" paragraph, retrain recipe updated in STATE.md environment facts if the command changes.

## Acceptance
- `make check` green; new hermetic tests for manifest writing, pointer resolution, and PSI math.
- Retrain NOT required for acceptance (12 min on full data): verify the new layout with a fast `--since-year 2018` smoke run locally, confirm API serves it and reports `model_version`, and that the old committed artifacts still serve when no run dir exists.
- `drift.py --check` runs against a live demo stack and prints real PSI numbers.
