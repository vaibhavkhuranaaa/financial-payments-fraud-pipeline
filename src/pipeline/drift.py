"""Population Stability Index (PSI) drift detection (ticket 17, v1.5b).

Data flow
---------
1. **At train time** (`src.pipeline.train`), `build_reference_stats` turns a
   training split's `amount` / `mcc` / `channel` columns into a reference
   distribution — quantile-bin counts for the numeric feature, category
   frequency counts for the two categorical ones — and that dict is persisted
   verbatim into `manifest.json`'s `"drift_reference"` key alongside the rest
   of the run's provenance.
2. **At check time**, `--check` (this module's CLI) or the lag exporter's
   periodic drift check (`maybe_update_drift` in `lag_exporter.py`) reads
   recent rows straight out of `bank.scored_transactions` — which stores
   `amount`, `mcc`, `channel` verbatim from the scored contract-v1 event, so
   no re-derivation or Redis join is needed — bins/counts them the same way,
   and compares against the reference with `psi_from_counts`.

Why these three features and not the full `FEATURE_COLUMNS` set: most model
features (the window aggregates, `is_new_city_30d`, `time_since_last_txn_s`,
...) only exist online in Redis's per-card `features:{card_token}` hash, not
in a population-level, queryable-at-scale form — joining every recent scored
row back to its point-in-time Redis state would need a time-travel feature
store this project doesn't have. `amount`, `mcc`, and `channel` are exactly
the raw transaction attributes both `bank.scored_transactions` stores AND the
model conditions its score on (via `amount`/`amount_log`, `mcc`/`mcc_group_id`,
`is_cnp`/`is_chip`/`is_swipe`) — the honest "top features" set for a
population-drift check without a feature-store dependency. See ADR 0005.

PSI thresholds (Yurdakul 2018 convention, used broadly in credit-risk
monitoring): < 0.1 no significant shift, 0.1-0.2 moderate shift worth a look,
> 0.2 significant shift — this module's default `--check` exit threshold.

Testability
-----------
`psi_from_counts`, `numeric_bin_edges`, `bin_numeric`, `count_categorical`,
`build_reference_stats`, and `compute_psi_table` are pure/hermetic — no I/O.
`fetch_recent_scored_transactions` takes a SQLAlchemy engine so tests mock it;
`load_reference_stats` takes a `model_dir` and reads plain JSON.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import Engine, text

load_dotenv()

logger = logging.getLogger(__name__)

MODEL_DIR = os.environ.get("MODEL_DIR", "models")

# The "top features" this module monitors — see module docstring for why
# these three and not the full FEATURE_COLUMNS set.
DRIFT_NUMERIC_FEATURES = ["amount"]
DRIFT_CATEGORICAL_FEATURES = ["mcc", "channel"]
DRIFT_FEATURES = DRIFT_NUMERIC_FEATURES + DRIFT_CATEGORICAL_FEATURES

DEFAULT_PSI_THRESHOLD = 0.2
DEFAULT_N_BINS = 10
DEFAULT_CHECK_LIMIT = 5000
# Standard PSI floor: bins with 0 share on either side would make the log
# term +-inf; epsilon keeps a genuinely-empty/unseen bin's contribution
# large-but-finite instead of crashing the check.
PSI_EPSILON = 1e-4

_SCORED_TXN_QUERY_COLUMNS = ("amount", "mcc", "channel")


# --- Pure PSI math ----------------------------------------------------------


def psi_from_counts(
    reference_counts: dict[str, int],
    current_counts: dict[str, int],
    epsilon: float = PSI_EPSILON,
) -> float:
    """Population Stability Index between two frequency distributions over
    the same bin/category keys.

    PSI = sum((cur_share - ref_share) * ln(cur_share / ref_share)) over the
    union of keys seen on either side. A key present in `current_counts` but
    not `reference_counts` (an unseen category, or a numeric bin nothing in
    the reference training split fell into) gets a reference share of
    `epsilon` rather than 0 — and symmetrically for a reference-only key —
    so drift onto/away-from a genuinely new bucket registers as a large,
    finite PSI contribution instead of a ZeroDivisionError/±inf.

    Returns 0.0 if both sides are empty (no signal either way).
    """
    ref_total = sum(reference_counts.values())
    cur_total = sum(current_counts.values())
    if ref_total == 0 and cur_total == 0:
        return 0.0

    keys = set(reference_counts) | set(current_counts)
    psi = 0.0
    for key in keys:
        ref_share = max(reference_counts.get(key, 0) / ref_total, epsilon) if ref_total else epsilon
        cur_share = max(current_counts.get(key, 0) / cur_total, epsilon) if cur_total else epsilon
        psi += (cur_share - ref_share) * np.log(cur_share / ref_share)
    return float(psi)


def numeric_bin_edges(values: np.ndarray, n_bins: int = DEFAULT_N_BINS) -> list[float]:
    """Quantile bin edges for `values`, fixed once at reference (train) time
    and reused unchanged at check time — that's what makes PSI comparable.

    Falls back to a single [min, max] bin (or [0, 1] on totally empty input)
    when `values` has too little variation for `n_bins` distinct quantile
    edges (e.g. a constant column) — `np.unique` collapsing duplicate
    quantiles is the usual cause.
    """
    values = np.asarray(values, dtype="float64")
    values = values[~np.isnan(values)]
    if len(values) == 0:
        return [0.0, 1.0]
    quantiles = np.linspace(0.0, 1.0, n_bins + 1)
    edges = np.unique(np.quantile(values, quantiles))
    if len(edges) < 2:
        lo, hi = float(values.min()), float(values.max())
        if lo == hi:
            lo, hi = lo - 0.5, hi + 0.5
        edges = np.array([lo, hi])
    # Guarantee the outer edges cover any future out-of-range value.
    edges[0] = -np.inf
    edges[-1] = np.inf
    return edges.tolist()


def bin_numeric(values: np.ndarray, edges: list[float]) -> dict[str, int]:
    """Count `values` into the bins defined by `edges` (as produced by
    `numeric_bin_edges`). Bin keys are stringified left-edge indices so they
    round-trip through JSON (manifest.json) as plain string dict keys."""
    values = np.asarray(values, dtype="float64")
    values = values[~np.isnan(values)]
    counts: dict[str, int] = {}
    if len(values) == 0:
        return counts
    edges_arr = np.asarray(edges, dtype="float64")
    # right=False bins as [edge_i, edge_i+1); clip keeps NaN-free values that
    # equal the extended +-inf edges inside range (they always do).
    idx = np.clip(np.digitize(values, edges_arr, right=False) - 1, 0, len(edges_arr) - 2)
    unique, freq = np.unique(idx, return_counts=True)
    for bin_idx, count in zip(unique.tolist(), freq.tolist(), strict=True):
        counts[str(bin_idx)] = int(count)
    return counts


def count_categorical(values: Any) -> dict[str, int]:
    """Frequency count of `values` (any iterable), stringified for JSON keys.
    Empty input returns `{}` (an empty-bins edge case `psi_from_counts`
    handles via its ref_total/cur_total == 0 guard)."""
    series = pd.Series(list(values)) if not isinstance(values, pd.Series) else values
    series = series.dropna()
    if len(series) == 0:
        return {}
    return {str(k): int(v) for k, v in series.astype(str).value_counts().items()}


# --- Reference-stats construction (train time) ------------------------------


def build_reference_stats(
    df: pd.DataFrame,
    numeric_features: list[str] = DRIFT_NUMERIC_FEATURES,
    categorical_features: list[str] = DRIFT_CATEGORICAL_FEATURES,
    n_bins: int = DEFAULT_N_BINS,
) -> dict[str, dict[str, Any]]:
    """Build the `manifest.json["drift_reference"]` payload from a training
    split. `df` must have every column named in `numeric_features` /
    `categorical_features` (train.py passes the train split with a
    reconstructed `channel` column — see train.py's `_drift_frame`)."""
    reference: dict[str, dict[str, Any]] = {}
    for feature in numeric_features:
        values = df[feature].to_numpy(dtype="float64")
        edges = numeric_bin_edges(values, n_bins=n_bins)
        reference[feature] = {
            "type": "numeric",
            "edges": edges,
            "counts": bin_numeric(values, edges),
            "mean": float(np.mean(values)) if len(values) else 0.0,
            "std": float(np.std(values)) if len(values) else 0.0,
        }
    for feature in categorical_features:
        reference[feature] = {
            "type": "categorical",
            "counts": count_categorical(df[feature]),
        }
    return reference


# --- Current-vs-reference PSI (check time) -----------------------------------


def feature_psi(reference_entry: dict[str, Any], current_values: Any) -> float:
    """PSI for one feature: dispatches on `reference_entry["type"]`."""
    if reference_entry["type"] == "numeric":
        current_counts = bin_numeric(
            np.asarray(list(current_values), dtype="float64"), reference_entry["edges"]
        )
    else:
        current_counts = count_categorical(current_values)
    return psi_from_counts(reference_entry["counts"], current_counts)


def compute_psi_table(
    reference_stats: dict[str, dict[str, Any]], current_df: pd.DataFrame
) -> dict[str, float]:
    """PSI per feature in `reference_stats`, vs the matching column of
    `current_df`. A feature missing from `current_df` (e.g. a partial/failed
    query result) is skipped rather than raising."""
    result: dict[str, float] = {}
    for feature, entry in reference_stats.items():
        if feature not in current_df.columns:
            continue
        result[feature] = feature_psi(entry, current_df[feature])
    return result


# --- Manifest / pointer resolution -------------------------------------------
#
# Deliberately NOT shared with src.app._resolve_model_dir: importing src.app
# here would execute its module-level `app = create_app()` (a real Flask app
# that tries to load a model and connect to Redis) as a side effect of
# importing this module for a CLI drift check. The resolution logic itself
# is ~10 lines; duplicating it keeps drift.py import-safe and dependency-free
# of Flask/redis.


def _resolve_run_dir(model_dir: str) -> str:
    """Same pointer-file convention as src.app: `<model_dir>/current.json`
    -> `{"run_id": ...}` selects `<model_dir>/<run_id>/`; falls back to the
    flat `model_dir` layout if there's no pointer or it's stale."""
    pointer_path = os.path.join(model_dir, "current.json")
    if os.path.exists(pointer_path):
        try:
            with open(pointer_path, encoding="utf-8") as f:
                run_id = json.load(f).get("run_id")
        except (OSError, json.JSONDecodeError):
            run_id = None
        if run_id:
            run_dir = os.path.join(model_dir, str(run_id))
            if os.path.isdir(run_dir):
                return run_dir
            logger.warning("current.json points at missing run dir %s", run_dir)
    return model_dir


def load_reference_stats(model_dir: str = MODEL_DIR) -> dict[str, dict[str, Any]] | None:
    """Load `drift_reference` from the current run's manifest.json. Returns
    None if there's no manifest (flat legacy layout predates this ticket) or
    it has no drift_reference key — callers must treat that as "drift
    checking unavailable", not an error."""
    run_dir = _resolve_run_dir(model_dir)
    manifest_path = os.path.join(run_dir, "manifest.json")
    if not os.path.exists(manifest_path):
        return None
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)
    return manifest.get("drift_reference")


def fetch_recent_scored_transactions(engine: Engine, limit: int = DEFAULT_CHECK_LIMIT) -> pd.DataFrame:
    """Most recent `limit` rows of `bank.scored_transactions`, the columns
    this module's drift check needs. `limit` is validated as a positive int
    before interpolation (T-SQL TOP takes no bind parameter here) — never
    pass through unsanitized user input."""
    limit = int(limit)
    if limit <= 0:
        raise ValueError(f"limit must be positive, got {limit}")
    query = text(
        f"SELECT TOP {limit} amount, mcc, channel "  # noqa: S608 — fixed columns, validated int limit
        "FROM bank.scored_transactions ORDER BY scored_at DESC"
    )
    with engine.connect() as conn:
        return pd.DataFrame(conn.execute(query).fetchall(), columns=list(_SCORED_TXN_QUERY_COLUMNS))


def check_drift(
    engine: Engine,
    reference_stats: dict[str, dict[str, Any]],
    limit: int = DEFAULT_CHECK_LIMIT,
    threshold: float = DEFAULT_PSI_THRESHOLD,
) -> tuple[dict[str, float], bool]:
    """Fetch recent scored transactions, compute PSI per feature. Returns
    (psi_by_feature, exceeded) where exceeded is True iff any feature's PSI
    is above `threshold`."""
    current_df = fetch_recent_scored_transactions(engine, limit=limit)
    psi_by_feature = compute_psi_table(reference_stats, current_df)
    exceeded = any(psi > threshold for psi in psi_by_feature.values())
    return psi_by_feature, exceeded


def format_drift_table(psi_by_feature: dict[str, float], threshold: float = DEFAULT_PSI_THRESHOLD) -> str:
    lines = [f"{'feature':<20}{'psi':>10}   status", "-" * 42]
    for feature, psi in sorted(psi_by_feature.items()):
        status = "DRIFT" if psi > threshold else "ok"
        lines.append(f"{feature:<20}{psi:>10.4f}   {status}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Check current scored-transaction drift vs training reference.")
    parser.add_argument("--check", action="store_true", required=True, help="Run the drift check (only supported mode).")
    parser.add_argument("--model-dir", default=MODEL_DIR, help="Base model dir (pointer + run dirs, or flat legacy).")
    parser.add_argument("--limit", type=int, default=DEFAULT_CHECK_LIMIT, help="Recent scored_transactions rows to sample.")
    parser.add_argument("--threshold", type=float, default=DEFAULT_PSI_THRESHOLD, help="PSI above this exits non-zero.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    reference_stats = load_reference_stats(args.model_dir)
    if reference_stats is None:
        print(f"no drift reference found under {args.model_dir} (no manifest.json / drift_reference key)")
        sys.exit(2)

    from src.bank.db import get_engine

    engine = get_engine()
    try:
        psi_by_feature, exceeded = check_drift(engine, reference_stats, limit=args.limit, threshold=args.threshold)
    except Exception as exc:  # noqa: BLE001 — CLI: report cleanly, don't dump a DB-driver traceback
        print(f"drift check failed: {exc}")
        sys.exit(3)
    print(format_drift_table(psi_by_feature, threshold=args.threshold))
    sys.exit(1 if exceeded else 0)


if __name__ == "__main__":
    main(sys.argv[1:])
