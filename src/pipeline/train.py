"""Offline feature build + XGBoost training job for the fraud model.

Data flow
---------
1. Read the TabFormer CSV (sample or full 24M-row file) and map each row to a
   contract-v1 event via ``src.pipeline.ingestion.to_event`` — the exact same
   mapping the streaming producer uses, so offline and online events agree.
2. Group events by ``card_token`` and compute point-in-time-correct windowed
   features with ``src.pipeline.features.compute_offline_card_features``: the
   features attached to event *t* only ever look at events strictly before
   *t* in that card's history. This mirrors what the Spark streaming job
   computes online, which is the whole point of sharing ``features.py``
   between both code paths (no train/serve skew).
3. Split **by time, not randomly**: earliest 64% of rows -> train, next 16%
   -> validation (early stopping + threshold selection), latest 20% -> test
   (final, held-out evaluation only). This nested 80/20-then-80/20 split
   keeps validation and test both strictly in the future relative to train,
   which is the only sound way to evaluate a fraud model meant to predict
   forward in time.
4. Train an XGBoost binary classifier with ``scale_pos_weight = neg/pos``
   (fraud is ~0.1-0.3% of rows) and early stopping on validation PR-AUC
   (``aucpr``), pick a decision threshold that maximizes F1 on the
   validation precision-recall curve, then score the untouched test split at
   that threshold for the reported metrics.
5. Persist four artifacts under ``MODEL_DIR``: the native XGBoost model, the
   chosen threshold, the exact ordered feature-column list (so the scoring
   service builds its feature vector in the same order), and a metrics
   summary for the README / Key Results.

Two loading/feature-build paths
--------------------------------
- **Row-wise** (``load_events`` + ``build_dataset``): builds one Python dict
  per event via ``to_event`` and validates each with the compiled jsonschema
  validator, then computes window features per card with
  ``compute_offline_card_features``. This is the reference implementation —
  correct and easy to audit — but at 24M rows the per-row dict/hashlib/
  jsonschema/datetime-parsing overhead makes it impractically slow and
  memory-heavy (24M live Python dicts).
- **Vectorized** (``load_events_vectorized`` + ``build_dataset_vectorized``):
  a chunked, pandas/numpy fast path for the full TabFormer file. It mirrors
  the contract with vectorized sanity filters (required fields non-null,
  amount parseable, time/MCC in range) instead of per-row jsonschema, hashes
  ``card_token`` only over the small set of *unique* (user, card) pairs
  (looked up via a dict for the full column) instead of once per row, and
  computes window features via ``features.compute_card_features_vectorized``
  (prefix-sum + two-pointer, see that function's docstring). It is
  numerically equivalent to the row-wise path (see
  ``tests/test_features.py::test_vectorized_matches_row_wise_on_sample``) and
  is what ``train_model`` uses automatically for large inputs (see ``fast``
  parameter / ``--fast``/``--no-fast`` CLI flags).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import xgboost as xgb
from dotenv import load_dotenv
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)

from src.pipeline.features import (
    FEATURE_COLUMNS,
    build_feature_row,
    compute_card_features_vectorized,
    compute_offline_card_features,
    enrich_vectorized,
)
from src.pipeline.ingestion import (
    CHANNEL_MAP,
    COUNTRY_NAME_TO_ISO2,
    TOKENIZATION_SALT,
    US_STATE_CODES,
    to_event,
    validate_event,
)

load_dotenv()

PRODUCER_INPUT_CSV = os.environ.get("PRODUCER_INPUT_CSV", "data/sample/transactions_sample.csv")
MODEL_DIR = os.environ.get("MODEL_DIR", "models")

LABEL_COL = "is_fraud"
TIME_COL = "event_time"

# Above this input file size, train_model defaults to the vectorized fast
# path (a cheap proxy for "large row count" that avoids pre-scanning the
# whole file just to count lines). Override explicitly with --fast/--no-fast.
FAST_PATH_SIZE_THRESHOLD_BYTES = 20 * 1024 * 1024  # 20 MB

_CSV_USECOLS = [
    "User",
    "Card",
    "Year",
    "Month",
    "Day",
    "Time",
    "Amount",
    "Use Chip",
    "Merchant City",
    "Merchant State",
    "MCC",
    "Errors?",
    "Is Fraud?",
]
_CSV_DTYPES = {
    "User": "int64",
    "Card": "int64",
    "Year": "int32",
    "Month": "int32",
    "Day": "int32",
    "Time": "string",
    "Amount": "string",
    "Use Chip": "string",
    "Merchant City": "string",
    "Merchant State": "string",
    "MCC": "string",
    "Errors?": "string",
    "Is Fraud?": "string",
}


def load_events(
    input_path: str,
    salt: str,
    since_year: int | None = None,
    until_year: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Read the CSV and map every row to a contract-v1 event via to_event.

    Returns (valid_events, skipped_count). Rows that fail mapping or contract
    validation are skipped and counted rather than crashing the training run
    (mirrors the producer's DLQ behavior, minus the actual DLQ publish).
    """
    events: list[dict[str, Any]] = []
    skipped = 0
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if since_year is not None or until_year is not None:
                try:
                    year = int(row["Year"])
                except (KeyError, ValueError, TypeError):
                    skipped += 1
                    continue
                if since_year is not None and year < since_year:
                    continue
                if until_year is not None and year > until_year:
                    continue
            try:
                event = to_event(row, salt)
            except (KeyError, ValueError):
                skipped += 1
                continue
            if validate_event(event) is not None:
                skipped += 1
                continue
            events.append(event)
    return events, skipped


def build_dataset(events: list[dict[str, Any]]) -> pd.DataFrame:
    """Group events by card_token, compute leakage-safe window features per
    card, then assemble the full feature matrix (+ label + event_time)."""
    by_card: dict[str, list[dict[str, Any]]] = {}
    for e in events:
        by_card.setdefault(e["card_token"], []).append(e)

    rows: list[dict[str, Any]] = []
    for card_events in by_card.values():
        window_features = compute_offline_card_features(card_events)
        for event, wf in zip(card_events, window_features, strict=True):
            row = build_feature_row(event, wf)
            row[TIME_COL] = event["event_time"]
            row[LABEL_COL] = int(bool(event.get("is_fraud")))
            rows.append(row)

    df = pd.DataFrame(rows)
    df = df.sort_values(TIME_COL, kind="mergesort").reset_index(drop=True)
    return df


def _resolve_country_vectorized(merchant_state: pd.Series, channel: pd.Series) -> pd.Series:
    """Vectorized equivalent of ingestion._resolve_country."""
    state_upper = merchant_state.str.upper()
    is_us = state_upper.isin(US_STATE_CODES)
    country_from_name = merchant_state.map(COUNTRY_NAME_TO_ISO2)
    resolved = np.where(
        (channel == "online").to_numpy(),
        "XX",
        np.where(is_us.to_numpy(), "US", country_from_name.fillna("XX").to_numpy()),
    )
    return pd.Series(resolved, index=merchant_state.index)


def _process_chunk(
    chunk: pd.DataFrame, salt: str, token_cache: dict[str, str]
) -> tuple[pd.DataFrame, int]:
    """Vectorized equivalent of mapping+validating a batch of raw CSV rows.

    Applies sanity filters mirroring contracts/transaction.schema.json
    (required fields present, amount parseable, time/MCC in range) instead of
    per-row jsonschema validation, then derives the same fields `to_event`
    does — but with whole-column vector ops instead of a Python loop.
    """
    n_before = len(chunk)

    required_notna = (
        chunk["User"].notna()
        & chunk["Card"].notna()
        & chunk["Year"].notna()
        & chunk["Month"].notna()
        & chunk["Day"].notna()
        & chunk["Time"].notna()
        & chunk["Amount"].notna()
        & chunk["Use Chip"].notna()
        & chunk["MCC"].notna()
    )

    amount_numeric = pd.to_numeric(
        chunk["Amount"].str.replace("$", "", regex=False).str.replace(",", "", regex=False),
        errors="coerce",
    )

    time_parts = chunk["Time"].str.split(":", expand=True)
    if time_parts.shape[1] < 2:
        time_parts[1] = pd.NA
    hour = pd.to_numeric(time_parts[0], errors="coerce")
    minute = pd.to_numeric(time_parts[1], errors="coerce")
    time_ok = hour.notna() & minute.notna() & hour.between(0, 23) & minute.between(0, 59)

    mcc_numeric = pd.to_numeric(chunk["MCC"], errors="coerce")
    mcc_ok = mcc_numeric.notna() & mcc_numeric.between(0, 9999)

    valid_mask = (required_notna & amount_numeric.notna() & time_ok & mcc_ok).to_numpy()
    skipped = int(n_before - valid_mask.sum())

    chunk = chunk.loc[valid_mask]
    amount_numeric = amount_numeric.loc[valid_mask]
    hour = hour.loc[valid_mask].astype("int64")
    minute = minute.loc[valid_mask].astype("int64")
    mcc_numeric = mcc_numeric.loc[valid_mask].astype("int64")

    if len(chunk) == 0:
        return chunk.iloc[0:0], skipped

    event_time = pd.to_datetime(
        {
            "year": chunk["Year"].astype("int64"),
            "month": chunk["Month"].astype("int64"),
            "day": chunk["Day"].astype("int64"),
            "hour": hour,
            "minute": minute,
        },
        utc=True,
    )

    use_chip = chunk["Use Chip"].str.strip()
    channel = use_chip.map(CHANNEL_MAP).fillna("online")

    merchant_state_raw = chunk["Merchant State"].fillna("").str.strip()
    merchant_country = _resolve_country_vectorized(merchant_state_raw, channel)

    merchant_city = chunk["Merchant City"].fillna("").str.strip()

    errors_raw = chunk["Errors?"]
    has_error = (errors_raw.notna() & (errors_raw.str.strip() != "")).to_numpy()

    is_fraud = (chunk["Is Fraud?"].fillna("").str.strip() == "Yes").astype("int64")

    user_str = chunk["User"].astype("int64").astype(str)
    card_str = chunk["Card"].astype("int64").astype(str)
    user_card_key = user_str + ":" + card_str

    for key in pd.unique(user_card_key.to_numpy()):
        if key not in token_cache:
            token_cache[key] = hashlib.sha256(f"{salt}:{key}".encode("utf-8")).hexdigest()
    card_token = user_card_key.map(token_cache)

    out = pd.DataFrame(
        {
            "card_token": card_token.to_numpy(),
            "event_time": event_time.to_numpy(),
            "amount": amount_numeric.to_numpy(dtype="float64"),
            "channel": channel.to_numpy(),
            "merchant_country": merchant_country.to_numpy(),
            "merchant_city": merchant_city.to_numpy(),
            "mcc": mcc_numeric.to_numpy(),
            "has_error": has_error,
            "is_fraud": is_fraud.to_numpy(),
        }
    )
    return out, skipped


_EVENTS_DF_COLUMNS = [
    "card_token",
    "event_time",
    "amount",
    "channel",
    "merchant_country",
    "merchant_city",
    "mcc",
    "has_error",
    "is_fraud",
]


def load_events_vectorized(
    input_path: str,
    salt: str = TOKENIZATION_SALT,
    since_year: int | None = None,
    until_year: int | None = None,
    chunksize: int = 2_000_000,
) -> tuple[pd.DataFrame, int]:
    """Chunked, pandas-vectorized loader for the full TabFormer CSV.

    No per-row Python dicts and no per-row jsonschema validation (see module
    docstring) — vectorized sanity filters mirror the contract instead.
    `card_token` is hashed once per UNIQUE (user, card) pair (cached across
    chunks) rather than once per row. Returns (events_df, skipped_count)
    where events_df has columns `_EVENTS_DF_COLUMNS`.
    """
    token_cache: dict[str, str] = {}
    chunks: list[pd.DataFrame] = []
    total_skipped = 0

    reader = pd.read_csv(
        input_path,
        usecols=_CSV_USECOLS,
        dtype=_CSV_DTYPES,
        chunksize=chunksize,
    )
    for chunk in reader:
        if since_year is not None:
            chunk = chunk[chunk["Year"] >= since_year]
        if until_year is not None:
            chunk = chunk[chunk["Year"] <= until_year]
        if len(chunk) == 0:
            continue
        processed, skipped = _process_chunk(chunk, salt, token_cache)
        total_skipped += skipped
        if len(processed):
            chunks.append(processed)

    if not chunks:
        return pd.DataFrame(columns=_EVENTS_DF_COLUMNS), total_skipped

    events_df = pd.concat(chunks, ignore_index=True)
    return events_df, total_skipped


def build_dataset_vectorized(events_df: pd.DataFrame) -> pd.DataFrame:
    """Vectorized equivalent of `build_dataset`: window features via
    `compute_card_features_vectorized` + enrichment via `enrich_vectorized`,
    assembled into the same FEATURE_COLUMNS + label + event_time shape."""
    window_features = compute_card_features_vectorized(events_df)
    enriched = enrich_vectorized(
        events_df["channel"],
        events_df["merchant_country"],
        events_df["amount"],
        events_df["mcc"],
        events_df["event_time"],
    )

    df = pd.concat(
        [events_df[["amount"]].reset_index(drop=True), enriched.reset_index(drop=True)],
        axis=1,
    )
    for col in window_features.columns:
        df[col] = window_features[col].to_numpy()
    df[TIME_COL] = events_df["event_time"].to_numpy()
    df[LABEL_COL] = events_df["is_fraud"].astype("int64").to_numpy()

    df = df.sort_values(TIME_COL, kind="mergesort").reset_index(drop=True)
    return df


def time_based_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Earliest 64% -> train, next 16% -> valid, latest 20% -> test.

    No shuffling: `df` must already be sorted by TIME_COL ascending.
    """
    n = len(df)
    test_cut = int(n * 0.8)
    train_full = df.iloc[:test_cut]
    test = df.iloc[test_cut:]

    valid_cut = int(len(train_full) * 0.8)
    train = train_full.iloc[:valid_cut]
    valid = train_full.iloc[valid_cut:]
    return train, valid, test


def select_threshold(y_valid: np.ndarray, prob_valid: np.ndarray) -> float:
    """Maximize F1 on the validation precision-recall curve."""
    precision, recall, thresholds = precision_recall_curve(y_valid, prob_valid)
    # precision/recall have one more element than thresholds; align by dropping the last.
    f1 = np.where(
        (precision[:-1] + recall[:-1]) > 0,
        2 * precision[:-1] * recall[:-1] / (precision[:-1] + recall[:-1]),
        0.0,
    )
    best_idx = int(np.argmax(f1))
    return float(thresholds[best_idx])


def train_model(
    input_path: str,
    salt: str = TOKENIZATION_SALT,
    model_dir: str = MODEL_DIR,
    since_year: int | None = None,
    until_year: int | None = None,
    fast: bool | None = None,
) -> dict[str, Any]:
    """End-to-end: load -> features -> split -> train -> threshold -> persist.

    `fast=None` (default) auto-selects the vectorized path for inputs larger
    than FAST_PATH_SIZE_THRESHOLD_BYTES (a cheap file-size proxy for "large
    row count" — see module docstring for why row-wise doesn't scale to 24M
    rows) and the row-wise reference path otherwise. Pass True/False to force
    a path regardless of input size.

    Returns the metrics dict that is also written to models/metrics.json.
    """
    if fast is None:
        try:
            file_size = os.path.getsize(input_path)
        except OSError:
            file_size = 0
        fast = file_size > FAST_PATH_SIZE_THRESHOLD_BYTES

    if fast:
        events_df, skipped = load_events_vectorized(
            input_path, salt, since_year=since_year, until_year=until_year
        )
        if len(events_df) == 0:
            raise ValueError(f"no valid events loaded from {input_path}")
        df = build_dataset_vectorized(events_df)
    else:
        events, skipped = load_events(input_path, salt, since_year=since_year, until_year=until_year)
        if not events:
            raise ValueError(f"no valid events loaded from {input_path}")
        df = build_dataset(events)

    train_df, valid_df, test_df = time_based_split(df)

    x_train = train_df[FEATURE_COLUMNS].astype(float)
    y_train = train_df[LABEL_COL].to_numpy()
    x_valid = valid_df[FEATURE_COLUMNS].astype(float)
    y_valid = valid_df[LABEL_COL].to_numpy()
    x_test = test_df[FEATURE_COLUMNS].astype(float)
    y_test = test_df[LABEL_COL].to_numpy()

    pos = int(y_train.sum())
    neg = int(len(y_train) - pos)
    scale_pos_weight = (neg / pos) if pos > 0 else 1.0

    dtrain = xgb.DMatrix(x_train, label=y_train, feature_names=FEATURE_COLUMNS)
    dvalid = xgb.DMatrix(x_valid, label=y_valid, feature_names=FEATURE_COLUMNS)
    dtest = xgb.DMatrix(x_test, label=y_test, feature_names=FEATURE_COLUMNS)

    params = {
        "objective": "binary:logistic",
        "eval_metric": "aucpr",
        "scale_pos_weight": scale_pos_weight,
        "max_depth": 6,
        "eta": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "tree_method": "hist",
    }

    booster = xgb.train(
        params,
        dtrain,
        num_boost_round=300,
        evals=[(dtrain, "train"), (dvalid, "valid")],
        early_stopping_rounds=20,
        verbose_eval=False,
    )

    prob_valid = booster.predict(dvalid, iteration_range=(0, booster.best_iteration + 1))
    threshold = select_threshold(y_valid, prob_valid)

    prob_test = booster.predict(dtest, iteration_range=(0, booster.best_iteration + 1))
    pred_test = (prob_test >= threshold).astype(int)

    pr_auc = float(average_precision_score(y_test, prob_test))
    roc_auc = float(roc_auc_score(y_test, prob_test)) if len(np.unique(y_test)) > 1 else float("nan")
    precision = float(precision_score(y_test, pred_test, zero_division=0))
    recall = float(recall_score(y_test, pred_test, zero_division=0))
    f1 = float(f1_score(y_test, pred_test, zero_division=0))
    tn, fp, fn, tp = confusion_matrix(y_test, pred_test, labels=[0, 1]).ravel()

    os.makedirs(model_dir, exist_ok=True)
    booster.save_model(os.path.join(model_dir, "model.json"))

    with open(os.path.join(model_dir, "threshold.json"), "w", encoding="utf-8") as f:
        json.dump({"threshold": threshold, "chosen_by": "max_f1_on_validation_pr_curve"}, f, indent=2)

    with open(os.path.join(model_dir, "feature_columns.json"), "w", encoding="utf-8") as f:
        json.dump(FEATURE_COLUMNS, f, indent=2)

    metrics = {
        "pr_auc": pr_auc,
        "roc_auc": roc_auc,
        "threshold": threshold,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "confusion": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
        "rows": {
            "train": int(len(train_df)),
            "valid": int(len(valid_df)),
            "test": int(len(test_df)),
            "skipped_invalid": int(skipped),
        },
        "fraud_rate": {
            "train": float(y_train.mean()) if len(y_train) else 0.0,
            "valid": float(y_valid.mean()) if len(y_valid) else 0.0,
            "test": float(y_test.mean()) if len(y_test) else 0.0,
        },
        "best_iteration": int(booster.best_iteration),
        "scale_pos_weight": scale_pos_weight,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "input_path": input_path,
        "since_year": since_year,
        "until_year": until_year,
        "fast_path_used": bool(fast),
    }
    if metrics["fraud_rate"]["test"] == 0.0 or metrics["fraud_rate"]["valid"] == 0.0:
        metrics["notes"] = (
            "One or more splits has zero fraud rows under the strict chronological "
            "64/16/20 split — this reflects the input's fraud time distribution (fraud "
            "labels clustered earlier than the bulk of transaction volume), not a bug "
            "in the split logic. Precision/recall/F1/ROC-AUC on a zero-fraud split are "
            "degenerate by construction; widen --since-year/--until-year or use more "
            "data to get a split with fraud present in all three partitions."
        )
    # roc_auc is NaN when a split has a single class present; NaN is not valid
    # JSON, so normalize to null for the persisted artifact.
    if isinstance(metrics.get("roc_auc"), float) and math.isnan(metrics["roc_auc"]):
        metrics["roc_auc"] = None

    with open(os.path.join(model_dir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    return metrics


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Train the XGBoost fraud model on TabFormer events.")
    parser.add_argument("--input", default=PRODUCER_INPUT_CSV, help="Path to source CSV.")
    parser.add_argument("--model-dir", default=MODEL_DIR, help="Output directory for model artifacts.")
    parser.add_argument(
        "--since-year", type=int, default=None, help="Drop rows with Year < this value."
    )
    parser.add_argument(
        "--until-year", type=int, default=None, help="Drop rows with Year > this value."
    )
    parser.add_argument(
        "--fast",
        dest="fast",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Force the vectorized (--fast) or row-wise (--no-fast) feature-build path. "
            f"Default: auto-select vectorized for inputs > {FAST_PATH_SIZE_THRESHOLD_BYTES // (1024 * 1024)}MB."
        ),
    )
    args = parser.parse_args(argv)

    metrics = train_model(
        args.input,
        model_dir=args.model_dir,
        since_year=args.since_year,
        until_year=args.until_year,
        fast=args.fast,
    )
    print(f"fast_path_used={metrics['fast_path_used']}")
    roc_auc = metrics["roc_auc"]
    roc_auc_str = f"{roc_auc:.4f}" if roc_auc is not None else "n/a (single class in test split)"
    print(f"PR-AUC: {metrics['pr_auc']:.4f}  ROC-AUC: {roc_auc_str}")
    print(
        f"threshold={metrics['threshold']:.4f} precision={metrics['precision']:.4f} "
        f"recall={metrics['recall']:.4f} f1={metrics['f1']:.4f}"
    )
    print(f"rows: {metrics['rows']}")


if __name__ == "__main__":
    main(sys.argv[1:])
