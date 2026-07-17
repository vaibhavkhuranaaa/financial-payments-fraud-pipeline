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
"""

from __future__ import annotations

import argparse
import csv
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

from src.pipeline.features import FEATURE_COLUMNS, build_feature_row, compute_offline_card_features
from src.pipeline.ingestion import TOKENIZATION_SALT, to_event, validate_event

load_dotenv()

PRODUCER_INPUT_CSV = os.environ.get("PRODUCER_INPUT_CSV", "data/sample/transactions_sample.csv")
MODEL_DIR = os.environ.get("MODEL_DIR", "models")

LABEL_COL = "is_fraud"
TIME_COL = "event_time"


def load_events(input_path: str, salt: str) -> tuple[list[dict[str, Any]], int]:
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
    input_path: str, salt: str = TOKENIZATION_SALT, model_dir: str = MODEL_DIR
) -> dict[str, Any]:
    """End-to-end: load -> features -> split -> train -> threshold -> persist.

    Returns the metrics dict that is also written to models/metrics.json.
    """
    events, skipped = load_events(input_path, salt)
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
        "notes": (
            "The 76,989-row sample is drawn as each user's most-recent transactions, "
            "which skews fraud (0.22% overall) toward the earlier part of the sample's "
            "time range; a strict chronological split can leave few/no fraud rows in "
            "validation/test (see fraud_rate above). This is a property of the sample, "
            "not the split logic — retraining on the full data/raw/card_transaction.v1.csv "
            "is expected to populate all three splits with fraud."
        ),
    }
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
    args = parser.parse_args(argv)

    metrics = train_model(args.input, model_dir=args.model_dir)
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
