"""Baseline, challenger, calibration, and held-out evaluation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

from .data import FEATURE_COLUMNS
from .policy import apply_review_policy, policy_summary


@dataclass(frozen=True)
class TrainingConfig:
    random_state: int = 42
    reviews_per_1000: float = 1.0
    minimum_pr_auc_lift: float = 0.02
    bootstrap_samples: int = 300
    xgb_estimators: int = 260
    xgb_max_depth: int = 4
    xgb_learning_rate: float = 0.05

    def as_dict(self) -> dict[str, int | float]:
        return asdict(self)


class PlattCalibrator:
    """Sigmoid calibration over clipped model logits."""

    def __init__(self) -> None:
        self.model = LogisticRegression(C=1_000_000.0, solver="lbfgs", max_iter=500)

    @staticmethod
    def _logits(probabilities: np.ndarray) -> np.ndarray:
        clipped = np.clip(np.asarray(probabilities, dtype=float), 1e-7, 1 - 1e-7)
        return np.log(clipped / (1 - clipped)).reshape(-1, 1)

    def fit(self, probabilities: np.ndarray, labels: np.ndarray) -> "PlattCalibrator":
        self.model.fit(self._logits(probabilities), labels)
        return self

    def predict(self, probabilities: np.ndarray) -> np.ndarray:
        return self.model.predict_proba(self._logits(probabilities))[:, 1]

    def parameters(self) -> dict[str, float]:
        return {
            "coefficient": float(self.model.coef_[0, 0]),
            "intercept": float(self.model.intercept_[0]),
        }


def _baseline(random_state: int) -> Pipeline:
    return Pipeline(
        [
            ("scale", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    class_weight="balanced",
                    max_iter=1_000,
                    random_state=random_state,
                    solver="lbfgs",
                ),
            ),
        ]
    )


def _challenger(labels: pd.Series, config: TrainingConfig) -> XGBClassifier:
    positives = int(labels.sum())
    negatives = len(labels) - positives
    return XGBClassifier(
        n_estimators=config.xgb_estimators,
        max_depth=config.xgb_max_depth,
        learning_rate=config.xgb_learning_rate,
        min_child_weight=2,
        subsample=0.85,
        colsample_bytree=0.9,
        reg_lambda=5.0,
        objective="binary:logistic",
        eval_metric="aucpr",
        scale_pos_weight=negatives / positives,
        random_state=config.random_state,
        n_jobs=-1,
        tree_method="hist",
    )


def _metrics(labels: np.ndarray, probabilities: np.ndarray) -> dict[str, float]:
    return {
        "pr_auc": float(average_precision_score(labels, probabilities)),
        "roc_auc": float(roc_auc_score(labels, probabilities)),
        "brier": float(brier_score_loss(labels, probabilities)),
        "log_loss": float(log_loss(labels, probabilities, labels=[0, 1])),
    }


def _bootstrap_pr_auc(
    labels: np.ndarray,
    probabilities: np.ndarray,
    samples: int,
    random_state: int,
) -> dict[str, float | int]:
    rng = np.random.default_rng(random_state)
    values: list[float] = []
    indices = np.arange(len(labels))
    for _ in range(samples):
        draw = rng.choice(indices, size=len(indices), replace=True)
        if np.unique(labels[draw]).size == 2:
            values.append(
                float(average_precision_score(labels[draw], probabilities[draw]))
            )
    if not values:
        return {"low": 0.0, "high": 0.0, "samples": 0}
    low, high = np.quantile(values, [0.025, 0.975])
    return {"low": float(low), "high": float(high), "samples": len(values)}


def capacity_threshold(probabilities: np.ndarray, reviews_per_1000: float) -> float:
    limit = int(np.floor(len(probabilities) * reviews_per_1000 / 1000.0))
    if limit <= 0:
        return 1.0
    ordered = np.sort(np.asarray(probabilities, dtype=float))[::-1]
    return float(ordered[min(limit, len(ordered)) - 1])


def _score(
    estimator: Any, calibrator: PlattCalibrator, features: pd.DataFrame
) -> tuple[np.ndarray, np.ndarray]:
    raw = estimator.predict_proba(features)[:, 1]
    return raw, calibrator.predict(raw)


def train_and_evaluate(
    splits: dict[str, pd.DataFrame],
    config: TrainingConfig,
) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any]]:
    """Fit on train, select/calibrate on calibration, and report test once."""
    train = splits["train"]
    calibration = splits["calibration"]
    test = splits["test"]
    x_train, y_train = train[FEATURE_COLUMNS], train["Class"].to_numpy()
    x_cal, y_cal = calibration[FEATURE_COLUMNS], calibration["Class"].to_numpy()
    x_test, y_test = test[FEATURE_COLUMNS], test["Class"].to_numpy()

    baseline = _baseline(config.random_state).fit(x_train, y_train)
    challenger = _challenger(train["Class"], config).fit(x_train, y_train)
    candidates: dict[str, Any] = {
        "logistic_regression": baseline,
        "xgboost": challenger,
    }
    calibration_results: dict[str, dict[str, Any]] = {}
    calibrators: dict[str, PlattCalibrator] = {}

    for name, estimator in candidates.items():
        raw_cal = estimator.predict_proba(x_cal)[:, 1]
        calibrator = PlattCalibrator().fit(raw_cal, y_cal)
        calibrated = calibrator.predict(raw_cal)
        calibrators[name] = calibrator
        calibration_results[name] = {
            "raw": _metrics(y_cal, raw_cal),
            "calibrated": _metrics(y_cal, calibrated),
            "calibrator": calibrator.parameters(),
        }

    lift = (
        calibration_results["xgboost"]["calibrated"]["pr_auc"]
        - calibration_results["logistic_regression"]["calibrated"]["pr_auc"]
    )
    selected_name = (
        "xgboost" if lift >= config.minimum_pr_auc_lift else "logistic_regression"
    )
    selected = candidates[selected_name]
    selected_calibrator = calibrators[selected_name]
    _, calibration_probabilities = _score(selected, selected_calibrator, x_cal)
    threshold = capacity_threshold(calibration_probabilities, config.reviews_per_1000)

    test_results: dict[str, dict[str, Any]] = {}
    candidate_test_probabilities: dict[str, np.ndarray] = {}
    for name, estimator in candidates.items():
        raw_test, calibrated_test = _score(estimator, calibrators[name], x_test)
        candidate_test_probabilities[name] = calibrated_test
        test_results[name] = {
            "raw": _metrics(y_test, raw_test),
            "calibrated": _metrics(y_test, calibrated_test),
            "pr_auc_ci_95": _bootstrap_pr_auc(
                y_test,
                calibrated_test,
                samples=config.bootstrap_samples,
                random_state=config.random_state + (1 if name == "xgboost" else 0),
            ),
        }

    scored = test.copy()
    scored["fraud_probability"] = candidate_test_probabilities[selected_name]
    scored["score_percentile"] = scored["fraud_probability"].rank(
        method="max", pct=True
    )
    scored = _add_signal_columns(scored, selected, selected_name)
    policy_frame = apply_review_policy(scored, threshold, config.reviews_per_1000)
    policy = policy_summary(policy_frame, threshold, config.reviews_per_1000)

    baseline_test = test_results["logistic_regression"]["calibrated"]
    selected_test = test_results[selected_name]["calibrated"]
    evaluation = {
        "selection": {
            "selected_model": selected_name,
            "calibration_pr_auc_lift": float(lift),
            "minimum_required_lift": config.minimum_pr_auc_lift,
            "capacity_threshold": threshold,
            "reviews_per_1000": config.reviews_per_1000,
        },
        "calibration": calibration_results,
        "test": test_results,
        "policy": policy,
        "gates": {
            "challenger_pr_auc_lift": float(
                test_results["xgboost"]["calibrated"]["pr_auc"]
                - baseline_test["pr_auc"]
            ),
            "precision_at_capacity": policy["precision"],
            "recall_at_capacity": policy["recall"],
            "brier_delta": float(selected_test["brier"] - baseline_test["brier"]),
        },
    }
    bundle = {
        "model_name": selected_name,
        "estimator": selected,
        "calibrator": selected_calibrator,
        "feature_columns": FEATURE_COLUMNS,
        "threshold": threshold,
        "reviews_per_1000": config.reviews_per_1000,
    }
    return bundle, policy_frame, evaluation


def _add_signal_columns(
    frame: pd.DataFrame, estimator: Any, model_name: str
) -> pd.DataFrame:
    features = frame[FEATURE_COLUMNS]
    if model_name == "xgboost":
        contributions = estimator.get_booster().predict(
            __import__("xgboost").DMatrix(features, feature_names=FEATURE_COLUMNS),
            pred_contribs=True,
        )[:, :-1]
    else:
        scaler = estimator.named_steps["scale"]
        model = estimator.named_steps["model"]
        contributions = scaler.transform(features) * model.coef_[0]
    order = np.argsort(np.abs(contributions), axis=1)[:, -3:][:, ::-1]
    names = np.asarray(FEATURE_COLUMNS)
    output = frame.copy()
    for rank in range(3):
        column_indices = order[:, rank]
        output[f"signal_{rank + 1}"] = names[column_indices]
        output[f"signal_{rank + 1}_contribution"] = contributions[
            np.arange(len(output)), column_indices
        ]
    return output
