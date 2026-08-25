"""Artifact-backed data access for the local fraud decision console."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.fraud_workbench.artifacts import load_run
from src.fraud_workbench.policy import (
    apply_review_policy,
    capacity_frontier,
    minimum_workload_for_recall,
    policy_summary,
)


class ArtifactLoadError(RuntimeError):
    """Raised when the console cannot load a complete model run."""


@dataclass
class DecisionView:
    frame: pd.DataFrame
    queue: pd.DataFrame
    summary: dict[str, Any]


class ArtifactStore:
    def __init__(self, artifact_root: str | Path) -> None:
        self.artifact_root = Path(artifact_root)
        self.manifest: dict[str, Any] | None = None
        self.evaluation: dict[str, Any] | None = None
        self.bundle: dict[str, Any] | None = None
        self.scored = pd.DataFrame()
        self.frontier = pd.DataFrame()
        self.recall_targets = pd.DataFrame()
        self.error: str | None = None
        self.refresh()

    def refresh(self) -> None:
        try:
            self.manifest, self.scored, self.evaluation, self.bundle = load_run(
                self.artifact_root
            )
            self.frontier = capacity_frontier(
                self.scored,
                capacity_points=(
                    0.5,
                    1.0,
                    1.5,
                    2.0,
                    3.0,
                    3.81,
                    5.0,
                    10.0,
                    25.0,
                    42.66,
                    50.0,
                ),
            )
            self.recall_targets = minimum_workload_for_recall(self.scored)
            self.error = None
        except Exception as exc:  # noqa: BLE001 - the UI must name artifact failures
            self.manifest = None
            self.evaluation = None
            self.bundle = None
            self.scored = pd.DataFrame()
            self.frontier = pd.DataFrame()
            self.recall_targets = pd.DataFrame()
            self.error = str(exc)

    @property
    def ready(self) -> bool:
        return self.error is None and not self.scored.empty and self.bundle is not None

    @property
    def default_threshold(self) -> float:
        return float(self.bundle["threshold"]) if self.ready else 0.5

    @property
    def default_capacity(self) -> float:
        return float(self.bundle["reviews_per_1000"]) if self.ready else 1.0

    def decision_view(
        self,
        threshold: float,
        reviews_per_1000: float,
        amount_min: float = 0.0,
        amount_max: float | None = None,
        outcome: str = "all",
    ) -> DecisionView:
        if not self.ready:
            raise ArtifactLoadError(self.error or "model artifacts are unavailable")
        policy = apply_review_policy(
            self.scored, float(threshold), float(reviews_per_1000)
        )
        summary = policy_summary(policy, float(threshold), float(reviews_per_1000))
        queue = policy.loc[policy["action"].eq("review")].sort_values(
            ["fraud_probability", "source_row_id"],
            ascending=[False, True],
            kind="mergesort",
        )
        queue = queue.copy()
        queue.insert(0, "rank", np.arange(1, len(queue) + 1))
        upper = (
            float(queue["Amount"].max())
            if amount_max is None and not queue.empty
            else amount_max
        )
        if upper is not None:
            queue = queue.loc[queue["Amount"].between(float(amount_min), float(upper))]
        if outcome != "all":
            queue = queue.loc[queue["outcome"].eq(outcome)]
        return DecisionView(frame=policy, queue=queue, summary=summary)

    def strategy_summary(self) -> dict[str, Any]:
        if not self.ready:
            raise ArtifactLoadError(self.error or "model artifacts are unavailable")
        return {
            "run_id": self.manifest["run_id"],
            "current_policy": policy_summary(
                apply_review_policy(
                    self.scored, self.default_threshold, self.default_capacity
                ),
                self.default_threshold,
                self.default_capacity,
            ),
            "frontier": self.frontier.to_dict("records"),
            "recall_targets": self.recall_targets.to_dict("records"),
            "model": {
                "selected": self.evaluation["selection"]["selected_model"],
                "pr_auc": self.evaluation["test"][self.bundle["model_name"]][
                    "calibrated"
                ]["pr_auc"],
                "pr_auc_ci_95": self.evaluation["test"][self.bundle["model_name"]][
                    "pr_auc_ci_95"
                ],
                "brier": self.evaluation["test"][self.bundle["model_name"]][
                    "calibrated"
                ]["brier"],
                "test_fraud_rows": int(self.scored["Class"].sum()),
                "test_rows": len(self.scored),
            },
        }

    def record(self, source_row_id: int) -> dict[str, Any] | None:
        if not self.ready:
            raise ArtifactLoadError(self.error or "model artifacts are unavailable")
        rows = self.scored.loc[self.scored["source_row_id"].eq(source_row_id)]
        if rows.empty:
            return None
        row = rows.iloc[0]
        signals = [
            {
                "feature": str(row[f"signal_{rank}"]),
                "contribution": float(row[f"signal_{rank}_contribution"]),
            }
            for rank in range(1, 4)
        ]
        return {
            "source_row_id": int(row["source_row_id"]),
            "elapsed_seconds": float(row["Time"]),
            "amount": float(row["Amount"]),
            "fraud_probability": float(row["fraud_probability"]),
            "score_percentile": float(row["score_percentile"]),
            "observed_class": int(row["Class"]),
            "signals": signals,
        }

    def score(self, features: dict[str, Any]) -> dict[str, Any]:
        if not self.ready:
            raise ArtifactLoadError(self.error or "model artifacts are unavailable")
        columns = self.bundle["feature_columns"]
        missing = [column for column in columns if column not in features]
        extra = [column for column in features if column not in columns]
        if missing or extra:
            raise ValueError(
                f"feature contract mismatch; missing={missing}, extra={extra}"
            )
        try:
            row = pd.DataFrame(
                [[float(features[column]) for column in columns]], columns=columns
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("all feature values must be numeric") from exc
        if (
            not np.isfinite(row.to_numpy()).all()
            or row["Time"].iloc[0] < 0
            or row["Amount"].iloc[0] < 0
        ):
            raise ValueError(
                "features must be finite; Time and Amount must be non-negative"
            )
        raw = self.bundle["estimator"].predict_proba(row)[:, 1]
        probability = float(self.bundle["calibrator"].predict(raw)[0])
        return {
            "fraud_probability": probability,
            "decision": "review_eligible"
            if probability >= self.default_threshold
            else "pass",
            "threshold": self.default_threshold,
            "model": self.bundle["model_name"],
        }


def format_ratio(value: float | None, digits: int = 1) -> str:
    return "Unavailable" if value is None else f"{value * 100:.{digits}f}%"


def format_count(value: int | float | None) -> str:
    return "Unavailable" if value is None else f"{int(value):,}"
