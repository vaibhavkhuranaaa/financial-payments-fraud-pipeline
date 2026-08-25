"""Pure capacity and threshold policy calculations used by tests and UI."""

from __future__ import annotations

import math
from collections.abc import Iterable

import numpy as np
import pandas as pd


def review_limit(row_count: int, reviews_per_1000: float) -> int:
    if row_count < 0 or reviews_per_1000 < 0:
        raise ValueError("row count and capacity must be non-negative")
    return min(row_count, math.floor(row_count * reviews_per_1000 / 1000.0))


def apply_review_policy(
    scored: pd.DataFrame,
    threshold: float,
    reviews_per_1000: float,
) -> pd.DataFrame:
    """Return scored rows with deterministic review actions and outcomes."""
    required = {"source_row_id", "fraud_probability", "Class"}
    missing = required.difference(scored.columns)
    if missing:
        raise ValueError(f"scored frame missing columns: {sorted(missing)}")
    if not 0 <= threshold <= 1:
        raise ValueError("threshold must be between zero and one")

    result = scored.copy()
    result["action"] = "pass"
    ranked = result.loc[result["fraud_probability"].ge(threshold)].sort_values(
        ["fraud_probability", "source_row_id"],
        ascending=[False, True],
        kind="mergesort",
    )
    limit = review_limit(len(result), reviews_per_1000)
    selected_index = ranked.head(limit).index
    result.loc[selected_index, "action"] = "review"
    result["outcome"] = "true_negative"
    result.loc[result["Class"].eq(1) & result["action"].eq("review"), "outcome"] = (
        "captured_fraud"
    )
    result.loc[result["Class"].eq(0) & result["action"].eq("review"), "outcome"] = (
        "false_positive"
    )
    result.loc[result["Class"].eq(1) & result["action"].eq("pass"), "outcome"] = (
        "missed_fraud"
    )
    return result


def policy_summary(
    policy_frame: pd.DataFrame, threshold: float, reviews_per_1000: float
) -> dict[str, int | float | str | None]:
    reviewed = policy_frame["action"].eq("review")
    fraud = policy_frame["Class"].eq(1)
    true_positive = int((reviewed & fraud).sum())
    false_positive = int((reviewed & ~fraud).sum())
    false_negative = int((~reviewed & fraud).sum())
    review_count = int(reviewed.sum())
    eligible_count = int(policy_frame["fraud_probability"].ge(threshold).sum())
    capacity = review_limit(len(policy_frame), reviews_per_1000)
    precision = true_positive / review_count if review_count else None
    recall = true_positive / int(fraud.sum()) if fraud.any() else None
    fp_per_capture = false_positive / true_positive if true_positive else None
    reviews_per_capture = review_count / true_positive if true_positive else None
    prevalence = float(fraud.mean()) if len(policy_frame) else None
    queue_lift = (
        precision / prevalence if precision is not None and prevalence else None
    )
    fraud_amount = (
        float(policy_frame.loc[fraud, "Amount"].sum())
        if "Amount" in policy_frame
        else None
    )
    captured_amount = (
        float(policy_frame.loc[reviewed & fraud, "Amount"].sum())
        if fraud_amount is not None
        else None
    )
    missed_amount = (
        fraud_amount - captured_amount
        if fraud_amount is not None and captured_amount is not None
        else None
    )
    amount_recall = captured_amount / fraud_amount if fraud_amount else None

    ranked = policy_frame.sort_values(
        ["fraud_probability", "source_row_id"],
        ascending=[False, True],
        kind="mergesort",
    ).head(capacity)
    capacity_true_positive = int(ranked["Class"].eq(1).sum())
    capacity_recall = capacity_true_positive / int(fraud.sum()) if fraud.any() else None
    recall_headroom = (
        capacity_recall - recall
        if capacity_recall is not None and recall is not None
        else None
    )
    binding = "capacity" if eligible_count > capacity else "threshold"
    if capacity == 0:
        binding = "zero capacity"
    return {
        "rows": len(policy_frame),
        "review_count": review_count,
        "review_rate": review_count / len(policy_frame) if len(policy_frame) else 0.0,
        "eligible_count": eligible_count,
        "capacity_limit": capacity,
        "true_positive": true_positive,
        "false_positive": false_positive,
        "false_negative": false_negative,
        "precision": precision,
        "recall": recall,
        "false_positives_per_capture": fp_per_capture,
        "reviews_per_capture": reviews_per_capture,
        "fraud_prevalence": prevalence,
        "queue_lift": queue_lift,
        "fraud_amount_total": fraud_amount,
        "captured_amount": captured_amount,
        "missed_amount": missed_amount,
        "amount_recall": amount_recall,
        "capacity_true_positive": capacity_true_positive,
        "capacity_recall": capacity_recall,
        "recall_headroom_at_capacity": recall_headroom,
        "binding_control": binding,
        "threshold": threshold,
        "reviews_per_1000": reviews_per_1000,
    }


def capacity_frontier(
    scored: pd.DataFrame,
    capacity_points: Iterable[float] | None = None,
) -> pd.DataFrame:
    """Return retrospective workload and capture trade-offs for score-ranked queues."""
    required = {"source_row_id", "fraud_probability", "Class"}
    missing = required.difference(scored.columns)
    if missing:
        raise ValueError(f"scored frame missing columns: {sorted(missing)}")
    points = tuple(capacity_points or (0.5, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0, 25.0, 50.0))
    if any(point < 0 for point in points):
        raise ValueError("capacity points must be non-negative")

    ranked = scored.sort_values(
        ["fraud_probability", "source_row_id"],
        ascending=[False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    labels = ranked["Class"].eq(1).astype(int).to_numpy()
    cumulative_fraud = np.cumsum(labels)
    total_fraud = int(cumulative_fraud[-1]) if len(cumulative_fraud) else 0
    if "Amount" in ranked:
        fraud_amounts = (
            ranked["Amount"].where(ranked["Class"].eq(1), 0.0).to_numpy(dtype=float)
        )
        cumulative_amount = np.cumsum(fraud_amounts)
        total_amount = float(cumulative_amount[-1]) if len(cumulative_amount) else 0.0
    else:
        cumulative_amount = None
        total_amount = 0.0

    rows: list[dict[str, int | float | None]] = []
    for point in points:
        review_count = review_limit(len(ranked), point)
        captured = int(cumulative_fraud[review_count - 1]) if review_count else 0
        captured_amount = (
            float(cumulative_amount[review_count - 1])
            if review_count and cumulative_amount is not None
            else None
        )
        rows.append(
            {
                "reviews_per_1000": float(point),
                "review_count": review_count,
                "true_positive": captured,
                "false_positive": review_count - captured,
                "recall": captured / total_fraud if total_fraud else None,
                "precision": captured / review_count if review_count else None,
                "reviews_per_capture": review_count / captured if captured else None,
                "amount_recall": captured_amount / total_amount
                if captured_amount is not None and total_amount
                else None,
            }
        )
    return pd.DataFrame(rows)


def minimum_workload_for_recall(
    scored: pd.DataFrame,
    targets: Iterable[float] = (0.75, 0.80, 0.85, 0.90, 0.95, 1.0),
) -> pd.DataFrame:
    """Find the smallest retrospective score-ranked queue reaching each recall target."""
    ranked = scored.sort_values(
        ["fraud_probability", "source_row_id"],
        ascending=[False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    fraud_rows = ranked.loc[ranked["Class"].eq(1)].copy()
    total_fraud = len(fraud_rows)
    if not total_fraud:
        return pd.DataFrame()
    fraud_rows["rank"] = fraud_rows.index + 1
    total_amount = (
        float(ranked.loc[ranked["Class"].eq(1), "Amount"].sum())
        if "Amount" in ranked
        else 0.0
    )

    rows: list[dict[str, int | float | None]] = []
    for target in targets:
        if not 0 < target <= 1:
            raise ValueError("recall targets must be greater than zero and at most one")
        target_count = min(total_fraud, max(1, math.ceil(total_fraud * target - 1e-12)))
        review_count = int(fraud_rows.iloc[target_count - 1]["rank"])
        queue = ranked.head(review_count)
        captured = int(queue["Class"].eq(1).sum())
        captured_amount = (
            float(queue.loc[queue["Class"].eq(1), "Amount"].sum())
            if "Amount" in ranked
            else None
        )
        rows.append(
            {
                "target_recall": float(target),
                "achieved_recall": captured / total_fraud,
                "review_count": review_count,
                "reviews_per_1000": review_count / len(ranked) * 1000.0,
                "true_positive": captured,
                "false_positive": review_count - captured,
                "precision": captured / review_count,
                "threshold": float(queue.iloc[-1]["fraud_probability"]),
                "amount_recall": captured_amount / total_amount
                if captured_amount is not None and total_amount
                else None,
            }
        )
    return pd.DataFrame(rows)
