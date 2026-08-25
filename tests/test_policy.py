from __future__ import annotations

import pandas as pd
import pytest

from src.fraud_workbench.policy import (
    apply_review_policy,
    capacity_frontier,
    minimum_workload_for_recall,
    policy_summary,
    review_limit,
)


def scored_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source_row_id": [4, 3, 2, 1],
            "fraud_probability": [0.9, 0.9, 0.8, 0.1],
            "Class": [0, 1, 1, 0],
            "Amount": [50.0, 100.0, 300.0, 20.0],
        }
    )


def test_capacity_truncation_and_ties_are_deterministic() -> None:
    selected = apply_review_policy(scored_frame(), threshold=0.5, reviews_per_1000=500)
    reviewed = selected.loc[selected["action"].eq("review"), "source_row_id"].tolist()
    assert reviewed == [4, 3]
    assert review_limit(4, 500) == 2


def test_zero_capacity_produces_defined_empty_summary() -> None:
    policy = apply_review_policy(scored_frame(), threshold=0.5, reviews_per_1000=0)
    summary = policy_summary(policy, threshold=0.5, reviews_per_1000=0)
    assert summary["review_count"] == 0
    assert summary["precision"] is None
    assert summary["binding_control"] == "zero capacity"


def test_threshold_outside_probability_range_is_rejected() -> None:
    with pytest.raises(ValueError, match="between zero and one"):
        apply_review_policy(scored_frame(), threshold=1.1, reviews_per_1000=1)


def test_policy_summary_exposes_workload_lift_amount_and_capacity_ceiling() -> None:
    policy = apply_review_policy(scored_frame(), threshold=0.85, reviews_per_1000=750)
    summary = policy_summary(policy, threshold=0.85, reviews_per_1000=750)
    assert summary["recall"] == 0.5
    assert summary["amount_recall"] == 0.25
    assert summary["reviews_per_capture"] == 2
    assert summary["queue_lift"] == 1
    assert summary["capacity_recall"] == 1
    assert summary["recall_headroom_at_capacity"] == 0.5


def test_frontier_and_recall_targets_make_capacity_tradeoff_explicit() -> None:
    frontier = capacity_frontier(scored_frame(), capacity_points=(500, 750))
    assert frontier.loc[0, "review_count"] == 2
    assert frontier.loc[0, "recall"] == 0.5
    assert frontier.loc[1, "recall"] == 1

    targets = minimum_workload_for_recall(scored_frame(), targets=(0.5, 1.0))
    assert targets.loc[0, "review_count"] == 1
    assert targets.loc[1, "review_count"] == 3
    assert targets.loc[1, "precision"] == pytest.approx(2 / 3)
