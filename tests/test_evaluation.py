from __future__ import annotations

import numpy as np

from src.fraud_workbench.modeling import capacity_threshold


def test_capacity_threshold_uses_the_kth_highest_score() -> None:
    probabilities = np.linspace(0, 1, 1000)
    threshold = capacity_threshold(probabilities, reviews_per_1000=5)
    assert threshold == probabilities[-5]


def test_capacity_threshold_with_zero_slots_fails_closed() -> None:
    assert capacity_threshold(np.array([0.2, 0.8]), reviews_per_1000=1) == 1.0
