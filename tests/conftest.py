from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.fraud_workbench.artifacts import build_run
from src.fraud_workbench.data import FEATURE_COLUMNS
from src.fraud_workbench.modeling import TrainingConfig


def synthetic_transactions(rows: int = 1200, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    frame = pd.DataFrame({"Time": np.arange(rows, dtype=float)})
    for column in FEATURE_COLUMNS[1:-1]:
        frame[column] = rng.normal(size=rows)
    frame["Amount"] = rng.lognormal(mean=3.0, sigma=1.0, size=rows)
    signal = (
        frame["V1"] * 1.8 - frame["V4"] + frame["V12"] * 0.8 + rng.normal(0, 0.7, rows)
    )
    frame["Class"] = 0
    for start in range(0, rows, 200):
        block = signal.iloc[start : start + 200]
        frame.loc[block.nlargest(8).index, "Class"] = 1
    return frame


@pytest.fixture(scope="session")
def artifact_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("workbench")
    source = root / "creditcard.csv"
    synthetic_transactions().to_csv(source, index=False)
    build_run(
        source,
        root / "artifacts",
        TrainingConfig(
            bootstrap_samples=8,
            xgb_estimators=25,
            xgb_max_depth=2,
            xgb_learning_rate=0.1,
        ),
    )
    return root / "artifacts"
