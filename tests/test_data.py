from __future__ import annotations

import pandas as pd
import pytest

from src.fraud_workbench.data import (
    DataContractError,
    chronological_split,
    load_transactions,
)
from tests.conftest import synthetic_transactions


def test_load_validates_and_preserves_source_rows(tmp_path) -> None:
    source = tmp_path / "creditcard.csv"
    frame = synthetic_transactions(600)
    frame.to_csv(source, index=False)

    valid, rejects, report = load_transactions(source)

    assert len(valid) == len(frame)
    assert rejects.empty
    assert report.source_rows == 600
    assert report.fraud_rows == int(frame["Class"].sum())


def test_invalid_values_are_rejected_with_a_reason(tmp_path) -> None:
    source = tmp_path / "creditcard.csv"
    frame = synthetic_transactions(600)
    frame.loc[3, "Amount"] = -1
    frame.loc[4, "Class"] = 2
    frame.to_csv(source, index=False)

    valid, rejects, report = load_transactions(source)

    assert len(valid) == 598
    assert report.rejected_rows == 2
    assert rejects["rejection_reason"].str.len().gt(0).all()


def test_schema_drift_fails_before_training(tmp_path) -> None:
    source = tmp_path / "creditcard.csv"
    pd.DataFrame({"Time": [0], "Class": [0]}).to_csv(source, index=False)
    with pytest.raises(DataContractError, match="unexpected columns"):
        load_transactions(source)


def test_chronological_split_keeps_equal_times_together() -> None:
    frame = synthetic_transactions(600)
    frame.insert(0, "source_row_id", range(len(frame)))
    frame.loc[350:370, "Time"] = 350
    splits = chronological_split(frame)

    assert splits["train"]["Time"].max() < splits["calibration"]["Time"].min()
    assert splits["calibration"]["Time"].max() < splits["test"]["Time"].min()
    assert sum(map(len, splits.values())) == len(frame)
