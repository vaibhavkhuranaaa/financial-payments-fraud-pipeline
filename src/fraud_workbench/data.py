"""Validation and chronology-preserving partitioning for the MLG-ULB data."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

FEATURE_COLUMNS = ["Time", *[f"V{i}" for i in range(1, 29)], "Amount"]
EXPECTED_COLUMNS = [*FEATURE_COLUMNS, "Class"]


class DataContractError(ValueError):
    """Raised when the source cannot satisfy the dataset contract."""


@dataclass(frozen=True)
class ValidationReport:
    source_rows: int
    valid_rows: int
    rejected_rows: int
    columns: int
    fraud_rows: int
    fraud_rate: float
    duplicate_rows: int
    missing_values: int
    time_min: float
    time_max: float

    def as_dict(self) -> dict[str, int | float]:
        return self.__dict__.copy()


def load_transactions(
    path: str | Path,
) -> tuple[pd.DataFrame, pd.DataFrame, ValidationReport]:
    """Load all rows, retaining invalid rows separately with explicit reasons."""
    source = Path(path)
    if not source.is_file():
        raise DataContractError(f"dataset not found: {source}")

    frame = pd.read_csv(source)
    if list(frame.columns) != EXPECTED_COLUMNS:
        missing = [column for column in EXPECTED_COLUMNS if column not in frame.columns]
        extra = [column for column in frame.columns if column not in EXPECTED_COLUMNS]
        raise DataContractError(f"unexpected columns; missing={missing}, extra={extra}")

    numeric = frame.apply(pd.to_numeric, errors="coerce")
    reasons = pd.Series("", index=numeric.index, dtype="object")
    invalid_numeric = numeric[FEATURE_COLUMNS].isna().any(axis=1)
    invalid_numeric |= ~np.isfinite(numeric[FEATURE_COLUMNS]).all(axis=1)
    invalid_class = ~numeric["Class"].isin([0, 1])
    invalid_time = numeric["Time"].lt(0)
    invalid_amount = numeric["Amount"].lt(0)

    for mask, label in (
        (invalid_numeric, "non-finite feature"),
        (invalid_class, "Class must be 0 or 1"),
        (invalid_time, "Time must be non-negative"),
        (invalid_amount, "Amount must be non-negative"),
    ):
        reasons.loc[mask] = (
            reasons.loc[mask].where(reasons.loc[mask].eq(""), reasons.loc[mask] + "; ")
            + label
        )

    invalid = reasons.ne("")
    rejects = frame.loc[invalid].copy()
    rejects.insert(0, "source_row_id", rejects.index.astype("int64"))
    rejects.insert(1, "rejection_reason", reasons.loc[invalid])

    valid = numeric.loc[~invalid].copy()
    valid.insert(0, "source_row_id", valid.index.astype("int64"))
    valid["Class"] = valid["Class"].astype("int8")
    valid = valid.sort_values(["Time", "source_row_id"], kind="mergesort").reset_index(
        drop=True
    )
    if valid.empty:
        raise DataContractError("dataset contains no valid rows")

    report = ValidationReport(
        source_rows=len(frame),
        valid_rows=len(valid),
        rejected_rows=len(rejects),
        columns=len(frame.columns),
        fraud_rows=int(valid["Class"].sum()),
        fraud_rate=float(valid["Class"].mean()),
        duplicate_rows=int(frame.duplicated().sum()),
        missing_values=int(frame.isna().sum().sum()),
        time_min=float(valid["Time"].min()),
        time_max=float(valid["Time"].max()),
    )
    return valid, rejects, report


def chronological_split(
    frame: pd.DataFrame,
    train_fraction: float = 0.60,
    calibration_fraction: float = 0.20,
) -> dict[str, pd.DataFrame]:
    """Split by elapsed time without placing equal timestamps in two folds."""
    if not 0 < train_fraction < 1 or not 0 < calibration_fraction < 1:
        raise ValueError("split fractions must be between zero and one")
    if train_fraction + calibration_fraction >= 1:
        raise ValueError("train and calibration fractions must leave a test split")
    if frame.empty:
        raise ValueError("cannot split an empty dataset")

    ordered = frame.sort_values(
        ["Time", "source_row_id"], kind="mergesort"
    ).reset_index(drop=True)
    times = ordered["Time"].to_numpy()
    train_target = max(1, int(len(ordered) * train_fraction))
    calibration_target = max(
        train_target + 1, int(len(ordered) * (train_fraction + calibration_fraction))
    )
    train_end = int(np.searchsorted(times, times[train_target - 1], side="right"))
    calibration_end = int(
        np.searchsorted(times, times[calibration_target - 1], side="right")
    )
    if train_end >= calibration_end or calibration_end >= len(ordered):
        raise ValueError("time boundaries do not leave three non-empty partitions")

    splits = {
        "train": ordered.iloc[:train_end].copy(),
        "calibration": ordered.iloc[train_end:calibration_end].copy(),
        "test": ordered.iloc[calibration_end:].copy(),
    }
    for name, split in splits.items():
        if split["Class"].nunique() != 2:
            raise ValueError(f"{name} partition must contain both target classes")
    return splits


def split_report(splits: dict[str, pd.DataFrame]) -> dict[str, dict[str, int | float]]:
    return {
        name: {
            "rows": len(split),
            "fraud_rows": int(split["Class"].sum()),
            "fraud_rate": float(split["Class"].mean()),
            "time_min": float(split["Time"].min()),
            "time_max": float(split["Time"].max()),
        }
        for name, split in splits.items()
    }
