"""Immutable local run artifacts and replay-safe build orchestration."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
import sklearn
import xgboost

from .data import chronological_split, load_transactions, split_report
from .modeling import TrainingConfig, train_and_evaluate


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _run_identity(report: dict[str, Any], config: TrainingConfig) -> str:
    identity = json.dumps(
        {"source": report, "config": config.as_dict()}, sort_keys=True
    ).encode()
    return hashlib.sha256(identity).hexdigest()[:16]


def build_run(
    data_path: str | Path,
    artifact_root: str | Path,
    config: TrainingConfig | None = None,
    force: bool = False,
) -> dict[str, Any]:
    config = config or TrainingConfig()
    root = Path(artifact_root)
    runs = root / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    frame, rejects, validation = load_transactions(data_path)
    validation_payload = validation.as_dict()
    run_id = _run_identity(validation_payload, config)
    run_dir = runs / run_id
    latest_path = root / "latest.json"

    if run_dir.is_dir() and (run_dir / "manifest.json").is_file() and not force:
        _write_json(latest_path, {"run_id": run_id})
        return {"run_id": run_id, "run_dir": str(run_dir), "replayed": True}

    staging = runs / f"{run_id}.building"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    try:
        _write_json(staging / "validation.json", validation_payload)
        if not rejects.empty:
            rejects.to_parquet(staging / "rejected_rows.parquet", index=False)
        splits = chronological_split(frame)
        partitions = split_report(splits)
        _write_json(staging / "partitions.json", partitions)
        bundle, scored_test, evaluation = train_and_evaluate(splits, config)
        joblib.dump(bundle, staging / "model.joblib")
        scored_test.to_parquet(staging / "scored_test.parquet", index=False)
        _write_json(staging / "evaluation.json", evaluation)
        manifest = {
            "schema_version": 1,
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "source": validation_payload,
            "partitions": partitions,
            "config": config.as_dict(),
            "selected_model": evaluation["selection"]["selected_model"],
            "files": [
                "evaluation.json",
                "model.joblib",
                "partitions.json",
                "scored_test.parquet",
                "validation.json",
            ],
            "runtime": {
                "python": platform.python_version(),
                "pandas": pd.__version__,
                "scikit_learn": sklearn.__version__,
                "xgboost": xgboost.__version__,
            },
        }
        if not rejects.empty:
            manifest["files"].append("rejected_rows.parquet")
        _write_json(staging / "manifest.json", manifest)
        if run_dir.exists():
            shutil.rmtree(run_dir)
        os.replace(staging, run_dir)
        _write_json(latest_path, {"run_id": run_id})
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return {"run_id": run_id, "run_dir": str(run_dir), "replayed": False}


def resolve_run(artifact_root: str | Path) -> Path:
    root = Path(artifact_root)
    latest = root / "latest.json"
    if not latest.is_file():
        raise FileNotFoundError(f"missing artifact pointer: {latest}")
    payload = json.loads(latest.read_text(encoding="utf-8"))
    run_id = payload.get("run_id")
    run_dir = root / "runs" / str(run_id)
    if not run_id or not run_dir.is_dir():
        raise FileNotFoundError(f"artifact run is unavailable: {run_id}")
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    for name in manifest.get("files", []):
        if not (run_dir / name).is_file():
            raise FileNotFoundError(f"artifact run is incomplete: {name}")
    return run_dir


def load_run(
    artifact_root: str | Path,
) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any], dict[str, Any]]:
    run_dir = resolve_run(artifact_root)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    evaluation = json.loads((run_dir / "evaluation.json").read_text(encoding="utf-8"))
    scored = pd.read_parquet(run_dir / "scored_test.parquet")
    bundle = joblib.load(run_dir / "model.joblib")
    return manifest, scored, evaluation, bundle
