from __future__ import annotations

import json

from src.fraud_workbench.artifacts import build_run, load_run, resolve_run
from src.fraud_workbench.modeling import TrainingConfig
from tests.conftest import synthetic_transactions


def test_complete_run_is_replayed_without_retraining(tmp_path) -> None:
    source = tmp_path / "creditcard.csv"
    root = tmp_path / "artifacts"
    synthetic_transactions(800).to_csv(source, index=False)
    config = TrainingConfig(bootstrap_samples=4, xgb_estimators=15, xgb_max_depth=2)

    first = build_run(source, root, config)
    second = build_run(source, root, config)

    assert first["run_id"] == second["run_id"]
    assert first["replayed"] is False
    assert second["replayed"] is True
    assert resolve_run(root).name == first["run_id"]


def test_loaded_run_has_manifest_scored_holdout_and_selected_model(
    artifact_root,
) -> None:
    manifest, scored, evaluation, bundle = load_run(artifact_root)
    assert manifest["source"]["valid_rows"] == 1200
    assert not scored.empty
    assert evaluation["selection"]["selected_model"] in {
        "logistic_regression",
        "xgboost",
    }
    assert bundle["model_name"] == evaluation["selection"]["selected_model"]


def test_incomplete_pointer_fails_closed(tmp_path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    (root / "latest.json").write_text(
        json.dumps({"run_id": "missing"}), encoding="utf-8"
    )
    try:
        resolve_run(root)
    except FileNotFoundError as exc:
        assert "unavailable" in str(exc)
    else:
        raise AssertionError("incomplete artifact pointer must fail")
