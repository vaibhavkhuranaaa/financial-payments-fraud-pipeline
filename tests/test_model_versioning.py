"""Unit tests for src.app's model-versioning surface (ticket 17, v1.5b):
`_resolve_model_dir` pointer resolution, and `model_version` showing up in
`/healthz` + `/score` when `create_app()` loads from disk.

Hermetic: no live Redis; a tiny real Booster is saved to tmp_path so
`create_app()`'s disk-loading path (not the dependency-injection path
tests/test_api.py exercises) gets covered end-to-end, including backward
compatibility with the flat pre-ticket-17 layout.
"""

from __future__ import annotations

import json

import numpy as np
import xgboost as xgb

from src.app import _resolve_model_dir, create_app
from src.pipeline.features import FEATURE_COLUMNS


def _write_flat_artifacts(model_dir) -> None:
    rng = np.random.default_rng(7)
    n = 50
    x = rng.random((n, len(FEATURE_COLUMNS)))
    y = rng.integers(0, 2, size=n)
    dtrain = xgb.DMatrix(x, label=y, feature_names=FEATURE_COLUMNS)
    booster = xgb.train({"objective": "binary:logistic", "max_depth": 2}, dtrain, num_boost_round=3)
    booster.save_model(str(model_dir / "model.json"))
    (model_dir / "threshold.json").write_text(json.dumps({"threshold": 0.5}))
    (model_dir / "feature_columns.json").write_text(json.dumps(FEATURE_COLUMNS))


class FakeRedis:
    def hgetall(self, key: str) -> dict[str, str]:
        return {}


def _sample_event() -> dict:
    return {
        "schema_version": "1.0.0",
        "event_id": "e1",
        "event_time": "2026-07-19T12:00:00Z",
        "card_token": "a" * 64,
        "user_id": "19",
        "amount": 42.50,
        "currency": "USD",
        "channel": "chip",
        "merchant_name": "some-merchant",
        "merchant_city": "Tucson",
        "merchant_state": "AZ",
        "merchant_country": "US",
        "zip": "85719",
        "mcc": 5411,
        "errors": None,
    }


class TestResolveModelDir:
    def test_no_pointer_falls_back_to_flat_layout(self, tmp_path):
        resolved_dir, version = _resolve_model_dir(str(tmp_path))
        assert resolved_dir == str(tmp_path)
        assert version is None

    def test_valid_pointer_resolves_run_dir(self, tmp_path):
        run_dir = tmp_path / "20260719T120000Z-abc1234"
        run_dir.mkdir()
        (tmp_path / "current.json").write_text(json.dumps({"run_id": run_dir.name}))
        resolved_dir, version = _resolve_model_dir(str(tmp_path))
        assert resolved_dir == str(run_dir)
        assert version == run_dir.name

    def test_stale_pointer_falls_back_to_flat_layout(self, tmp_path):
        (tmp_path / "current.json").write_text(json.dumps({"run_id": "missing-run"}))
        resolved_dir, version = _resolve_model_dir(str(tmp_path))
        assert resolved_dir == str(tmp_path)
        assert version is None

    def test_malformed_pointer_falls_back_to_flat_layout(self, tmp_path):
        (tmp_path / "current.json").write_text("not json")
        resolved_dir, version = _resolve_model_dir(str(tmp_path))
        assert resolved_dir == str(tmp_path)
        assert version is None


class TestCreateAppModelVersionFromDisk:
    def test_flat_legacy_layout_serves_with_no_model_version(self, tmp_path, monkeypatch):
        _write_flat_artifacts(tmp_path)
        monkeypatch.setattr("src.app.MODEL_DIR", str(tmp_path))
        monkeypatch.setattr("src.app.SCORE_THRESHOLD_PATH", str(tmp_path / "threshold.json"))

        app = create_app(redis_client=FakeRedis())
        client = app.test_client()

        healthz = client.get("/healthz").get_json()
        assert healthz["model_loaded"] is True
        assert healthz["model_version"] is None

        score = client.post("/score", json=_sample_event()).get_json()
        assert score["model_version"] is None

    def test_versioned_layout_reports_run_id_as_model_version(self, tmp_path, monkeypatch):
        run_id = "20260719T120000Z-abc1234"
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        _write_flat_artifacts(run_dir)
        (tmp_path / "current.json").write_text(json.dumps({"run_id": run_id}))
        monkeypatch.setattr("src.app.MODEL_DIR", str(tmp_path))
        monkeypatch.setattr("src.app.SCORE_THRESHOLD_PATH", str(tmp_path / "threshold.json"))

        app = create_app(redis_client=FakeRedis())
        client = app.test_client()

        healthz = client.get("/healthz").get_json()
        assert healthz["model_version"] == run_id

        score = client.post("/score", json=_sample_event()).get_json()
        assert score["model_version"] == run_id

    def test_explicit_model_version_override_wins_even_with_injected_model(self, tmp_path):
        rng = np.random.default_rng(1)
        x = rng.random((20, len(FEATURE_COLUMNS)))
        y = rng.integers(0, 2, size=20)
        dtrain = xgb.DMatrix(x, label=y, feature_names=FEATURE_COLUMNS)
        booster = xgb.train({"objective": "binary:logistic", "max_depth": 2}, dtrain, num_boost_round=2)

        app = create_app(
            model=booster,
            threshold=0.5,
            feature_columns=FEATURE_COLUMNS,
            redis_client=FakeRedis(),
            model_version="explicit-version",
        )
        client = app.test_client()
        assert client.get("/healthz").get_json()["model_version"] == "explicit-version"

    def test_injected_dependencies_default_model_version_to_none_without_touching_disk(self, tmp_path, monkeypatch):
        # If this accidentally touched disk it would try to resolve a
        # pointer under a directory that doesn't exist and could raise;
        # asserting None here is really asserting "disk was never touched".
        monkeypatch.setattr("src.app.MODEL_DIR", str(tmp_path / "does-not-exist"))
        rng = np.random.default_rng(2)
        x = rng.random((20, len(FEATURE_COLUMNS)))
        y = rng.integers(0, 2, size=20)
        dtrain = xgb.DMatrix(x, label=y, feature_names=FEATURE_COLUMNS)
        booster = xgb.train({"objective": "binary:logistic", "max_depth": 2}, dtrain, num_boost_round=2)

        app = create_app(model=booster, threshold=0.5, feature_columns=FEATURE_COLUMNS, redis_client=FakeRedis())
        client = app.test_client()
        assert client.get("/healthz").get_json()["model_version"] is None
