"""Unit tests for src.pipeline.train's model-ops surface (ticket 17, v1.5b):
run-id generation, versioned artifact layout, current.json pointer writing,
and manifest contents (including the drift.py reference stats).

The full train_model() end-to-end pipeline is exercised on a small (5k-row)
slice of the committed sample CSV — same fixture pattern as
tests/test_features.py::test_vectorized_matches_row_wise_on_sample — kept to
one module-scoped run since training (even at this size) is the slow part of
this test file.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pytest

from src.pipeline import train


@pytest.fixture(scope="module")
def small_csv(tmp_path_factory) -> str:
    sample_path = "data/sample/transactions_sample.csv"
    tmp_dir = tmp_path_factory.mktemp("train_fixture")
    small_path = tmp_dir / "small5k.csv"
    with open(sample_path, encoding="utf-8") as src:
        lines = src.readlines()
    small_path.write_text("".join(lines[:5001]), encoding="utf-8")  # header + 5000 rows
    return str(small_path)


@pytest.fixture(scope="module")
def trained_run(tmp_path_factory, small_csv: str):
    """One real (small, fast) train_model() run, artifacts in an isolated
    model_dir shared read-only across the tests in this module."""
    model_dir = str(tmp_path_factory.mktemp("models"))
    metrics = train.train_model(
        small_csv,
        salt="test-salt",
        model_dir=model_dir,
        run_id="20260719T120000Z-deadbee",
    )
    return model_dir, metrics


class TestMakeRunId:
    def test_format(self):
        run_id = train.make_run_id(now=datetime(2026, 7, 19, 12, 0, 0, tzinfo=timezone.utc), git_sha="abc1234")
        assert run_id == "20260719T120000Z-abc1234"

    def test_falls_back_to_nogit_sha_when_git_lookup_fails(self):
        run_id = train.make_run_id(now=datetime(2026, 7, 19, 12, 0, 0, tzinfo=timezone.utc), git_sha="nogit")
        assert run_id == "20260719T120000Z-nogit"

    def test_default_now_is_utc_timestamp_shaped(self):
        run_id = train.make_run_id(git_sha="abc1234")
        timestamp_part = run_id.split("-")[0]
        # Round-trips through strptime -> proves the format is exactly what
        # app.py / drift.py expect to find as a subdirectory name.
        datetime.strptime(timestamp_part, "%Y%m%dT%H%M%SZ")


class TestVersionedArtifactLayout:
    def test_writes_into_run_subdirectory_not_flat_model_dir(self, trained_run):
        model_dir, metrics = trained_run
        run_dir = os.path.join(model_dir, metrics["run_id"])
        for name in ("model.json", "threshold.json", "feature_columns.json", "metrics.json", "manifest.json"):
            assert os.path.exists(os.path.join(run_dir, name)), f"missing {name} in run dir"
        # Nothing was ever written flat directly under model_dir except the pointer.
        assert not os.path.exists(os.path.join(model_dir, "model.json"))

    def test_run_id_is_deterministic_when_passed_explicitly(self, trained_run):
        _model_dir, metrics = trained_run
        assert metrics["run_id"] == "20260719T120000Z-deadbee"

    def test_current_json_pointer_written_by_default(self, trained_run):
        model_dir, metrics = trained_run
        pointer_path = os.path.join(model_dir, "current.json")
        assert os.path.exists(pointer_path)
        with open(pointer_path, encoding="utf-8") as f:
            pointer = json.load(f)
        assert pointer == {"run_id": metrics["run_id"]}

    def test_set_current_false_skips_pointer(self, small_csv, tmp_path):
        model_dir = str(tmp_path)
        train.train_model(
            small_csv,
            salt="test-salt",
            model_dir=model_dir,
            run_id="20260719T130000Z-cafebee",
            set_current=False,
        )
        assert not os.path.exists(os.path.join(model_dir, "current.json"))
        assert os.path.exists(os.path.join(model_dir, "20260719T130000Z-cafebee", "model.json"))

    def test_second_run_repoints_current_without_deleting_first_run(self, small_csv, tmp_path):
        model_dir = str(tmp_path)
        train.train_model(small_csv, salt="test-salt", model_dir=model_dir, run_id="run-a")
        train.train_model(small_csv, salt="test-salt", model_dir=model_dir, run_id="run-b")

        with open(os.path.join(model_dir, "current.json"), encoding="utf-8") as f:
            pointer = json.load(f)
        assert pointer == {"run_id": "run-b"}
        # Both run dirs still exist on disk (no deletion of prior runs).
        assert os.path.exists(os.path.join(model_dir, "run-a", "model.json"))
        assert os.path.exists(os.path.join(model_dir, "run-b", "model.json"))


class TestManifestContents:
    def test_manifest_has_required_provenance_fields(self, trained_run):
        model_dir, metrics = trained_run
        with open(os.path.join(model_dir, metrics["run_id"], "manifest.json"), encoding="utf-8") as f:
            manifest = json.load(f)
        for key in (
            "run_id",
            "git_sha",
            "trained_at",
            "seed",
            "input_path",
            "data_range",
            "row_counts",
            "class_balance",
            "package_versions",
            "drift_reference",
        ):
            assert key in manifest, f"manifest missing {key}"
        assert manifest["run_id"] == metrics["run_id"]
        assert manifest["git_sha"] == "deadbee"
        assert manifest["seed"] == train.SEED
        assert manifest["row_counts"] == metrics["rows"]
        assert manifest["class_balance"] == metrics["fraud_rate"]

    def test_manifest_package_versions_are_nonempty_strings(self, trained_run):
        model_dir, metrics = trained_run
        with open(os.path.join(model_dir, metrics["run_id"], "manifest.json"), encoding="utf-8") as f:
            manifest = json.load(f)
        versions = manifest["package_versions"]
        for pkg in ("python", "xgboost", "pandas", "numpy", "scikit-learn"):
            assert pkg in versions
            assert isinstance(versions[pkg], str) and versions[pkg]

    def test_manifest_drift_reference_covers_the_top_features(self, trained_run):
        model_dir, metrics = trained_run
        with open(os.path.join(model_dir, metrics["run_id"], "manifest.json"), encoding="utf-8") as f:
            manifest = json.load(f)
        drift_reference = manifest["drift_reference"]
        assert set(drift_reference) == {"amount", "mcc", "channel"}
        assert drift_reference["amount"]["type"] == "numeric"
        assert drift_reference["mcc"]["type"] == "categorical"
        assert drift_reference["channel"]["type"] == "categorical"
        # channel values reconstructed from is_cnp/is_chip/is_swipe must be
        # exactly the raw contract vocabulary, never "unknown" on real data.
        assert set(drift_reference["channel"]["counts"]) <= {"chip", "swipe", "online"}


class TestDriftFrameChannelReconstruction:
    def test_reconstructs_exclusive_channel_labels(self):
        import pandas as pd

        df = pd.DataFrame(
            {
                "amount": [1.0, 2.0, 3.0],
                "mcc": [100, 200, 300],
                "is_cnp": [1, 0, 0],
                "is_chip": [0, 1, 0],
                "is_swipe": [0, 0, 1],
            }
        )
        out = train._drift_frame(df)
        assert list(out["channel"]) == ["online", "chip", "swipe"]
        assert list(out["amount"]) == [1.0, 2.0, 3.0]
        assert list(out["mcc"]) == [100, 200, 300]
