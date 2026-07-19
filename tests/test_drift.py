"""Unit tests for src.pipeline.drift: PSI math, reference-stats construction,
manifest/pointer resolution, and the DB-facing pieces (mocked engine).

Hermetic by construction: no live bank DB, no model artifacts on disk except
what a test writes into tmp_path itself.
"""

from __future__ import annotations

import json
import math
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from src.pipeline import drift


class TestPsiFromCounts:
    def test_identical_distributions_are_zero(self):
        ref = {"a": 50, "b": 50}
        cur = {"a": 50, "b": 50}
        assert drift.psi_from_counts(ref, cur) == pytest.approx(0.0, abs=1e-9)

    def test_known_two_bin_shift_matches_hand_computation(self):
        # ref: 90/10 split: cur: 10/90 split -> PSI computed by hand.
        ref = {"a": 90, "b": 10}
        cur = {"a": 10, "b": 90}
        ref_a, ref_b = 0.9, 0.1
        cur_a, cur_b = 0.1, 0.9
        expected = (cur_a - ref_a) * math.log(cur_a / ref_a) + (cur_b - ref_b) * math.log(cur_b / ref_b)
        assert drift.psi_from_counts(ref, cur) == pytest.approx(expected, rel=1e-9)

    def test_moderate_shift_is_between_stable_and_severe(self):
        stable_ref = {"a": 50, "b": 50}
        stable_cur = {"a": 48, "b": 52}
        severe_cur = {"a": 5, "b": 95}
        psi_stable = drift.psi_from_counts(stable_ref, stable_cur)
        psi_severe = drift.psi_from_counts(stable_ref, severe_cur)
        assert psi_stable < 0.1
        assert psi_severe > 0.2
        assert psi_stable < psi_severe

    def test_both_sides_empty_is_zero_not_nan_or_error(self):
        assert drift.psi_from_counts({}, {}) == 0.0

    def test_reference_empty_current_populated_is_finite_large(self):
        # Empty-bin edge case: nothing in the reference at all.
        psi = drift.psi_from_counts({}, {"a": 100})
        assert psi > 0.0
        assert math.isfinite(psi)

    def test_unseen_category_in_current_contributes_without_crashing(self):
        ref = {"a": 100}
        cur = {"a": 100, "unseen": 20}
        psi = drift.psi_from_counts(ref, cur)
        assert psi > 0.0
        assert math.isfinite(psi)

    def test_category_present_in_reference_only_also_contributes(self):
        ref = {"a": 80, "vanished": 20}
        cur = {"a": 100}
        psi = drift.psi_from_counts(ref, cur)
        assert psi > 0.0
        assert math.isfinite(psi)

    def test_zero_count_bin_uses_epsilon_not_zero_division(self):
        ref = {"a": 100, "b": 0}
        cur = {"a": 100, "b": 0}
        # Both sides agree (0 vs 0 in bin b) -> still finite, near zero.
        psi = drift.psi_from_counts(ref, cur)
        assert math.isfinite(psi)
        assert psi == pytest.approx(0.0, abs=1e-9)


class TestNumericBinning:
    def test_bin_edges_cover_full_range_with_open_ends(self):
        values = np.arange(100, dtype="float64")
        edges = drift.numeric_bin_edges(values, n_bins=10)
        assert edges[0] == -np.inf
        assert edges[-1] == np.inf

    def test_constant_column_falls_back_to_single_bin(self):
        values = np.full(50, 7.0)
        edges = drift.numeric_bin_edges(values, n_bins=10)
        assert len(edges) == 2
        counts = drift.bin_numeric(values, edges)
        assert counts == {"0": 50}

    def test_empty_input_returns_default_edges_and_empty_counts(self):
        values = np.array([], dtype="float64")
        edges = drift.numeric_bin_edges(values)
        assert edges == [0.0, 1.0]
        assert drift.bin_numeric(values, edges) == {}

    def test_bin_numeric_out_of_training_range_lands_in_outer_bin(self):
        edges = drift.numeric_bin_edges(np.arange(1, 101, dtype="float64"), n_bins=4)
        counts = drift.bin_numeric(np.array([-9999.0, 9999.0]), edges)
        assert sum(counts.values()) == 2
        assert min(int(k) for k in counts) == 0
        assert max(int(k) for k in counts) == len(edges) - 2

    def test_bin_numeric_reproduces_reference_counts_on_same_data(self):
        rng = np.random.default_rng(0)
        values = rng.normal(size=1000)
        edges = drift.numeric_bin_edges(values, n_bins=10)
        counts = drift.bin_numeric(values, edges)
        assert sum(counts.values()) == 1000


class TestCategoricalCounting:
    def test_counts_match_value_counts(self):
        values = pd.Series(["a", "a", "b", "a", "c"])
        assert drift.count_categorical(values) == {"a": 3, "b": 1, "c": 1}

    def test_empty_series_is_empty_dict(self):
        assert drift.count_categorical(pd.Series([], dtype=object)) == {}

    def test_nan_values_are_dropped(self):
        values = pd.Series(["a", None, "a", float("nan")])
        assert drift.count_categorical(values) == {"a": 2}


class TestBuildReferenceStatsAndFeaturePsi:
    def test_build_reference_stats_shape(self):
        df = pd.DataFrame(
            {
                "amount": np.linspace(1, 100, 200),
                "mcc": ([5411] * 150) + ([5812] * 50),
                "channel": (["chip"] * 100) + (["online"] * 100),
            }
        )
        ref = drift.build_reference_stats(df)
        assert ref["amount"]["type"] == "numeric"
        assert "edges" in ref["amount"] and "counts" in ref["amount"]
        assert ref["mcc"]["type"] == "categorical"
        assert ref["mcc"]["counts"] == {"5411": 150, "5812": 50}
        assert ref["channel"]["counts"] == {"chip": 100, "online": 100}

    def test_feature_psi_zero_when_current_matches_reference_exactly(self):
        df = pd.DataFrame({"amount": np.linspace(1, 100, 500), "mcc": [5411] * 500, "channel": ["chip"] * 500})
        ref = drift.build_reference_stats(df)
        current = pd.DataFrame({"amount": df["amount"], "mcc": df["mcc"], "channel": df["channel"]})
        psi_table = drift.compute_psi_table(ref, current)
        for psi in psi_table.values():
            assert psi == pytest.approx(0.0, abs=1e-6)

    def test_compute_psi_table_skips_missing_current_columns(self):
        df = pd.DataFrame({"amount": [1.0, 2.0, 3.0], "mcc": [1, 2, 3], "channel": ["a", "b", "c"]})
        ref = drift.build_reference_stats(df)
        current = pd.DataFrame({"amount": [1.0, 2.0, 3.0]})  # missing mcc/channel
        psi_table = drift.compute_psi_table(ref, current)
        assert set(psi_table) == {"amount"}

    def test_severe_shift_in_categorical_feature_exceeds_default_threshold(self):
        df = pd.DataFrame({"mcc": ([1] * 990) + ([2] * 10)})
        ref = drift.build_reference_stats(df, numeric_features=[], categorical_features=["mcc"])
        drifted = pd.DataFrame({"mcc": ([1] * 100) + ([2] * 900)})
        psi_table = drift.compute_psi_table(ref, drifted)
        assert psi_table["mcc"] > drift.DEFAULT_PSI_THRESHOLD


class TestManifestPointerResolution:
    def test_load_reference_stats_none_when_no_manifest(self, tmp_path):
        assert drift.load_reference_stats(str(tmp_path)) is None

    def test_load_reference_stats_flat_layout(self, tmp_path):
        manifest = {"drift_reference": {"amount": {"type": "numeric", "edges": [0, 1], "counts": {}}}}
        (tmp_path / "manifest.json").write_text(json.dumps(manifest))
        loaded = drift.load_reference_stats(str(tmp_path))
        assert loaded == manifest["drift_reference"]

    def test_load_reference_stats_follows_current_pointer(self, tmp_path):
        run_dir = tmp_path / "20260719T000000Z-abc1234"
        run_dir.mkdir()
        manifest = {"drift_reference": {"mcc": {"type": "categorical", "counts": {"1": 5}}}}
        (run_dir / "manifest.json").write_text(json.dumps(manifest))
        (tmp_path / "current.json").write_text(json.dumps({"run_id": run_dir.name}))
        loaded = drift.load_reference_stats(str(tmp_path))
        assert loaded == manifest["drift_reference"]

    def test_stale_pointer_falls_back_to_flat_layout(self, tmp_path):
        (tmp_path / "current.json").write_text(json.dumps({"run_id": "does-not-exist"}))
        flat_manifest = {"drift_reference": {"amount": {"type": "numeric", "edges": [0, 1], "counts": {}}}}
        (tmp_path / "manifest.json").write_text(json.dumps(flat_manifest))
        loaded = drift.load_reference_stats(str(tmp_path))
        assert loaded == flat_manifest["drift_reference"]

    def test_malformed_pointer_falls_back_to_flat_layout(self, tmp_path):
        (tmp_path / "current.json").write_text("not json")
        flat_manifest = {"drift_reference": {"amount": {"type": "numeric", "edges": [0, 1], "counts": {}}}}
        (tmp_path / "manifest.json").write_text(json.dumps(flat_manifest))
        loaded = drift.load_reference_stats(str(tmp_path))
        assert loaded == flat_manifest["drift_reference"]


class TestFetchAndCheckDrift:
    def test_fetch_recent_scored_transactions_rejects_non_positive_limit(self):
        engine = MagicMock()
        with pytest.raises(ValueError):
            drift.fetch_recent_scored_transactions(engine, limit=0)

    def test_check_drift_flags_exceeded_when_any_feature_over_threshold(self):
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        rows = [(10.0, 1, "chip")] * 5 + [(10.0, 2, "chip")] * 95
        conn.execute.return_value.fetchall.return_value = rows

        reference = drift.build_reference_stats(
            pd.DataFrame({"mcc": [1] * 995 + [2] * 5}),
            numeric_features=[],
            categorical_features=["mcc"],
        )
        psi_by_feature, exceeded = drift.check_drift(engine, reference, limit=100, threshold=0.2)
        assert exceeded is True
        assert psi_by_feature["mcc"] > 0.2

    def test_check_drift_not_exceeded_when_distributions_match(self):
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        rows = [(10.0, 1, "chip")] * 50 + [(10.0, 2, "chip")] * 50
        conn.execute.return_value.fetchall.return_value = rows

        reference = drift.build_reference_stats(
            pd.DataFrame({"mcc": [1] * 500 + [2] * 500}),
            numeric_features=[],
            categorical_features=["mcc"],
        )
        psi_by_feature, exceeded = drift.check_drift(engine, reference, limit=100, threshold=0.2)
        assert exceeded is False


class TestFormatDriftTable:
    def test_marks_drift_status_for_features_over_threshold(self):
        table = drift.format_drift_table({"amount": 0.05, "mcc": 0.35}, threshold=0.2)
        assert "amount" in table and "ok" in table
        assert "mcc" in table and "DRIFT" in table
