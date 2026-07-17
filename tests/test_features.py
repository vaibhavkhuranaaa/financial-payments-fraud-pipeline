"""Unit tests for src.pipeline.features: shared enrichment + windowed features.

No Kafka/Redis/Spark required for the core tests. Anything that needs a real
Spark session is marked `@pytest.mark.spark` and skipped if pyspark can't
start in this environment, per the ticket's test-isolation requirement.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.pipeline.features import (
    CARD_WINDOWS,
    FEATURE_COLUMNS,
    MCC_GROUP_IDS,
    build_feature_row,
    compute_offline_card_features,
    enrich,
    window_feature_names,
)


def _event(
    card_token: str = "cardA",
    event_time: str = "2019-06-01T12:00:00Z",
    amount: float = 10.0,
    channel: str = "chip",
    merchant_country: str = "US",
    merchant_city: str = "Tucson",
    mcc: int = 5411,
    errors: str | None = None,
) -> dict:
    return {
        "card_token": card_token,
        "event_time": event_time,
        "amount": amount,
        "channel": channel,
        "merchant_country": merchant_country,
        "merchant_city": merchant_city,
        "mcc": mcc,
        "errors": errors,
    }


def test_enrich_is_cnp_flag() -> None:
    assert enrich(_event(channel="online"))["is_cnp"] is True
    assert enrich(_event(channel="chip"))["is_cnp"] is False
    assert enrich(_event(channel="swipe"))["is_cnp"] is False


def test_enrich_cross_border_flag() -> None:
    assert enrich(_event(merchant_country="US"))["is_cross_border"] is False
    assert enrich(_event(merchant_country="XX"))["is_cross_border"] is False
    assert enrich(_event(merchant_country="IT"))["is_cross_border"] is True


def test_enrich_mcc_group() -> None:
    assert enrich(_event(mcc=5411))["mcc_group"] == "grocery"  # grocery stores
    assert enrich(_event(mcc=4511))["mcc_group"] == "travel"  # airlines
    assert enrich(_event(mcc=6011))["mcc_group"] == "cash"  # ATM cash disbursement
    assert enrich(_event(mcc=5964))["mcc_group"] == "online_retail"  # direct marketing
    assert enrich(_event(mcc=1234))["mcc_group"] == "other"


def test_enrich_mcc_group_travel_range_boundary() -> None:
    """The 3000-3999 range rule (airlines/car rental/lodging) in `_mcc_group`
    has an inclusive boundary on both ends; regressing it would silently
    change mcc_group_id for those codes with no other test catching it."""
    assert enrich(_event(mcc=2999))["mcc_group"] == "other"  # just below range
    assert enrich(_event(mcc=3000))["mcc_group"] == "travel"  # range start
    assert enrich(_event(mcc=3999))["mcc_group"] == "travel"  # range end
    assert enrich(_event(mcc=4000))["mcc_group"] == "other"  # just above range


def test_mcc_group_id_ordinals_are_stable() -> None:
    """mcc_group_id is a model-facing ordinal encoding — the numeric ids
    themselves (not just the group names) are part of the trained model's
    contract and must not silently shift. Also the values the dbt
    `mcc_group`/`fct_merchant_risk` mart mirror (docs/governance/lineage.md)."""
    assert MCC_GROUP_IDS == {
        "travel": 0,
        "grocery": 1,
        "cash": 2,
        "online_retail": 3,
        "other": 4,
    }


def test_enrich_amount_log_and_time_parts() -> None:
    result = enrich(_event(amount=-54.0, event_time="2019-06-01T13:30:00Z"))
    assert result["amount_log"] > 0  # log1p(abs(amount)), not log1p(amount)
    assert result["hour_of_day"] == 13
    assert result["day_of_week"] == 5  # Saturday, 2019-06-01


def test_window_feature_names_cover_all_windows() -> None:
    for window_key in CARD_WINDOWS:
        names = window_feature_names(window_key)
        assert names == [
            f"txn_count_{window_key}",
            f"amount_sum_{window_key}",
            f"amount_mean_{window_key}",
            f"distinct_merchant_city_{window_key}",
            f"decline_rate_{window_key}",
        ]


def test_feature_columns_match_enrichment_and_windows() -> None:
    expected_event_level = {
        "amount",
        "amount_log",
        "is_cnp",
        "is_cross_border",
        "mcc_group_id",
        "hour_of_day",
        "day_of_week",
    }
    assert expected_event_level.issubset(set(FEATURE_COLUMNS))
    for window_key in CARD_WINDOWS:
        for name in window_feature_names(window_key):
            assert name in FEATURE_COLUMNS


def test_leakage_safety_three_event_toy_history() -> None:
    """Feature at event t must only reflect events strictly before t."""
    events = [
        _event(event_time="2019-06-01T12:00:00Z", amount=10.0, merchant_city="A"),
        _event(event_time="2019-06-01T12:00:10Z", amount=20.0, merchant_city="B"),
        _event(event_time="2019-06-01T12:00:20Z", amount=30.0, merchant_city="C"),
    ]
    features = compute_offline_card_features(events)

    # First event in the card's history has no prior events at all.
    assert features[0]["txn_count_1m"] == 0.0
    assert features[0]["amount_sum_1m"] == 0.0
    assert features[0]["distinct_merchant_city_1m"] == 0.0

    # Second event's 1m-window features reflect ONLY the first event, never itself.
    assert features[1]["txn_count_1m"] == 1.0
    assert features[1]["amount_sum_1m"] == 10.0
    assert features[1]["distinct_merchant_city_1m"] == 1.0

    # Third event reflects the first two, not itself (amount 30 excluded from sum).
    assert features[2]["txn_count_1m"] == 2.0
    assert features[2]["amount_sum_1m"] == 30.0  # 10 + 20, NOT +30
    assert features[2]["distinct_merchant_city_1m"] == 2.0  # A, B — not C


def test_leakage_safety_respects_window_eviction() -> None:
    """Events older than the window size must drop out of the aggregate."""
    events = [
        _event(event_time="2019-06-01T12:00:00Z", amount=10.0),
        _event(event_time="2019-06-01T12:02:00Z", amount=20.0),  # 2 min later, outside 1m window
    ]
    features = compute_offline_card_features(events)
    assert features[1]["txn_count_1m"] == 0.0
    assert features[1]["amount_sum_1m"] == 0.0
    # But the 10m/1h windows still include the first event.
    assert features[1]["txn_count_10m"] == 1.0
    assert features[1]["amount_sum_10m"] == 10.0


def test_offline_features_are_order_independent_on_input() -> None:
    """compute_offline_card_features must sort internally; shuffled input order
    should produce the same per-event features once re-aligned by time."""
    events_in_order = [
        _event(event_time="2019-06-01T12:00:00Z", amount=1.0),
        _event(event_time="2019-06-01T12:00:05Z", amount=2.0),
        _event(event_time="2019-06-01T12:00:10Z", amount=3.0),
    ]
    shuffled = [events_in_order[2], events_in_order[0], events_in_order[1]]

    features_in_order = compute_offline_card_features(events_in_order)
    features_shuffled = compute_offline_card_features(shuffled)

    # shuffled[1] is events_in_order[0] -> should match features_in_order[0]
    assert features_shuffled[1] == features_in_order[0]
    # shuffled[2] is events_in_order[1] -> should match features_in_order[1]
    assert features_shuffled[2] == features_in_order[1]
    # shuffled[0] is events_in_order[2] -> should match features_in_order[2]
    assert features_shuffled[0] == features_in_order[2]


def test_build_feature_row_combines_enrichment_and_window_features() -> None:
    event = _event()
    window_features = compute_offline_card_features([event])[0]
    row = build_feature_row(event, window_features)
    for col in FEATURE_COLUMNS:
        assert col in row


@pytest.mark.spark
def test_event_schema_builds_with_pyspark() -> None:
    """Spark-dependent: only exercises schema construction, no cluster I/O."""
    pyspark = pytest.importorskip("pyspark")
    try:
        from pyspark.sql import SparkSession

        SparkSession.builder.master("local[1]").appName("test").getOrCreate().stop()
    except Exception as exc:  # noqa: BLE001 - environment-dependent Spark startup
        pytest.skip(f"pyspark session could not start: {exc}")

    from src.pipeline.features import _event_schema

    schema = _event_schema()
    field_names = {f.name for f in schema.fields}
    assert "card_token" in field_names
    assert "event_time" in field_names
    _ = pyspark


def test_vectorized_matches_row_wise_on_sample(tmp_path) -> None:
    """The 24M-row full-dataset training run needs a vectorized fast path
    (src.pipeline.train.load_events_vectorized / build_dataset_vectorized)
    because the row-wise reference path (one Python dict + jsonschema
    validation per event) is too slow/memory-heavy at that scale. This test
    is the correctness guarantee for that fast path: on a ~5k-row slice of
    the committed sample CSV, the vectorized build must produce the exact
    same labels/order and numerically equal (allclose) feature values as the
    row-wise reference for every column in FEATURE_COLUMNS.
    """
    from src.pipeline.train import (
        build_dataset,
        build_dataset_vectorized,
        load_events,
        load_events_vectorized,
    )

    sample_path = "data/sample/transactions_sample.csv"
    small_csv = tmp_path / "small5k.csv"
    with open(sample_path, encoding="utf-8") as src:
        lines = src.readlines()
    small_csv.write_text("".join(lines[:5001]), encoding="utf-8")  # header + 5000 rows

    salt = "test-salt"

    events, skipped_row = load_events(str(small_csv), salt)
    df_row = build_dataset(events)

    events_df, skipped_vec = load_events_vectorized(str(small_csv), salt)
    df_vec = build_dataset_vectorized(events_df)

    assert skipped_row == skipped_vec
    assert len(df_row) == len(df_vec) == 5000

    # Same event order (both sorted by event_time ascending) and same labels.
    row_times = pd.to_datetime(df_row["event_time"], utc=True).to_numpy()
    vec_times = pd.to_datetime(df_vec["event_time"], utc=True).to_numpy()
    assert (row_times == vec_times).all()
    assert (df_row["is_fraud"].to_numpy() == df_vec["is_fraud"].to_numpy()).all()

    for col in FEATURE_COLUMNS:
        row_values = df_row[col].to_numpy(dtype=float)
        vec_values = df_vec[col].to_numpy(dtype=float)
        assert np.allclose(row_values, vec_values, atol=1e-6), f"mismatch in column {col}"
