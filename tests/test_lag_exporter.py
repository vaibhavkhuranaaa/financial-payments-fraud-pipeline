"""Unit tests for src.pipeline.lag_exporter.

Hermetic — no Kafka broker, no live DB. Lag arithmetic and the LAG_GROUPS
parser are pure; `collect_kafka_lag` runs against a mocked Consumer,
`collect_bank_metrics` against a mocked engine, and `update_once` is pinned
end-to-end by rendering the registry to Prometheus text format.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from src.pipeline import lag_exporter


class TestParseGroupTopics:
    def test_parses_pairs(self):
        assert lag_exporter.parse_group_topics("scorer:transactions,ct:bankdb.bank.t") == [
            ("scorer", "transactions"),
            ("ct", "bankdb.bank.t"),
        ]

    def test_topic_may_contain_dots_but_group_split_is_first_colon(self):
        assert lag_exporter.parse_group_topics("g:a:b") == [("g", "a:b")]

    def test_rejects_missing_topic(self):
        with pytest.raises(ValueError):
            lag_exporter.parse_group_topics("scorer")

    def test_ignores_empty_chunks(self):
        assert lag_exporter.parse_group_topics(" scorer:transactions , ") == [
            ("scorer", "transactions")
        ]


class TestComputeLag:
    def test_normal_lag(self):
        assert lag_exporter.compute_lag(committed=40, low=0, high=100) == 60

    def test_caught_up(self):
        assert lag_exporter.compute_lag(committed=100, low=0, high=100) == 0

    def test_no_committed_offset_counts_all_retained(self):
        # OFFSET_INVALID (-1001) / None => group has consumed nothing: lag is
        # everything between the watermarks, not high-(-1001).
        assert lag_exporter.compute_lag(committed=-1001, low=10, high=100) == 90
        assert lag_exporter.compute_lag(committed=None, low=10, high=100) == 90

    def test_never_negative(self):
        # committed can transiently exceed the cached high watermark
        assert lag_exporter.compute_lag(committed=101, low=0, high=100) == 0


def _mock_consumer(partitions: dict[int, int], watermarks: dict[int, tuple[int, int]]):
    """Consumer-shaped mock: `partitions` maps partition -> committed offset,
    `watermarks` maps partition -> (low, high)."""
    consumer = MagicMock()
    topic_meta = MagicMock()
    topic_meta.error = None
    topic_meta.partitions = dict.fromkeys(partitions)
    meta = MagicMock()
    meta.topics = {"transactions": topic_meta}
    consumer.list_topics.return_value = meta

    def committed(tps, timeout):
        out = []
        for tp in tps:
            committed_tp = MagicMock()
            committed_tp.partition = tp.partition
            committed_tp.offset = partitions[tp.partition]
            out.append(committed_tp)
        return out

    consumer.committed.side_effect = committed
    consumer.get_watermark_offsets.side_effect = lambda tp, timeout: watermarks[tp.partition]
    return consumer


class TestCollectKafkaLag:
    def test_lag_per_partition(self):
        consumer = _mock_consumer({0: 40, 1: -1001}, {0: (0, 100), 1: (5, 25)})
        samples = lag_exporter.collect_kafka_lag(
            {"scorer": consumer}, [("scorer", "transactions")]
        )
        by_partition = {s["partition"]: s for s in samples}
        assert by_partition[0] == {
            "group": "scorer",
            "topic": "transactions",
            "partition": 0,
            "lag": 60,
        }
        assert by_partition[1]["lag"] == 20

    def test_missing_topic_is_skipped_not_fatal(self):
        consumer = MagicMock()
        meta = MagicMock()
        meta.topics = {}
        consumer.list_topics.return_value = meta
        samples = lag_exporter.collect_kafka_lag({"g": consumer}, [("g", "not-yet")])
        assert samples == []


class TestCollectBankMetrics:
    def _engine(self, counts, staleness):
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        results = [*counts, staleness]
        conn.execute.side_effect = [
            MagicMock(scalar_one=MagicMock(return_value=value)) for value in results
        ]
        return engine

    def test_counts_and_staleness(self):
        engine = self._engine([100, 7, 5000], 12)
        out = lag_exporter.collect_bank_metrics(engine)
        assert out["rows"] == {
            "scored_transactions": 100,
            "fraud_alerts": 7,
            "card_transactions": 5000,
        }
        assert out["staleness_seconds"] == 12.0

    def test_empty_table_gives_nan_staleness(self):
        engine = self._engine([0, 0, 0], None)
        out = lag_exporter.collect_bank_metrics(engine)
        assert out["staleness_seconds"] != out["staleness_seconds"]  # NaN


class TestUpdateOnceRendering:
    def test_renders_expected_series(self):
        registry = CollectorRegistry()
        metrics = lag_exporter.build_metrics(registry)
        consumer = _mock_consumer({0: 40}, {0: (0, 100)})
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.side_effect = [
            MagicMock(scalar_one=MagicMock(return_value=value)) for value in [10, 2, 30, 3]
        ]

        lag_exporter.update_once(metrics, {"scorer": consumer}, [("scorer", "transactions")], engine)

        text = generate_latest(registry).decode()
        assert (
            'kafka_consumergroup_lag{group="scorer",partition="0",topic="transactions"} 60.0'
            in text
        )
        assert 'bank_rows_total{table="scored_transactions"} 10.0' in text
        assert 'bank_rows_total{table="fraud_alerts"} 2.0' in text
        assert "scoring_staleness_seconds 3.0" in text
        assert 'lag_exporter_target_up{target="kafka"} 1.0' in text
        assert 'lag_exporter_target_up{target="bankdb"} 1.0' in text

    def test_backend_failures_are_isolated(self):
        registry = CollectorRegistry()
        metrics = lag_exporter.build_metrics(registry)
        consumer = MagicMock()
        consumer.list_topics.side_effect = RuntimeError("broker down")
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.side_effect = [
            MagicMock(scalar_one=MagicMock(return_value=value)) for value in [10, 2, 30, 3]
        ]

        lag_exporter.update_once(metrics, {"scorer": consumer}, [("scorer", "transactions")], engine)

        text = generate_latest(registry).decode()
        # Kafka side marked down + error counted; bank side still fully populated.
        assert 'lag_exporter_target_up{target="kafka"} 0.0' in text
        assert 'lag_exporter_poll_errors_total{target="kafka"} 1.0' in text
        assert 'bank_rows_total{table="scored_transactions"} 10.0' in text
        assert 'lag_exporter_target_up{target="bankdb"} 1.0' in text
