"""Unit tests for src.pipeline.scorer.

Hermetic — no Kafka broker, no live DB, no live HTTP. `handle_event` is
exercised directly with a fake Kafka-message-shaped dict, a mocked
`requests.Session`, and a mocked SQLAlchemy engine; tests call `flush`
explicitly (flush/commit ownership lives in `run()`, which is exercised with
a fake confluent_kafka Consumer to pin the commit-after-flush ordering).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests
from sqlalchemy.exc import IntegrityError

from src.pipeline import scorer

EVENT = {
    "schema_version": "1.0.0",
    "event_id": "11111111-1111-1111-1111-111111111111",
    "event_time": "2019-02-13T14:06:00Z",
    "card_token": "a" * 64,
    "user_id": "19",
    "amount": 80.0,
    "currency": "USD",
    "channel": "chip",
    "merchant_name": "Some Merchant",
    "merchant_city": "Tucson",
    "merchant_state": "AZ",
    "merchant_country": "US",
    "zip": "85719",
    "mcc": 4829,
    "errors": None,
    "is_fraud": None,
}


def _mock_response(status_code: int, payload: dict | None = None, text: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload or {}
    resp.text = text
    return resp


def _mock_engine() -> tuple[MagicMock, MagicMock]:
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_engine.begin.return_value.__exit__.return_value = False
    return mock_engine, mock_conn


@pytest.fixture(autouse=True)
def _reset_alert_threshold():
    """ALERT_THRESHOLD is read once at import time; tests override the module
    global directly and must restore it so tests don't leak state."""
    original = scorer.ALERT_THRESHOLD
    yield
    scorer.ALERT_THRESHOLD = original


def test_handle_event_writes_scored_transactions_row_shape() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _mock_response(
        200, {"fraud_probability": 0.1, "decision": "approve", "threshold": 0.5, "cold_card": True, "latency_ms": 3.2}
    )
    mock_engine, mock_conn = _mock_engine()
    buffer = scorer.ScoreBuffer(batch_size=1)

    result = scorer.handle_event(EVENT, session, mock_engine, buffer)
    scorer.flush(mock_engine, buffer)

    assert result is True
    session.post.assert_called_once()
    called_url = session.post.call_args.args[0] if session.post.call_args.args else session.post.call_args.kwargs["url"]
    assert called_url == scorer.SCORE_URL

    # one execute() for scored_transactions (alert_rows empty -> insert skipped)
    assert mock_conn.execute.call_count == 1
    stmt, rows = mock_conn.execute.call_args.args
    assert stmt is scorer._INSERT_SCORED_TXN_SQL
    assert len(rows) == 1
    row = rows[0]
    assert row["event_id"] == EVENT["event_id"]
    assert row["card_token"] == EVENT["card_token"]
    assert row["amount"] == EVENT["amount"]
    assert row["merchant_name"] == EVENT["merchant_name"]
    assert row["merchant_city"] == EVENT["merchant_city"]
    assert row["merchant_state"] == EVENT["merchant_state"]
    assert row["mcc"] == EVENT["mcc"]
    assert row["channel"] == EVENT["channel"]
    assert row["fraud_probability"] == 0.1
    assert row["decision"] == "approve"
    assert row["cold_card"] is True
    assert row["latency_ms"] == 3.2
    assert row["event_time"].isoformat() == "2019-02-13T14:06:00"


def test_handle_event_inserts_alert_only_above_threshold() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _mock_response(
        200, {"fraud_probability": 0.9, "decision": "review", "threshold": 0.5, "cold_card": False, "latency_ms": 5.0}
    )
    mock_engine, mock_conn = _mock_engine()
    buffer = scorer.ScoreBuffer(batch_size=1)

    scorer.handle_event(EVENT, session, mock_engine, buffer)
    scorer.flush(mock_engine, buffer)

    # two execute()s: scored_transactions then fraud_alerts
    assert mock_conn.execute.call_count == 2
    stmt, rows = mock_conn.execute.call_args_list[1].args
    assert stmt is scorer._INSERT_FRAUD_ALERT_SQL
    assert len(rows) == 1
    alert = rows[0]
    assert alert["event_id"] == EVENT["event_id"]
    assert alert["fraud_probability"] == 0.9
    assert alert["amount"] == EVENT["amount"]


def test_handle_event_below_threshold_writes_no_alert() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _mock_response(
        200, {"fraud_probability": 0.2, "decision": "approve", "threshold": 0.5, "cold_card": False, "latency_ms": 4.0}
    )
    mock_engine, mock_conn = _mock_engine()
    buffer = scorer.ScoreBuffer(batch_size=1)

    scorer.handle_event(EVENT, session, mock_engine, buffer)
    scorer.flush(mock_engine, buffer)

    assert mock_conn.execute.call_count == 1  # only scored_transactions


def test_alert_threshold_env_override_takes_priority_over_response_threshold() -> None:
    scorer.ALERT_THRESHOLD = 0.3
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _mock_response(
        200, {"fraud_probability": 0.4, "decision": "approve", "threshold": 0.9, "cold_card": False, "latency_ms": 1.0}
    )
    mock_engine, mock_conn = _mock_engine()
    buffer = scorer.ScoreBuffer(batch_size=1)

    scorer.handle_event(EVENT, session, mock_engine, buffer)
    scorer.flush(mock_engine, buffer)

    # response threshold (0.9) would not have alerted; override (0.3) does
    assert mock_conn.execute.call_count == 2


def test_duplicate_event_id_swallowed_on_batch_insert() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _mock_response(
        200, {"fraud_probability": 0.1, "decision": "approve", "threshold": 0.5, "cold_card": False, "latency_ms": 2.0}
    )
    mock_engine, mock_conn = _mock_engine()
    # First execute() (the batch attempt) raises duplicate-key IntegrityError;
    # the per-row fallback also raises for one row.
    mock_conn.execute.side_effect = [
        IntegrityError("INSERT", {}, Exception("duplicate key")),
        IntegrityError("INSERT", {}, Exception("duplicate key")),
    ]
    buffer = scorer.ScoreBuffer(batch_size=1)

    # Should not raise: duplicate key is swallowed.
    scorer.handle_event(EVENT, session, mock_engine, buffer)
    scorer.flush(mock_engine, buffer)

    assert mock_conn.execute.call_count == 2  # batch attempt + 1 row fallback


def test_score_event_retries_5xx_then_skips() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _mock_response(500, text="boom")

    with patch("src.pipeline.scorer.time.sleep"):
        result = scorer.score_event(session, EVENT)

    assert result is None
    assert session.post.call_count == scorer.SCORE_MAX_ATTEMPTS


def test_score_event_skips_4xx_without_retry() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _mock_response(400, text="bad request")

    result = scorer.score_event(session, EVENT)

    assert result is None
    assert session.post.call_count == 1


def test_score_event_succeeds_after_transient_5xx() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.side_effect = [
        _mock_response(500, text="transient"),
        _mock_response(200, {"fraud_probability": 0.05, "decision": "approve", "threshold": 0.5, "cold_card": True, "latency_ms": 2.0}),
    ]

    with patch("src.pipeline.scorer.time.sleep"):
        result = scorer.score_event(session, EVENT)

    assert result is not None
    assert result["fraud_probability"] == 0.05
    assert session.post.call_count == 2


def test_handle_event_skips_and_returns_false_when_score_fails() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _mock_response(400, text="bad")
    mock_engine, mock_conn = _mock_engine()
    buffer = scorer.ScoreBuffer(batch_size=1)

    result = scorer.handle_event(EVENT, session, mock_engine, buffer)

    assert result is False
    mock_conn.execute.assert_not_called()


def test_score_buffer_defers_flush_until_batch_size_reached() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _mock_response(
        200, {"fraud_probability": 0.1, "decision": "approve", "threshold": 0.5, "cold_card": False, "latency_ms": 1.0}
    )
    mock_engine, mock_conn = _mock_engine()
    buffer = scorer.ScoreBuffer(batch_size=2, flush_interval_s=999)

    event_1 = dict(EVENT, event_id="22222222-2222-2222-2222-222222222222")
    scorer.handle_event(event_1, session, mock_engine, buffer)
    assert buffer.should_flush() is False  # below batch_size, not flushable yet
    mock_conn.execute.assert_not_called()

    event_2 = dict(EVENT, event_id="33333333-3333-3333-3333-333333333333")
    scorer.handle_event(event_2, session, mock_engine, buffer)
    assert buffer.should_flush() is True
    scorer.flush(mock_engine, buffer)

    assert mock_conn.execute.call_count == 1
    _, rows = mock_conn.execute.call_args.args
    assert len(rows) == 2


def test_kafka_consumer_config_sets_group_and_offset_reset() -> None:
    config = scorer._consumer_config()
    assert config["group.id"] == "scorer"
    assert config["auto.offset.reset"] == "earliest"
    assert "bootstrap.servers" in config
    # Delivery semantics: offsets are committed manually, strictly after the
    # SQL flush — auto-commit must be off or the boundary is meaningless.
    assert config["enable.auto.commit"] is False


class _FakeMessage:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def error(self):  # noqa: ANN201 — mirrors confluent_kafka.Message
        return None

    def value(self) -> bytes:
        return self._payload


class _FakeConsumer:
    """Stands in for confluent_kafka.Consumer: serves canned messages and
    records commit() calls into a shared ordered log."""

    def __init__(self, messages: list[_FakeMessage], log: list[str]) -> None:
        self._messages = list(messages)
        self._log = log

    def subscribe(self, topics: list[str]) -> None:
        self.topics = topics

    def poll(self, timeout: float) -> _FakeMessage | None:
        return self._messages.pop(0) if self._messages else None

    def commit(self, asynchronous: bool = True) -> None:
        assert asynchronous is False, "offset commits must be synchronous"
        self._log.append("commit")

    def close(self) -> None:
        self._log.append("close")


def test_run_commits_offsets_only_after_sql_flush() -> None:
    """The delivery-semantics boundary: with 60 messages and BATCH_SIZE=50,
    run() must flush-then-commit mid-stream at 50 rows, and flush-then-commit
    the 10-row remainder on shutdown — never commit before a flush."""
    import json as _json

    log: list[str] = []
    payload = _json.dumps(EVENT).encode("utf-8")
    messages = [_FakeMessage(payload) for _ in range(60)]

    def fake_flush(engine, buffer) -> None:  # noqa: ANN001 — patch target
        log.append("flush")
        buffer.mark_flushed()

    score_response = {
        "fraud_probability": 0.1,
        "decision": "approve",
        "threshold": 0.5,
        "cold_card": False,
        "latency_ms": 1.0,
    }
    with (
        patch("confluent_kafka.Consumer", lambda config: _FakeConsumer(messages, log)),
        patch("src.pipeline.scorer.get_engine", return_value=MagicMock()),
        patch("src.pipeline.scorer.score_event", return_value=score_response),
        patch("src.pipeline.scorer.flush", side_effect=fake_flush),
    ):
        scored = scorer.run(max_events=60)

    assert scored == 60
    assert log == ["flush", "commit", "flush", "commit", "close"]
