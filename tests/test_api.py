"""Unit tests for src.app: /score, /healthz, /metrics.

Hermetic by construction: no live Redis or model.json is required.
- A tiny XGBoost Booster is trained in-process on 100 random rows using the
  real FEATURE_COLUMNS from src.pipeline.features, so tests never depend on
  models/model.json (which may not exist / may be refreshed by another job).
- The Redis client is a small fake/stub injected via create_app(), so tests
  never touch a socket.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pytest
import redis
import xgboost as xgb

from src.app import create_app
from src.pipeline.features import FEATURE_COLUMNS


class FakeRedis:
    """Minimal stand-in for redis.Redis supporting only hgetall."""

    def __init__(self, hashes: dict[str, dict[str, str]] | None = None, raise_error: bool = False) -> None:
        self._hashes = hashes or {}
        self._raise_error = raise_error

    def hgetall(self, key: str) -> dict[str, str]:
        if self._raise_error:
            raise redis.exceptions.ConnectionError("simulated redis outage")
        return self._hashes.get(key, {})


@pytest.fixture(scope="module")
def tiny_booster() -> xgb.Booster:
    """Train a tiny Booster on 100 random rows using the real FEATURE_COLUMNS
    so the model vector shape always matches what src.app builds."""
    rng = np.random.default_rng(42)
    n = 100
    x = rng.random((n, len(FEATURE_COLUMNS)))
    y = rng.integers(0, 2, size=n)
    dtrain = xgb.DMatrix(x, label=y, feature_names=FEATURE_COLUMNS)
    booster = xgb.train({"objective": "binary:logistic", "max_depth": 2}, dtrain, num_boost_round=5)
    return booster


def _sample_event(**overrides: Any) -> dict[str, Any]:
    base = {
        "schema_version": "1.0.0",
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
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
    base.update(overrides)
    return base


def _make_client(tiny_booster: xgb.Booster, redis_client: Any):
    app = create_app(
        model=tiny_booster,
        threshold=0.5,
        feature_columns=FEATURE_COLUMNS,
        redis_client=redis_client,
    )
    app.config["TESTING"] = True
    return app.test_client()


def test_score_valid_event_returns_probability(tiny_booster: xgb.Booster) -> None:
    client = _make_client(tiny_booster, FakeRedis())
    resp = client.post("/score", json=_sample_event())
    assert resp.status_code == 200
    body = resp.get_json()
    assert 0.0 <= body["fraud_probability"] <= 1.0
    assert body["decision"] in ("approve", "review")
    assert body["threshold"] == 0.5
    assert isinstance(body["latency_ms"], float)


def test_score_invalid_event_missing_field_returns_400(tiny_booster: xgb.Booster) -> None:
    client = _make_client(tiny_booster, FakeRedis())
    event = _sample_event()
    del event["card_token"]
    resp = client.post("/score", json=event)
    assert resp.status_code == 400
    body = resp.get_json()
    assert "error" in body


def test_score_cold_card_when_hash_missing(tiny_booster: xgb.Booster) -> None:
    client = _make_client(tiny_booster, FakeRedis(hashes={}))
    resp = client.post("/score", json=_sample_event())
    assert resp.status_code == 200
    assert resp.get_json()["cold_card"] is True


def test_score_warm_card_when_hash_present(tiny_booster: xgb.Booster) -> None:
    card_token = "b" * 64
    warm_hash = {
        "txn_count_1m": "3",
        "amount_sum_1m": "150.0",
        "amount_mean_1m": "50.0",
        "distinct_merchant_city_1m": "2",
        "decline_rate_1m": "0.0",
    }
    client = _make_client(tiny_booster, FakeRedis(hashes={f"features:{card_token}": warm_hash}))
    resp = client.post("/score", json=_sample_event(card_token=card_token))
    assert resp.status_code == 200
    assert resp.get_json()["cold_card"] is False


def test_score_redis_down_falls_back_to_cold_card_not_500(tiny_booster: xgb.Booster) -> None:
    client = _make_client(tiny_booster, FakeRedis(raise_error=True))
    resp = client.post("/score", json=_sample_event())
    assert resp.status_code == 200
    assert resp.get_json()["cold_card"] is True


def test_healthz_ok(tiny_booster: xgb.Booster) -> None:
    client = _make_client(tiny_booster, FakeRedis())
    resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["model_loaded"] is True


def test_healthz_503_when_model_missing() -> None:
    app = create_app(model=None, threshold=0.5, feature_columns=FEATURE_COLUMNS, redis_client=FakeRedis())
    client = app.test_client()
    resp = client.get("/healthz")
    assert resp.status_code == 503
    assert resp.get_json()["model_loaded"] is False


def test_metrics_exposes_histogram(tiny_booster: xgb.Booster) -> None:
    client = _make_client(tiny_booster, FakeRedis())
    client.post("/score", json=_sample_event())
    resp = client.get("/metrics")
    assert resp.status_code == 200
    text = resp.get_data(as_text=True)
    assert "score_latency_seconds" in text
    assert "http_requests_total" in text
