"""Flask scoring microservice — POST /score, GET /healthz, GET /metrics.

Serving contract (see docs/tickets/02-api.md + docs/adr/0001):
the streaming job (``src/pipeline/features.py``) maintains a Redis hash
``features:{card_token}`` with the latest per-card windowed features. This
service enriches an incoming contract-v1 transaction event with the SAME
``enrich()`` used at training time (train/serve-skew prevention), joins the
Redis window features, and scores the resulting vector with the XGBoost
model trained by ``src/pipeline/train.py``.

Redis is a soft dependency: any failure to reach it (down, absent, slow)
falls back to zero-valued window features and marks the response
``cold_card: true`` — the endpoint never 500s because of Redis.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import redis
import xgboost as xgb
from dotenv import load_dotenv
from flask import Flask, Response, current_app, jsonify, request
from jsonschema import Draft202012Validator
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from src.pipeline.features import CARD_WINDOWS, enrich, window_feature_names

load_dotenv()

logger = logging.getLogger(__name__)

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SCHEMA_PATH = os.path.join(_REPO_ROOT, "contracts", "transaction.schema.json")

# --- Env-driven configuration (defaults mirror .env.example) ---------------

MODEL_DIR = os.environ.get("MODEL_DIR", "models")
SCORE_THRESHOLD_PATH = os.environ.get("SCORE_THRESHOLD_PATH", "models/threshold.json")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_SOCKET_TIMEOUT = float(os.environ.get("REDIS_SOCKET_TIMEOUT", "0.05"))

# --- Prometheus metrics (module-level: one process-wide registry) ----------

REQUEST_COUNTER = Counter(
    "http_requests_total",
    "Total HTTP requests handled, by endpoint and status code.",
    ["endpoint", "status"],
)
SCORE_LATENCY = Histogram(
    "score_latency_seconds",
    "Server-side scoring path latency (enrich + Redis join + model predict), seconds.",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0),
)
REDIS_ERRORS = Counter(
    "redis_errors_total",
    "Redis failures on the online-feature lookup path (triggers cold-card fallback).",
)


def _load_schema_validator() -> Draft202012Validator:
    with open(_SCHEMA_PATH, encoding="utf-8") as f:
        schema = json.load(f)
    return Draft202012Validator(schema)


_VALIDATOR = _load_schema_validator()


def _validate_event(event: dict[str, Any]) -> str | None:
    """Return None if `event` matches the contract-v1 schema, else a reason string."""
    errors = sorted(_VALIDATOR.iter_errors(event), key=lambda e: e.path)
    if not errors:
        return None
    first = errors[0]
    path = "/".join(str(p) for p in first.path) or "<root>"
    return f"{path}: {first.message}"


def _load_model(model_dir: str) -> xgb.Booster | None:
    model_path = os.path.join(model_dir, "model.json")
    if not os.path.exists(model_path):
        logger.warning("model file not found at %s; /score will 503", model_path)
        return None
    booster = xgb.Booster()
    booster.load_model(model_path)
    return booster


def _load_threshold(threshold_path: str) -> float:
    if not os.path.exists(threshold_path):
        logger.warning("threshold file not found at %s; defaulting to 0.5", threshold_path)
        return 0.5
    with open(threshold_path, encoding="utf-8") as f:
        return float(json.load(f)["threshold"])


def _load_feature_columns(model_dir: str) -> list[str]:
    path = os.path.join(model_dir, "feature_columns.json")
    with open(path, encoding="utf-8") as f:
        return list(json.load(f))


def _build_redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        socket_timeout=REDIS_SOCKET_TIMEOUT,
        socket_connect_timeout=REDIS_SOCKET_TIMEOUT,
        decode_responses=True,
    )


def _fetch_window_features(redis_client: redis.Redis | None, card_token: str) -> tuple[dict[str, str], bool]:
    """Fetch the online feature hash for `card_token`.

    Returns (hash_or_empty, cold_card). Any Redis error (down, timeout,
    absent) is swallowed here and reported as a cold card — the caller must
    never see a Redis exception.
    """
    if redis_client is None:
        return {}, True
    try:
        hash_data = redis_client.hgetall(f"features:{card_token}")
    except redis.exceptions.RedisError:
        REDIS_ERRORS.inc()
        return {}, True
    if not hash_data:
        return {}, True
    return hash_data, False


def _build_feature_vector(
    event: dict[str, Any],
    feature_columns: list[str],
    redis_client: redis.Redis | None,
) -> tuple[list[float], bool]:
    """Enrich `event` + join Redis window features into a model-ready vector,
    ordered exactly per `feature_columns`. Returns (vector, cold_card)."""
    enriched = enrich(event)
    window_hash, cold_card = _fetch_window_features(redis_client, event["card_token"])

    row: dict[str, float] = {
        "amount": float(event["amount"]),
        "amount_log": float(enriched["amount_log"]),
        "is_cnp": float(int(enriched["is_cnp"])),
        "is_cross_border": float(int(enriched["is_cross_border"])),
        "mcc_group_id": float(enriched["mcc_group_id"]),
        "hour_of_day": float(enriched["hour_of_day"]),
        "day_of_week": float(enriched["day_of_week"]),
    }
    for window_key in CARD_WINDOWS:
        for name in window_feature_names(window_key):
            row[name] = float(window_hash.get(name, 0.0))

    vector = [row[col] for col in feature_columns]
    return vector, cold_card


_UNSET: Any = object()


def create_app(
    model: xgb.Booster | None = _UNSET,
    threshold: float | None = None,
    feature_columns: list[str] | None = None,
    redis_client: redis.Redis | None = None,
) -> Flask:
    """Flask application factory.

    All four dependencies are optional overrides so tests can inject a tiny
    in-memory Booster / fake Redis client without touching disk or network;
    production (gunicorn) calls `create_app()` with no arguments and loads
    everything from `MODEL_DIR` / `SCORE_THRESHOLD_PATH` / env once at
    startup. `model` uses a sentinel default (rather than `None`) so tests can
    pass `model=None` explicitly to exercise the "model missing" path without
    it falling back to loading the real model from `MODEL_DIR`.
    """
    app = Flask(__name__)

    app.config["MODEL"] = _load_model(MODEL_DIR) if model is _UNSET else model
    app.config["THRESHOLD"] = threshold if threshold is not None else _load_threshold(SCORE_THRESHOLD_PATH)
    app.config["FEATURE_COLUMNS"] = (
        feature_columns if feature_columns is not None else _load_feature_columns(MODEL_DIR)
    )
    app.config["REDIS_CLIENT"] = redis_client if redis_client is not None else _build_redis_client()

    @app.after_request
    def _record_metrics(response: Response) -> Response:
        endpoint = request.endpoint or "unknown"
        REQUEST_COUNTER.labels(endpoint=endpoint, status=str(response.status_code)).inc()
        return response

    @app.route("/healthz", methods=["GET"])
    def healthz() -> tuple[Response, int]:
        model_loaded = current_app.config["MODEL"] is not None
        status_code = 200 if model_loaded else 503
        return jsonify({"status": "ok" if model_loaded else "unavailable", "model_loaded": model_loaded}), status_code

    @app.route("/metrics", methods=["GET"])
    def metrics() -> Response:
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

    @app.route("/score", methods=["POST"])
    def score() -> tuple[Response, int]:
        booster = current_app.config["MODEL"]
        if booster is None:
            return jsonify({"error": "model not loaded"}), 503

        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"error": "request body must be a JSON object"}), 400

        reason = _validate_event(payload)
        if reason is not None:
            return jsonify({"error": reason}), 400

        threshold = current_app.config["THRESHOLD"]
        feature_columns = current_app.config["FEATURE_COLUMNS"]
        redis_client = current_app.config["REDIS_CLIENT"]

        start = time.perf_counter()
        vector, cold_card = _build_feature_vector(payload, feature_columns, redis_client)
        dmatrix = xgb.DMatrix([vector], feature_names=feature_columns)
        fraud_probability = float(booster.predict(dmatrix)[0])
        elapsed = time.perf_counter() - start
        SCORE_LATENCY.observe(elapsed)
        latency_ms = elapsed * 1000.0

        decision = "review" if fraud_probability >= threshold else "approve"

        return jsonify(
            {
                "fraud_probability": fraud_probability,
                "decision": decision,
                "threshold": threshold,
                "cold_card": cold_card,
                "latency_ms": latency_ms,
            }
        ), 200

    return app


app = create_app()
