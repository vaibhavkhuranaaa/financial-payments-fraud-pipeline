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

Model versioning (ticket 17, v1.5b): ``MODEL_DIR`` may be either the flat
legacy layout (``model.json``/``threshold.json``/``feature_columns.json``
directly under it) or a versioned layout with a ``current.json`` pointer
file selecting one of its ``<run_id>/`` subdirectories (written by
``src/pipeline/train.py``); ``_resolve_model_dir`` picks whichever applies
and both ``/healthz`` and every ``/score`` response report the resolved
``model_version`` (``None`` under the flat legacy layout).
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

from src.pipeline.features import CARD_WINDOWS, build_feature_row, window_feature_names

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


def _resolve_model_dir(base_dir: str) -> tuple[str, str | None]:
    """Resolve the directory to actually load model/threshold/feature_columns
    from, plus the model_version to report (ticket 17, v1.5b).

    `<base_dir>/current.json` -> `{"run_id": ...}` selects a versioned run
    dir `<base_dir>/<run_id>/` written by `src.pipeline.train`. No pointer
    file, an unreadable one, or one naming a run dir that doesn't exist on
    disk all fall back to the flat `base_dir` layout with `model_version =
    None` — the pre-ticket-17 committed artifacts keep serving unchanged.
    """
    pointer_path = os.path.join(base_dir, "current.json")
    if os.path.exists(pointer_path):
        try:
            with open(pointer_path, encoding="utf-8") as f:
                run_id = json.load(f).get("run_id")
        except (OSError, json.JSONDecodeError):
            run_id = None
        if run_id:
            run_dir = os.path.join(base_dir, str(run_id))
            if os.path.isdir(run_dir):
                return run_dir, str(run_id)
            logger.warning(
                "current.json points at missing run dir %s; falling back to flat %s layout",
                run_dir,
                base_dir,
            )
    return base_dir, None


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
    ordered exactly per `feature_columns`. Returns (vector, cold_card).

    All derivation lives in the shared ``build_feature_row`` (train/serve-skew
    rule): this function only supplies zero-valued window defaults for keys the
    Redis hash doesn't have (cold/partial cards) and flattens to the model's
    column order. The hash's ``last_event_ts`` passes straight through —
    ``build_feature_row`` is the one place that turns it into
    ``time_since_last_txn_s``.
    """
    window_hash, cold_card = _fetch_window_features(redis_client, event["card_token"])

    window_features: dict[str, Any] = {
        name: 0.0 for window_key in CARD_WINDOWS for name in window_feature_names(window_key)
    }
    window_features.update(window_hash)

    row = build_feature_row(event, window_features)
    vector = [float(row[col]) for col in feature_columns]
    return vector, cold_card


_UNSET: Any = object()


def create_app(
    model: xgb.Booster | None = _UNSET,
    threshold: float | None = None,
    feature_columns: list[str] | None = None,
    redis_client: redis.Redis | None = None,
    model_version: str | None = _UNSET,
) -> Flask:
    """Flask application factory.

    All dependencies are optional overrides so tests can inject a tiny
    in-memory Booster / fake Redis client without touching disk or network;
    production (gunicorn) calls `create_app()` with no arguments and loads
    everything from `MODEL_DIR` / `SCORE_THRESHOLD_PATH` / env once at
    startup. `model` and `model_version` use a sentinel default (rather than
    `None`) so tests can pass `model=None` / `model_version=None` explicitly
    without it falling back to disk resolution.

    `model_version` (ticket 17, v1.5b): when any of model/threshold/
    feature_columns is loaded from disk, `MODEL_DIR` is resolved via
    `_resolve_model_dir` — a `<MODEL_DIR>/current.json` pointer selects a
    versioned run dir and its run_id becomes the reported model_version;
    with no pointer (or a stale one), the flat legacy `MODEL_DIR` layout is
    used and model_version is None. `SCORE_THRESHOLD_PATH` keeps working
    exactly as before when explicitly overridden away from its default
    (`<MODEL_DIR>/threshold.json`); left at the default, it follows the
    resolved dir like model.json/feature_columns.json do.
    """
    app = Flask(__name__)

    needs_disk_resolution = model is _UNSET or threshold is None or feature_columns is None
    if needs_disk_resolution:
        resolved_dir, resolved_version = _resolve_model_dir(MODEL_DIR)
    else:
        resolved_dir, resolved_version = MODEL_DIR, None

    app.config["MODEL"] = _load_model(resolved_dir) if model is _UNSET else model
    default_threshold_path = os.path.join(MODEL_DIR, "threshold.json")
    threshold_path = (
        SCORE_THRESHOLD_PATH
        if SCORE_THRESHOLD_PATH != default_threshold_path
        else os.path.join(resolved_dir, "threshold.json")
    )
    app.config["THRESHOLD"] = threshold if threshold is not None else _load_threshold(threshold_path)
    app.config["FEATURE_COLUMNS"] = (
        feature_columns if feature_columns is not None else _load_feature_columns(resolved_dir)
    )
    app.config["MODEL_VERSION"] = resolved_version if model_version is _UNSET else model_version
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
        return jsonify(
            {
                "status": "ok" if model_loaded else "unavailable",
                "model_loaded": model_loaded,
                "model_version": current_app.config["MODEL_VERSION"],
            }
        ), status_code

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
                "model_version": current_app.config["MODEL_VERSION"],
            }
        ), 200

    return app


app = create_app()
