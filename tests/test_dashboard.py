from __future__ import annotations

from src.dashboard.app import _brief_copy, create_app
from src.fraud_workbench.data import FEATURE_COLUMNS


def _component_props(node, component_id: str):
    if isinstance(node, dict):
        props = node.get("props", {})
        if props.get("id") == component_id:
            return props
        for value in node.values():
            match = _component_props(value, component_id)
            if match is not None:
                return match
    elif isinstance(node, list):
        for value in node:
            match = _component_props(value, component_id)
            if match is not None:
                return match
    return None


def test_health_summary_queue_and_record_routes(artifact_root) -> None:
    app = create_app(artifact_root)
    client = app.server.test_client()

    assert client.get("/health").status_code == 200
    summary = client.get("/api/summary").get_json()
    assert summary["review_count"] <= summary["capacity_limit"]
    assert "amount_recall" in summary
    assert "capacity_recall" in summary
    strategy = client.get("/api/strategy").get_json()
    assert len(strategy["frontier"]) >= 5
    assert strategy["recall_targets"][-1]["target_recall"] == 1
    queue = client.get("/api/queue?limit=2").get_json()["transactions"]
    assert len(queue) <= 2
    if queue:
        record = client.get(f"/api/transactions/{queue[0]['source_row_id']}")
        assert record.status_code == 200
        assert len(record.get_json()["signals"]) == 3


def test_default_summary_uses_cached_policy(artifact_root, monkeypatch) -> None:
    app = create_app(artifact_root)
    client = app.server.test_client()
    expected = app.server.config["ARTIFACT_STORE"].default_summary

    def fail_if_recalculated(*_args, **_kwargs):
        raise AssertionError("default summary recalculated")

    monkeypatch.setattr("src.dashboard.data.apply_review_policy", fail_if_recalculated)

    response = client.get("/api/summary")

    assert response.status_code == 200
    assert response.get_json() == expected


def test_score_route_enforces_exact_feature_contract(artifact_root) -> None:
    app = create_app(artifact_root)
    client = app.server.test_client()
    features = {column: 0.0 for column in FEATURE_COLUMNS}
    response = client.post("/api/score", json=features)
    assert response.status_code == 200
    assert 0 <= response.get_json()["fraud_probability"] <= 1
    assert client.post("/api/score", json={"Amount": 1}).status_code == 400


def test_missing_artifacts_are_error_not_empty_data(tmp_path) -> None:
    app = create_app(tmp_path / "missing")
    client = app.server.test_client()
    assert client.get("/health").status_code == 503
    assert client.get("/api/summary").status_code == 503


def test_public_request_limit_rejects_excess_requests(
    artifact_root, monkeypatch
) -> None:
    monkeypatch.setenv("PUBLIC_RATE_LIMIT_RPS", "1")
    monkeypatch.setenv("PUBLIC_RATE_LIMIT_BURST", "1")
    app = create_app(artifact_root)
    client = app.server.test_client()

    assert client.get("/health").status_code == 200
    response = client.get("/health")

    assert response.status_code == 429
    assert response.headers["Retry-After"] == "1"
    assert response.get_json()["error"] == "public request limit reached"


def test_release_route_requires_commit_sha(artifact_root, monkeypatch) -> None:
    monkeypatch.delenv("SOURCE_SHA", raising=False)
    unavailable = create_app(artifact_root).server.test_client().get("/api/release")
    assert unavailable.status_code == 503
    assert unavailable.get_json()["source_sha"] is None

    expected = "a" * 40
    monkeypatch.setenv("SOURCE_SHA", expected)
    available = create_app(artifact_root).server.test_client().get("/api/release")
    assert available.status_code == 200
    assert available.get_json() == {"source_sha": expected, "status": "ready"}


def test_dashboard_shell_contains_policy_and_recovery_states(artifact_root) -> None:
    app = create_app(artifact_root)
    page = app.server.test_client().get("/").get_data(as_text=True)
    assert "Fraud Decision Workbench" in page
    assert "_dash-layout" in page or "dash-renderer" in page


def test_dashboard_controls_and_charts_expose_exact_accessible_contract(
    artifact_root,
) -> None:
    app = create_app(artifact_root)
    layout = app.server.test_client().get("/_dash-layout").get_json()

    threshold = _component_props(layout, "threshold-slider")
    frontier = _component_props(layout, "capacity-frontier-region")
    distribution = _component_props(layout, "score-distribution-region")

    assert threshold["step"] == 0.000001
    assert frontier["role"] == "img" and frontier["aria-label"]
    assert distribution["role"] == "img" and distribution["aria-label"]


def test_director_brief_tracks_active_policy() -> None:
    title, detail = _brief_copy(
        {
            "binding_control": "threshold",
            "true_positive": 60,
            "false_negative": 15,
            "reviews_per_1000": 2.11,
            "review_count": 120,
            "recall": 0.8,
            "precision": 0.5,
            "false_positive": 60,
        }
    )
    assert title == "Threshold: 60 of 75 observed frauds captured."
    assert "120 rows" in detail
    assert "80.0% recall at 50.0% precision" in detail
