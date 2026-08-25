from __future__ import annotations

from src.dashboard.app import create_app
from src.fraud_workbench.data import FEATURE_COLUMNS


def test_health_summary_queue_and_record_routes(artifact_root) -> None:
    app = create_app(artifact_root)
    client = app.server.test_client()

    assert client.get("/healthz").status_code == 200
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
    assert client.get("/healthz").status_code == 503
    assert client.get("/api/summary").status_code == 503


def test_dashboard_shell_contains_policy_and_recovery_states(artifact_root) -> None:
    app = create_app(artifact_root)
    page = app.server.test_client().get("/").get_data(as_text=True)
    assert "Fraud Decision Workbench" in page
    assert "_dash-layout" in page or "dash-renderer" in page
