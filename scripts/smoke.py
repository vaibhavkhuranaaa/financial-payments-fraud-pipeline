"""Assertion-driven local product smoke test."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.dashboard.app import create_app


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, default=Path("artifacts"))
    args = parser.parse_args()
    app = create_app(args.artifacts)
    client = app.server.test_client()

    health = client.get("/healthz")
    assert health.status_code == 200, health.get_json()
    summary = client.get("/api/summary").get_json()
    assert summary["review_count"] <= summary["capacity_limit"]
    assert summary["rows"] > 0
    queue_response = client.get("/api/queue?limit=3")
    assert queue_response.status_code == 200
    queue = queue_response.get_json()["transactions"]
    if queue:
        detail = client.get(f"/api/transactions/{queue[0]['source_row_id']}")
        assert detail.status_code == 200
        assert detail.get_json()["signals"]
    page = client.get("/")
    assert page.status_code == 200
    print(
        json.dumps(
            {"status": "passed", "summary": summary, "sampled_queue_rows": len(queue)},
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
