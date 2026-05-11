"""
Smoke test for the FastAPI scoring service.

Uses FastAPI's TestClient — no Postgres, no MLflow, no docker required.
Liveness probe must always return 200.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_root_returns_service_info():
    with TestClient(app) as c:
        r = c.get("/")
    assert r.status_code == 200
    body = r.json()
    assert body["service"] == "accent-fleet-api"
    assert "version" in body
    assert body["docs"] == "/docs"


def test_health_is_always_200():
    with TestClient(app) as c:
        r = c.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_ready_returns_payload_even_when_deps_down():
    """/ready must always respond (200) — it reports degraded state, not crashes."""
    with TestClient(app) as c:
        r = c.get("/ready")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] in {"ok", "degraded"}
    assert "checks" in body
    assert {"db", "mlflow"} <= set(body["checks"].keys())
