"""
Tests for /devices/{id}/profile and /devices/top-risk.

These need a reachable database — the endpoints read from
`marts.v_device_risk_profile` and `marts.mart_device_monthly_behavior`.
Tests auto-skip when the configured Postgres is unreachable so that pure-unit
test runs (no env, no DB) still pass.

The most important check here pins the AmbiguousParameter bug fix on
/devices/top-risk: psycopg sends a typeless NULL for None, so the old
`WHERE (:tenant_id IS NULL OR tenant_id = :tenant_id)` raised 500 whenever
tenant_id was omitted. The endpoint now branches the SQL string instead.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from accent_fleet.db.engine import get_engine

from app.main import app


@pytest.fixture(scope="module")
def db_available() -> bool:
    """True if the configured Postgres is reachable. Used to skip integration tests."""
    try:
        with get_engine().connect() as c:
            c.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture(scope="module")
def client() -> TestClient:
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# /devices/top-risk
# ---------------------------------------------------------------------------
def test_top_risk_without_tenant_does_not_500(client, db_available):
    """Regression for the typeless-NULL bug: omitting tenant_id must not 500."""
    if not db_available:
        pytest.skip("postgres not reachable")
    r = client.get("/devices/top-risk?n=3")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["n"] == 3
    assert isinstance(body["devices"], list)
    assert len(body["devices"]) <= 3


def test_top_risk_with_tenant_filter_works(client, db_available):
    if not db_available:
        pytest.skip("postgres not reachable")
    r = client.get("/devices/top-risk?n=5&tenant_id=1")
    assert r.status_code == 200, r.text
    body = r.json()
    for d in body["devices"]:
        # Every returned row must match the requested tenant.
        assert d["tenant_id"] == 1


def test_top_risk_validates_n_bounds(client):
    """Query-param validation happens before DB hit — works without a DB."""
    with TestClient(app) as c:
        r = c.get("/devices/top-risk?n=0")
    assert r.status_code == 422
    with TestClient(app) as c:
        r = c.get("/devices/top-risk?n=500")
    assert r.status_code == 422


def test_top_risk_devices_are_ordered_by_risk_score(client, db_available):
    if not db_available:
        pytest.skip("postgres not reachable")
    r = client.get("/devices/top-risk?n=20")
    assert r.status_code == 200
    scores = [d["risk_score"] for d in r.json()["devices"] if d["risk_score"] is not None]
    # Non-strict descending — duplicates are fine, regressions are not.
    assert scores == sorted(scores, reverse=True)


# ---------------------------------------------------------------------------
# /devices/{id}/profile
# ---------------------------------------------------------------------------
def test_device_profile_unknown_id_returns_404(client, db_available):
    if not db_available:
        pytest.skip("postgres not reachable")
    # A device_id chosen high enough to be very unlikely to exist.
    r = client.get("/devices/999999999/profile?months=3")
    assert r.status_code == 404


def test_device_profile_validates_months(client):
    """Query-param validation — no DB needed."""
    with TestClient(app) as c:
        r = c.get("/devices/1/profile?months=0")
    assert r.status_code == 422
    with TestClient(app) as c:
        r = c.get("/devices/1/profile?months=120")
    assert r.status_code == 422


def test_device_profile_shape_for_known_device(client, db_available):
    """Resolve an actual device_id from the warehouse and verify the response shape."""
    if not db_available:
        pytest.skip("postgres not reachable")
    with get_engine().connect() as c:
        row = c.execute(
            text("SELECT device_id FROM marts.v_device_risk_profile LIMIT 1")
        ).first()
    if row is None:
        pytest.skip("no rows in v_device_risk_profile — run training/scoring first")

    device_id = int(row[0])
    r = client.get(f"/devices/{device_id}/profile?months=6")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["device_id"] == device_id
    assert "rolling" in body and "history" in body
    assert isinstance(body["history"], list)
    assert len(body["history"]) <= 6
