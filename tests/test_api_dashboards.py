"""
Tests for /v1/dashboards/{executive,operations,maintenance,risk}.

The endpoints read from `marts.v_executive_dashboard`,
`v_operational_dashboard`, `v_maintenance_dashboard`,
`v_fleet_risk_dashboard`, `v_device_risk_profile`, and
`fact_device_cluster_assignment`. Tests auto-skip when Postgres is
unreachable so pure-unit runs (no env, no DB) still pass — same pattern
as tests/test_api_devices.py.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from accent_fleet.db.engine import get_engine
from app.deps import get_db
from app.main import app


@pytest.fixture(autouse=True)
def _advisory_auth(monkeypatch):
    """Pin AUTH_ENFORCEMENT=advisory so the suite exercises route logic
    rather than the auth middleware (which has its own tests). Without
    this, an `.env` file with `AUTH_ENFORCEMENT=enforce` makes every
    request 401 — same gotcha test_auth_middleware.py works around."""
    from accent_fleet.config import settings as settings_fn

    monkeypatch.setenv("AUTH_ENFORCEMENT", "advisory")
    settings_fn.cache_clear()
    yield
    settings_fn.cache_clear()


@pytest.fixture(scope="module")
def db_available() -> bool:
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


@pytest.fixture
def client_no_db() -> TestClient:
    """TestClient with `get_db` overridden so query-validation tests don't open a connection."""

    def _no_db():
        yield None

    app.dependency_overrides[get_db] = _no_db
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.pop(get_db, None)


# ---------------------------------------------------------------------------
# Response shapes (DB required)
# ---------------------------------------------------------------------------
def test_executive_shape(client, db_available):
    if not db_available:
        pytest.skip("postgres not reachable")
    r = client.get("/v1/dashboards/executive")
    assert r.status_code == 200, r.text
    body = r.json()
    assert {"rows", "monthly", "kpi"} <= body.keys()
    assert isinstance(body["rows"], list)
    assert isinstance(body["monthly"], list)
    # KPI is either null (no rows) or a dict with year_month.
    assert body["kpi"] is None or "year_month" in body["kpi"]


def test_executive_monthly_aggregates_unique_year_months(client, db_available):
    """Each month should appear at most once in the aggregate."""
    if not db_available:
        pytest.skip("postgres not reachable")
    r = client.get("/v1/dashboards/executive")
    months = [m["year_month"] for m in r.json()["monthly"]]
    assert len(months) == len(set(months))


def test_operations_shape(client, db_available):
    if not db_available:
        pytest.skip("postgres not reachable")
    r = client.get("/v1/dashboards/operations")
    assert r.status_code == 200, r.text
    body = r.json()
    assert {"rows", "kpi"} <= body.keys()
    assert {"total_trips", "total_distance_km", "total_alerts", "total_overspeed"} <= body["kpi"].keys()


def test_maintenance_top_cost_vehicles_capped_at_20(client, db_available):
    if not db_available:
        pytest.skip("postgres not reachable")
    r = client.get("/v1/dashboards/maintenance")
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["top_cost_vehicles"]) <= 20
    # Top list is sorted desc by total_cost.
    costs = [row["total_cost"] for row in body["top_cost_vehicles"] if row["total_cost"] is not None]
    assert costs == sorted(costs, reverse=True)


def test_risk_shape(client, db_available):
    if not db_available:
        pytest.skip("postgres not reachable")
    r = client.get("/v1/dashboards/risk")
    assert r.status_code == 200, r.text
    body = r.json()
    assert {
        "fleet",
        "devices",
        "category_counts",
        "clusters",
        "cluster_sizes",
        "cluster_risk_crosstab",
        "cluster_model_versions",
    } <= body.keys()


def test_risk_devices_ordered_by_score_desc(client, db_available):
    if not db_available:
        pytest.skip("postgres not reachable")
    r = client.get("/v1/dashboards/risk")
    scores = [d["risk_score"] for d in r.json()["devices"] if d["risk_score"] is not None]
    assert scores == sorted(scores, reverse=True)


# ---------------------------------------------------------------------------
# Filter parameter handling (DB required)
# ---------------------------------------------------------------------------
def test_executive_respects_tenant_filter(client, db_available):
    """Every row returned must carry one of the requested tenant_ids."""
    if not db_available:
        pytest.skip("postgres not reachable")
    with get_engine().connect() as c:
        row = c.execute(
            text("SELECT tenant_id FROM marts.v_executive_dashboard WHERE tenant_id IS NOT NULL LIMIT 1")
        ).first()
    if row is None:
        pytest.skip("no rows in v_executive_dashboard")
    tenant_id = int(row[0])
    r = client.get(f"/v1/dashboards/executive?tenant_ids={tenant_id}")
    assert r.status_code == 200, r.text
    for row_ in r.json()["rows"]:
        assert row_["tenant_id"] == tenant_id


def test_operations_respects_date_window(client, db_available):
    """Date window is inclusive; rows outside it must not appear."""
    if not db_available:
        pytest.skip("postgres not reachable")
    r = client.get("/v1/dashboards/operations?start=2099-01-01&end=2099-12-31")
    assert r.status_code == 200, r.text
    assert r.json()["rows"] == []


# ---------------------------------------------------------------------------
# Parameter validation (no DB needed)
# ---------------------------------------------------------------------------
def test_invalid_date_returns_422(client_no_db):
    r = client_no_db.get("/v1/dashboards/executive?start=not-a-date")
    assert r.status_code == 422


def test_invalid_tenant_id_returns_422(client_no_db):
    r = client_no_db.get("/v1/dashboards/executive?tenant_ids=abc")
    assert r.status_code == 422
