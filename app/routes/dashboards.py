"""
Dashboard endpoints — one per page of the React client.

  GET /dashboards/executive   marts.v_executive_dashboard
  GET /dashboards/operations  marts.v_operational_dashboard
  GET /dashboards/maintenance marts.v_maintenance_dashboard
  GET /dashboards/risk        marts.v_fleet_risk_dashboard
                              + v_device_risk_profile
                              + fact_device_cluster_assignment

All endpoints take the same three filters (``start``, ``end``,
``tenant_ids``) so the React sidebar can drive every page from a single
context. The SQL composition and per-month aggregation live in
:mod:`app.services.dashboards`; the routes are thin wrappers that own
parameter parsing and response serialization.

Tenant scoping is still a query parameter for v1 to match the current
Streamlit behaviour. A follow-up will fold it into the JWT principal
once the React app is the only consumer.
"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Query
from sqlalchemy.engine import Connection

from app.deps import DbDep
from app.schemas.dashboards import (
    ExecutiveDashboardResponse,
    MaintenanceDashboardResponse,
    OperationsDashboardResponse,
    RiskDashboardResponse,
)
from app.services.dashboards import (
    fetch_executive,
    fetch_maintenance,
    fetch_operations,
    fetch_risk,
    parse_filters,
)

router = APIRouter(prefix="/dashboards", tags=["dashboards"])


def _tenant_ids(raw: list[int] | None) -> list[int]:
    """FastAPI passes [] when the param is omitted entirely with default=None
    on a list-typed Query; normalise to a plain list either way."""
    return list(raw) if raw else []


@router.get("/executive", response_model=ExecutiveDashboardResponse)
def executive_overview(
    start: date | None = Query(default=None, description="Inclusive window start; defaults to 90 days ago."),
    end: date | None = Query(default=None, description="Inclusive window end; defaults to today."),
    tenant_ids: list[int] | None = Query(default=None, description="Optional tenant scope; omit for all tenants."),
    conn: Connection = DbDep,
) -> ExecutiveDashboardResponse:
    f = parse_filters(start, end, _tenant_ids(tenant_ids))
    return fetch_executive(conn, f)


@router.get("/operations", response_model=OperationsDashboardResponse)
def operations_overview(
    start: date | None = Query(default=None),
    end: date | None = Query(default=None),
    tenant_ids: list[int] | None = Query(default=None),
    conn: Connection = DbDep,
) -> OperationsDashboardResponse:
    f = parse_filters(start, end, _tenant_ids(tenant_ids))
    return fetch_operations(conn, f)


@router.get("/maintenance", response_model=MaintenanceDashboardResponse)
def maintenance_overview(
    start: date | None = Query(default=None),
    end: date | None = Query(default=None),
    tenant_ids: list[int] | None = Query(default=None),
    conn: Connection = DbDep,
) -> MaintenanceDashboardResponse:
    f = parse_filters(start, end, _tenant_ids(tenant_ids))
    return fetch_maintenance(conn, f)


@router.get("/risk", response_model=RiskDashboardResponse)
def risk_overview(
    start: date | None = Query(default=None),
    end: date | None = Query(default=None),
    tenant_ids: list[int] | None = Query(default=None),
    conn: Connection = DbDep,
) -> RiskDashboardResponse:
    f = parse_filters(start, end, _tenant_ids(tenant_ids))
    return fetch_risk(conn, f)
