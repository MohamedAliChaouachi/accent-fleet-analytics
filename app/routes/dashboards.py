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

Tenant scoping is split between role types:
  * superadmin  — trusts the client's ``tenant_ids`` filter as-is (or
                  treats omission as "all tenants"). They can drill into
                  any subset.
  * tenant_admin / tenant_user — the JWT's ``tenant_id`` is the source
                  of truth; whatever the client sends is ignored. RLS is
                  the final clamp, but pinning at the API layer means a
                  tenant-scoped user sees their data on first load
                  without having to know and type their own tenant_id.
"""

from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Query
from sqlalchemy.engine import Connection

from app.auth.principal import current_principal
from app.deps import DbDep
from app.schemas.dashboards import (
    ExecutiveDashboardResponse,
    FleetEfficiencyDashboardResponse,
    MaintenanceDashboardResponse,
    OperationsDashboardResponse,
    PredictiveAlertsDashboardResponse,
    RiskDashboardResponse,
    SafetyScorecardDashboardResponse,
    TenantBillingDashboardResponse,
)
from app.services.dashboards import (
    fetch_executive,
    fetch_fleet_efficiency,
    fetch_maintenance,
    fetch_operations,
    fetch_predictive_alerts,
    fetch_risk,
    fetch_safety_scorecard,
    fetch_tenant_billing,
    parse_filters,
)

# Dashboards router — one thin route per React page, all sharing the same filters.
router = APIRouter(prefix="/dashboards", tags=["dashboards"])

# Annotated aliases keep the four route signatures short and let ruff's B008
# rule see the Query(...) call outside of a default-value position. The
# rule's built-in immutable allowlist doesn't include `datetime.date`, so
# the older `start: date | None = Query(default=None)` form trips B008
# even though FastAPI is the canonical consumer of that pattern.
StartParam = Annotated[
    date | None,
    Query(description="Inclusive window start; defaults to 90 days ago."),
]
EndParam = Annotated[
    date | None,
    Query(description="Inclusive window end; defaults to today."),
]
TenantIdsParam = Annotated[
    list[int] | None,
    Query(description="Optional tenant scope; omit for all tenants."),
]


def _effective_tenant_ids(raw: list[int] | None) -> list[int]:
    """
    Resolve which tenant_ids the query is allowed to filter on.

    FastAPI passes ``[]`` when the param is omitted on a list-typed Query;
    we normalise that to a plain list either way. On top of that, we
    enforce the role contract from the module docstring:

      * No principal in scope (auth_enforcement=off, or advisory mode
        with no token) → trust the client. Dev-only path; production
        always has a principal.
      * superadmin → trust the client. They may scope to any subset of
        tenants, or omit to mean "all tenants".
      * tenant_admin / tenant_user → ignore the client. Force the scope
        to ``[principal.tenant_id]`` so the page hydrates with the
        user's data immediately. A user spoofing a different tenant_id
        in the URL is still safe because RLS clamps every query to the
        same GUC.
    """
    p = current_principal()
    if p is None or p.is_superadmin:
        return list(raw) if raw else []
    # Non-superadmin: Principal.__post_init__ guarantees tenant_id is not None
    # whenever role != 'superadmin', so this assertion is a safety net for
    # future refactors rather than a real branch.
    assert p.tenant_id is not None
    return [p.tenant_id]


# Executive overview page — KPIs, monthly aggregates, per-tenant rows.
@router.get("/executive", response_model=ExecutiveDashboardResponse)
def executive_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> ExecutiveDashboardResponse:
    f = parse_filters(start, end, _effective_tenant_ids(tenant_ids))
    return fetch_executive(conn, f)


# Operations page — daily fleet activity and alert breakdown.
@router.get("/operations", response_model=OperationsDashboardResponse)
def operations_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> OperationsDashboardResponse:
    f = parse_filters(start, end, _effective_tenant_ids(tenant_ids))
    return fetch_operations(conn, f)


# Maintenance page — per-vehicle cost rows plus top-cost vehicles.
@router.get("/maintenance", response_model=MaintenanceDashboardResponse)
def maintenance_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> MaintenanceDashboardResponse:
    f = parse_filters(start, end, _effective_tenant_ids(tenant_ids))
    return fetch_maintenance(conn, f)


# Risk & behavior page — fleet rollup, device risk, cluster overlay.
@router.get("/risk", response_model=RiskDashboardResponse)
def risk_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> RiskDashboardResponse:
    f = parse_filters(start, end, _effective_tenant_ids(tenant_ids))
    return fetch_risk(conn, f)


# Fleet efficiency page — cost/utilization ratios with MoM deltas.
@router.get("/fleet-efficiency", response_model=FleetEfficiencyDashboardResponse)
def fleet_efficiency_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> FleetEfficiencyDashboardResponse:
    f = parse_filters(start, end, _effective_tenant_ids(tenant_ids))
    return fetch_fleet_efficiency(conn, f)


# Safety scorecard page — overspeed/harsh rates and composite safety score.
@router.get("/safety-scorecard", response_model=SafetyScorecardDashboardResponse)
def safety_scorecard_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> SafetyScorecardDashboardResponse:
    f = parse_filters(start, end, _effective_tenant_ids(tenant_ids))
    return fetch_safety_scorecard(conn, f)


# Predictive alerts page — proactive alerts plus the live 24h stream.
@router.get("/predictive-alerts", response_model=PredictiveAlertsDashboardResponse)
def predictive_alerts_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> PredictiveAlertsDashboardResponse:
    f = parse_filters(start, end, _effective_tenant_ids(tenant_ids))
    return fetch_predictive_alerts(conn, f)


# Tenant billing page — per-tenant revenue, devices, and pricing tiers.
@router.get("/tenant-billing", response_model=TenantBillingDashboardResponse)
def tenant_billing_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> TenantBillingDashboardResponse:
    f = parse_filters(start, end, _effective_tenant_ids(tenant_ids))
    return fetch_tenant_billing(conn, f)
