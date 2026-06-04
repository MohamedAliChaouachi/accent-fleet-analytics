"""
Dashboard endpoints with Redis caching.
"""

from __future__ import annotations

from datetime import date
from typing import Annotated
import json

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

router = APIRouter(prefix="/dashboards", tags=["dashboards"])

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
    p = current_principal()
    if p is None:
        return list(raw) if raw else []
    if p.role == "superadmin":
        return list(raw) if raw else []
    return [p.tenant_id]


async def _cache_get(key: str):
    from app.cache import get_redis
    client = get_redis()
    if client is None:
        return None
    try:
        data = await client.get(key)
        return json.loads(data) if data else None
    except:
        return None


async def _cache_set(key: str, value: dict, ttl: int = 60):
    from app.cache import get_redis
    client = get_redis()
    if client is None:
        return
    try:
        await client.setex(key, ttl, json.dumps(value))
    except:
        pass


def _make_cache_key(endpoint: str, tenant_ids: list[int]) -> str:
    tenants_str = ",".join(map(str, sorted(tenant_ids))) if tenant_ids else "all"
    return f"dashboard:{endpoint}:{tenants_str}"


@router.get("/executive", response_model=ExecutiveDashboardResponse)
async def executive_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> ExecutiveDashboardResponse:
    effective = _effective_tenant_ids(tenant_ids)
    cache_key = _make_cache_key("executive", effective)
    cached = await _cache_get(cache_key)
    if cached:
        return ExecutiveDashboardResponse(**cached)
    f = parse_filters(start, end, effective)
    result = fetch_executive(conn, f)
    await _cache_set(cache_key, result.model_dump(), ttl=60)
    return result


@router.get("/operations", response_model=OperationsDashboardResponse)
async def operations_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> OperationsDashboardResponse:
    effective = _effective_tenant_ids(tenant_ids)
    cache_key = _make_cache_key("operations", effective)
    cached = await _cache_get(cache_key)
    if cached:
        return OperationsDashboardResponse(**cached)
    f = parse_filters(start, end, effective)
    result = fetch_operations(conn, f)
    await _cache_set(cache_key, result.model_dump(), ttl=60)
    return result


@router.get("/maintenance", response_model=MaintenanceDashboardResponse)
async def maintenance_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> MaintenanceDashboardResponse:
    effective = _effective_tenant_ids(tenant_ids)
    cache_key = _make_cache_key("maintenance", effective)
    cached = await _cache_get(cache_key)
    if cached:
        return MaintenanceDashboardResponse(**cached)
    f = parse_filters(start, end, effective)
    result = fetch_maintenance(conn, f)
    await _cache_set(cache_key, result.model_dump(), ttl=60)
    return result


@router.get("/risk", response_model=RiskDashboardResponse)
async def risk_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> RiskDashboardResponse:
    effective = _effective_tenant_ids(tenant_ids)
    cache_key = _make_cache_key("risk", effective)
    cached = await _cache_get(cache_key)
    if cached:
        return RiskDashboardResponse(**cached)
    f = parse_filters(start, end, effective)
    result = fetch_risk(conn, f)
    await _cache_set(cache_key, result.model_dump(), ttl=60)
    return result


@router.get("/fleet-efficiency", response_model=FleetEfficiencyDashboardResponse)
async def fleet_efficiency_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> FleetEfficiencyDashboardResponse:
    effective = _effective_tenant_ids(tenant_ids)
    cache_key = _make_cache_key("fleet-efficiency", effective)
    cached = await _cache_get(cache_key)
    if cached:
        return FleetEfficiencyDashboardResponse(**cached)
    f = parse_filters(start, end, effective)
    result = fetch_fleet_efficiency(conn, f)
    await _cache_set(cache_key, result.model_dump(), ttl=60)
    return result


@router.get("/safety-scorecard", response_model=SafetyScorecardDashboardResponse)
async def safety_scorecard_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> SafetyScorecardDashboardResponse:
    effective = _effective_tenant_ids(tenant_ids)
    cache_key = _make_cache_key("safety-scorecard", effective)
    cached = await _cache_get(cache_key)
    if cached:
        return SafetyScorecardDashboardResponse(**cached)
    f = parse_filters(start, end, effective)
    result = fetch_safety_scorecard(conn, f)
    await _cache_set(cache_key, result.model_dump(), ttl=60)
    return result


@router.get("/predictive-alerts", response_model=PredictiveAlertsDashboardResponse)
async def predictive_alerts_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> PredictiveAlertsDashboardResponse:
    effective = _effective_tenant_ids(tenant_ids)
    cache_key = _make_cache_key("predictive-alerts", effective)
    cached = await _cache_get(cache_key)
    if cached:
        return PredictiveAlertsDashboardResponse(**cached)
    f = parse_filters(start, end, effective)
    result = fetch_predictive_alerts(conn, f)
    await _cache_set(cache_key, result.model_dump(), ttl=60)
    return result


@router.get("/tenant-billing", response_model=TenantBillingDashboardResponse)
async def tenant_billing_overview(
    start: StartParam = None,
    end: EndParam = None,
    tenant_ids: TenantIdsParam = None,
    conn: Connection = DbDep,
) -> TenantBillingDashboardResponse:
    effective = _effective_tenant_ids(tenant_ids)
    cache_key = _make_cache_key("tenant-billing", effective)
    cached = await _cache_get(cache_key)
    if cached:
        return TenantBillingDashboardResponse(**cached)
    f = parse_filters(start, end, effective)
    result = fetch_tenant_billing(conn, f)
    await _cache_set(cache_key, result.model_dump(), ttl=60)
    return result
