"""
Service layer for the /v1/dashboards/* endpoints.

Holds the SQL the Streamlit pages currently embed (against
``marts.v_executive_dashboard``, ``v_operational_dashboard``,
``v_maintenance_dashboard``, ``v_fleet_risk_dashboard``,
``v_device_risk_profile``, ``fact_device_cluster_assignment``) and the
per-month / per-category aggregations the pages compute client-side with
pandas. Pulling both into one place means the React client gets one
chart-ready JSON per page and never has to talk to the DB.

The dashboard's ``Filters`` class in ``dashboard/lib/theme.py`` still
exists; we duplicate its SQL-fragment helpers here as
:class:`DashboardFilters` rather than reaching across packages, so the
Streamlit container keeps working unchanged during the migration window.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from app.schemas.dashboards import (
    ClusterAssignmentRow,
    ClusterRiskCrossRow,
    ClusterSize,
    DeviceRiskRow,
    ExecutiveDashboardResponse,
    ExecutiveKpi,
    ExecutiveMonthlyAggregate,
    ExecutiveMonthlyRow,
    FleetRiskRow,
    MaintenanceDashboardResponse,
    MaintenanceKpi,
    MaintenanceRow,
    OperationsDailyRow,
    OperationsDashboardResponse,
    OperationsKpi,
    RiskCategoryCount,
    RiskDashboardResponse,
)

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

DEFAULT_WINDOW_DAYS = 90


@dataclass(frozen=True)
class DashboardFilters:
    """Resolved query filters shared by all four dashboard endpoints.

    Mirrors :class:`dashboard.lib.theme.Filters` exactly so SQL fragments
    can be lifted unchanged. Empty ``tenant_ids`` means "all tenants".
    """

    start: date
    end: date
    tenant_ids: tuple[int, ...] = field(default_factory=tuple)

    @property
    def start_iso(self) -> str:
        return self.start.isoformat()

    @property
    def end_iso(self) -> str:
        return self.end.isoformat()

    @property
    def start_month(self) -> str:
        return self.start.strftime("%Y-%m")

    @property
    def end_month(self) -> str:
        return self.end.strftime("%Y-%m")

    def tenant_clause(self, alias: str = "") -> str:
        if not self.tenant_ids:
            return ""
        prefix = f"{alias}." if alias else ""
        return f"AND {prefix}tenant_id = ANY(CAST(:tenant_ids AS int[]))"

    def date_clause(self, column: str) -> str:
        return f"AND {column} BETWEEN :start AND :end"

    def month_clause(self, column: str) -> str:
        return f"AND {column} BETWEEN :start_month AND :end_month"

    def params(self) -> dict[str, Any]:
        # psycopg sends typeless NULL for Python None; SQLAlchemy with
        # `ANY(CAST(:tenant_ids AS int[]))` does the right thing because
        # the cast pins the type, but only when the clause is actually
        # included. We still send None when no tenants are selected so
        # the dict shape is stable for callers.
        return {
            "start": self.start_iso,
            "end": self.end_iso,
            "start_month": self.start_month,
            "end_month": self.end_month,
            "tenant_ids": list(self.tenant_ids) if self.tenant_ids else None,
        }


def parse_filters(
    start: date | None,
    end: date | None,
    tenant_ids: list[int] | None,
) -> DashboardFilters:
    """Apply Streamlit defaults: last 90 days, all tenants."""
    today = date.today()
    if start is None:
        start = today - timedelta(days=DEFAULT_WINDOW_DAYS)
    if end is None:
        end = today
    return DashboardFilters(
        start=start,
        end=end,
        tenant_ids=tuple(tenant_ids or ()),
    )


# ---------------------------------------------------------------------------
# Executive overview
# ---------------------------------------------------------------------------

_ADDITIVE_EXEC_COLS = (
    "active_vehicles",
    "active_devices",
    "total_trips",
    "total_distance_km",
    "total_maintenance_cost",
    "total_fuel_cost",
    "total_operating_cost",
    "total_alerts",
    "panic_alerts",
    "total_overspeed",
    "total_harsh_events",
)


def fetch_executive(conn: Connection, f: DashboardFilters) -> ExecutiveDashboardResponse:
    """Per-tenant rows + per-month aggregate + latest-month KPI strip."""
    sql = f"""
        SELECT *
          FROM marts.v_executive_dashboard
         WHERE 1=1
           {f.tenant_clause()}
           {f.month_clause('year_month')}
         ORDER BY year_month, tenant_id
    """
    raw = conn.execute(text(sql), f.params()).mappings().all()
    rows = [ExecutiveMonthlyRow(**dict(r)) for r in raw]

    # Aggregate by month (sum additive columns across tenants in scope).
    # cost_per_km is a ratio so we recompute it from the totals instead
    # of summing per-tenant ratios — see the comment block in
    # dashboard/pages/0_Executive_Overview.py for the rationale.
    by_month: dict[str, dict[str, float]] = defaultdict(
        lambda: dict.fromkeys(_ADDITIVE_EXEC_COLS, 0.0)
    )
    tenants_in_month: dict[str, set[int | None]] = defaultdict(set)
    for r in rows:
        bucket = by_month[r.year_month]
        for c in _ADDITIVE_EXEC_COLS:
            v = getattr(r, c)
            if v is not None:
                bucket[c] += v
        tenants_in_month[r.year_month].add(r.tenant_id)

    monthly: list[ExecutiveMonthlyAggregate] = []
    for ym in sorted(by_month):
        b = by_month[ym]
        dist = b["total_distance_km"]
        cost_per_km = (b["total_operating_cost"] / dist) if dist else 0.0
        monthly.append(
            ExecutiveMonthlyAggregate(
                year_month=ym,
                active_vehicles=int(b["active_vehicles"]),
                active_devices=int(b["active_devices"]),
                total_trips=int(b["total_trips"]),
                total_distance_km=b["total_distance_km"],
                total_maintenance_cost=b["total_maintenance_cost"],
                total_fuel_cost=b["total_fuel_cost"],
                total_operating_cost=b["total_operating_cost"],
                total_alerts=int(b["total_alerts"]),
                panic_alerts=int(b["panic_alerts"]),
                total_overspeed=int(b["total_overspeed"]),
                total_harsh_events=int(b["total_harsh_events"]),
                cost_per_km=cost_per_km,
            )
        )

    kpi = None
    if monthly:
        latest = monthly[-1]
        kpi = ExecutiveKpi(
            year_month=latest.year_month,
            tenants_in_latest_month=len(tenants_in_month[latest.year_month]),
            active_devices=latest.active_devices,
            total_trips=latest.total_trips,
            total_distance_km=latest.total_distance_km,
            cost_per_km=latest.cost_per_km,
        )

    return ExecutiveDashboardResponse(rows=rows, monthly=monthly, kpi=kpi)


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


def fetch_operations(conn: Connection, f: DashboardFilters) -> OperationsDashboardResponse:
    sql = f"""
        SELECT *
          FROM marts.v_operational_dashboard
         WHERE 1=1
           {f.tenant_clause()}
           {f.date_clause('fleet_date')}
         ORDER BY fleet_date
    """
    raw = conn.execute(text(sql), f.params()).mappings().all()
    rows: list[OperationsDailyRow] = []
    for r in raw:
        d = dict(r)
        if isinstance(d.get("fleet_date"), date):
            d["fleet_date"] = d["fleet_date"].isoformat()
        rows.append(OperationsDailyRow(**d))

    kpi = OperationsKpi(
        total_trips=sum(r.total_trips or 0 for r in rows),
        total_distance_km=sum(r.total_distance_km or 0.0 for r in rows),
        total_alerts=sum(r.total_alerts or 0 for r in rows),
        total_overspeed=sum(r.overspeed_events or 0 for r in rows),
    )
    return OperationsDashboardResponse(rows=rows, kpi=kpi)


# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------


def fetch_maintenance(conn: Connection, f: DashboardFilters) -> MaintenanceDashboardResponse:
    sql = f"""
        SELECT *
          FROM marts.v_maintenance_dashboard
         WHERE 1=1
           {f.tenant_clause()}
           {f.month_clause('year_month')}
         ORDER BY year_month DESC, total_cost DESC NULLS LAST
    """
    raw = conn.execute(text(sql), f.params()).mappings().all()
    rows = [MaintenanceRow(**dict(r)) for r in raw]

    kpi = MaintenanceKpi(
        maintenance_events=sum(r.maintenance_events or 0 for r in rows),
        total_cost=sum(r.total_cost or 0.0 for r in rows),
    )
    # Already sorted by total_cost DESC; head 20.
    top_cost_vehicles = [r for r in rows if r.total_cost is not None][:20]

    return MaintenanceDashboardResponse(
        rows=rows,
        kpi=kpi,
        top_cost_vehicles=top_cost_vehicles,
    )


# ---------------------------------------------------------------------------
# Risk & behavior
# ---------------------------------------------------------------------------


def fetch_risk(conn: Connection, f: DashboardFilters) -> RiskDashboardResponse:
    # Fleet rollup — no date column on this view; tenant filter only.
    fleet_sql = f"""
        SELECT *
          FROM marts.v_fleet_risk_dashboard
         WHERE 1=1
           {f.tenant_clause()}
    """
    fleet_rows = conn.execute(text(fleet_sql), f.params()).mappings().all()
    fleet = [FleetRiskRow(**dict(r)) for r in fleet_rows]

    # Device-level — filter on latest_month (CHAR(7)).
    device_sql = f"""
        SELECT tenant_id, device_id, latest_month::text AS latest_month,
               trips_3m, distance_3m, overspeed_3m, severe_overspeed_3m,
               alerts_3m, risk_score, risk_category
          FROM marts.v_device_risk_profile
         WHERE 1=1
           {f.tenant_clause()}
           {f.month_clause('latest_month')}
         ORDER BY risk_score DESC NULLS LAST
    """
    device_rows = conn.execute(text(device_sql), f.params()).mappings().all()
    devices = [DeviceRiskRow(**dict(r)) for r in device_rows]

    # Risk category distribution (preserves insertion order; React can
    # color-map). Skip None categories to match the Streamlit page.
    counts: dict[str, int] = defaultdict(int)
    for d in devices:
        if d.risk_category:
            counts[d.risk_category] += 1
    category_counts = [
        RiskCategoryCount(category=cat, device_count=n) for cat, n in counts.items()
    ]

    # Cluster overlay — joins fact_device_cluster_assignment with the
    # risk view so the crosstab can ask "do clusters track risk?".
    cluster_sql = f"""
        SELECT a.tenant_id,
               a.device_id,
               a.year_month::text AS year_month,
               a.cluster_id,
               a.distance_to_centroid,
               a.model_version,
               r.risk_score,
               r.risk_category
          FROM marts.fact_device_cluster_assignment a
          LEFT JOIN marts.v_device_risk_profile r
            ON r.tenant_id = a.tenant_id
           AND r.device_id = a.device_id
         WHERE 1=1
           {f.tenant_clause('a')}
           {f.month_clause('a.year_month')}
    """
    clusters = [
        ClusterAssignmentRow(**dict(r))
        for r in conn.execute(text(cluster_sql), f.params()).mappings().all()
    ]

    # Per-cluster size + crosstab. Both empty when no rows.
    size_buckets: dict[int, int] = defaultdict(int)
    cross_buckets: dict[tuple[int, str], int] = defaultdict(int)
    versions: list[str] = []
    seen_versions: set[str] = set()
    for c in clusters:
        size_buckets[c.cluster_id] += 1
        if c.risk_category:
            cross_buckets[(c.cluster_id, c.risk_category)] += 1
        if c.model_version and c.model_version not in seen_versions:
            seen_versions.add(c.model_version)
            versions.append(c.model_version)

    cluster_sizes = [
        ClusterSize(cluster_id=cid, devices=n) for cid, n in sorted(size_buckets.items())
    ]
    cluster_risk_crosstab = [
        ClusterRiskCrossRow(cluster_id=cid, risk_category=cat, devices=n)
        for (cid, cat), n in sorted(cross_buckets.items())
    ]

    return RiskDashboardResponse(
        fleet=fleet,
        devices=devices,
        category_counts=category_counts,
        clusters=clusters,
        cluster_sizes=cluster_sizes,
        cluster_risk_crosstab=cluster_risk_crosstab,
        cluster_model_versions=versions,
    )
