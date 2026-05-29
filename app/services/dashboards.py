"""
Service layer for the /v1/dashboards/* endpoints.

Wraps the marts views (``marts.v_executive_dashboard``,
``v_operational_dashboard``, ``v_maintenance_dashboard``,
``v_fleet_risk_dashboard``, ``v_device_risk_profile``,
``fact_device_cluster_assignment``) and the per-month / per-category
aggregations the React pages render. The client gets one chart-ready JSON
per page and never has to talk to the DB.

:class:`DashboardFilters` owns the SQL-fragment helpers (start/end window,
tenant scoping) so every endpoint applies the filters identically.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from app.schemas.dashboards import (
    AlertCount,
    ClusterAssignmentRow,
    ClusterRiskCrossRow,
    ClusterSize,
    DeviceRiskRow,
    ExecutiveDashboardResponse,
    ExecutiveKpi,
    ExecutiveMonthlyAggregate,
    ExecutiveMonthlyRow,
    FleetEfficiencyDashboardResponse,
    FleetEfficiencyKpi,
    FleetEfficiencyMonthly,
    FleetEfficiencyRow,
    FleetRiskRow,
    LiveAlertRow,
    LiveAlertsSummary,
    MaintenanceDashboardResponse,
    MaintenanceKpi,
    MaintenanceRow,
    OperationsDailyRow,
    OperationsDashboardResponse,
    OperationsKpi,
    PredictiveAlertRow,
    PredictiveAlertsDashboardResponse,
    PredictiveAlertsSummary,
    RiskCategoryCount,
    RiskDashboardResponse,
    SafetyScorecardDashboardResponse,
    SafetyScorecardKpi,
    SafetyScorecardMonthly,
    SafetyScorecardRow,
    TenantBillingDashboardResponse,
    TenantBillingKpi,
    TenantBillingMonthly,
    TenantBillingRow,
    TenantBillingTier,
)

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

DEFAULT_WINDOW_DAYS = 90


@dataclass(frozen=True)
class DashboardFilters:
    """Resolved query filters shared by every dashboard endpoint.

    Empty ``tenant_ids`` means "all tenants".
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
    """Apply defaults: last 90 days, all tenants."""
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
        # total_devices: provisioned count from dim_device, scoped to the
        # same tenants as the rest of the KPIs. Distinct from active_devices
        # which is "moved this month". RLS / SET ROLE on the connection has
        # already pinned visibility; the tenant_clause here is the optional
        # explicit filter from the URL params.
        total_devices_sql = (
            "SELECT COUNT(DISTINCT device_id) "
            "FROM warehouse.dim_device WHERE 1=1 " + f.tenant_clause()
        )
        total_devices_row = conn.execute(text(total_devices_sql), f.params()).scalar()
        total_devices = int(total_devices_row) if total_devices_row is not None else None

        kpi = ExecutiveKpi(
            year_month=latest.year_month,
            tenants_in_latest_month=len(tenants_in_month[latest.year_month]),
            active_devices=latest.active_devices,
            total_devices=total_devices,
            total_trips=latest.total_trips,
            total_distance_km=latest.total_distance_km,
            cost_per_km=latest.cost_per_km,
            total_fuel_cost=latest.total_fuel_cost,
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
    # color-map). Skip None categories.
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


# ---------------------------------------------------------------------------
# Fleet efficiency
# ---------------------------------------------------------------------------
#
# The per-tenant rows from v_fleet_efficiency_dashboard already carry ratios
# (cost_per_km, fuel_litres_per_100km, utilization_rate_pct, idle_time_pct).
# When we aggregate to a fleet roll-up across tenants those ratios cannot be
# summed — we recompute them from the additive totals. Utilization/idle are
# device-weighted across tenants because they're already averages.

_ADDITIVE_FLEET_EFF_COLS = (
    "active_devices",
    "active_vehicles",
    "total_trips",
    "total_distance_km",
    "total_driving_hours",
    "total_operating_cost",
    "total_fuel_cost",
    "total_maintenance_cost",
    "total_fuel_litres",
)


def _pct_delta(current: float | None, prior: float | None) -> float | None:
    if current is None or prior is None or prior == 0:
        return None
    return (current - prior) / prior * 100


def _last_day_of_ym(year_month: str) -> date:
    """Return the calendar last day of a 'YYYY-MM' string.

    Used to decide whether a month-grain row has *complete* coverage in the
    underlying daily facts. A month is complete when its last calendar day
    is on or before the latest observed fact date.
    """
    y, m = year_month.split("-")
    first_next = date(int(y) + (1 if m == "12" else 0), 1 if m == "12" else int(m) + 1, 1)
    return first_next - timedelta(days=1)


def _filter_complete_months(
    conn: Connection,
    monthly_year_months: list[str],
    f: DashboardFilters,
) -> list[str]:
    """Drop partial-month entries so MoM comparisons aren't misleading.

    Background: dashboards aggregate to (tenant, year_month). When the
    underlying daily facts (fact_trip etc.) only cover part of the current
    month — common during demos / staging where seeded data ends mid-month —
    that month's totals look catastrophically lower than the previous one's,
    and every MoM-growth bar collapses to a deep-negative value.

    Heuristic: ask the warehouse for the latest observed ``trip_date`` under
    the active tenant filter, then keep only months whose calendar end is at
    or before that date. The KPI / MoM-chart consumer should fall back to
    the full list if literally nothing is complete (e.g. a brand-new tenant
    with one partial month of data).
    """
    if not monthly_year_months:
        return []
    sql = "SELECT MAX(trip_date) FROM warehouse.fact_trip WHERE 1=1 " + f.tenant_clause()
    latest_trip_date = conn.execute(text(sql), f.params()).scalar()
    if latest_trip_date is None:
        return list(monthly_year_months)
    return [ym for ym in monthly_year_months if _last_day_of_ym(ym) <= latest_trip_date]


def fetch_fleet_efficiency(
    conn: Connection, f: DashboardFilters
) -> FleetEfficiencyDashboardResponse:
    sql = f"""
        SELECT *
          FROM marts.v_fleet_efficiency_dashboard
         WHERE 1=1
           {f.tenant_clause()}
           {f.month_clause('year_month')}
         ORDER BY year_month, tenant_id
    """
    raw = conn.execute(text(sql), f.params()).mappings().all()
    rows = [FleetEfficiencyRow(**dict(r)) for r in raw]

    # Aggregate to fleet × month.
    by_month: dict[str, dict[str, float]] = defaultdict(
        lambda: dict.fromkeys(_ADDITIVE_FLEET_EFF_COLS, 0.0)
    )
    # Device-weighted utilization/idle: numerator = sum(ratio × devices),
    # denominator = sum(devices). Avoids the "quiet tenant drags fleet
    # mean down" problem you'd get by averaging tenant-level ratios.
    weighted_util_num: dict[str, float] = defaultdict(float)
    weighted_idle_num: dict[str, float] = defaultdict(float)
    weighted_den: dict[str, float] = defaultdict(float)

    for r in rows:
        bucket = by_month[r.year_month]
        for c in _ADDITIVE_FLEET_EFF_COLS:
            v = getattr(r, c)
            if v is not None:
                bucket[c] += v
        devices = r.active_devices or 0
        if r.utilization_rate_pct is not None and devices > 0:
            weighted_util_num[r.year_month] += r.utilization_rate_pct * devices
            weighted_den[r.year_month] += devices
        if r.idle_time_pct is not None and devices > 0:
            weighted_idle_num[r.year_month] += r.idle_time_pct * devices

    monthly: list[FleetEfficiencyMonthly] = []
    for ym in sorted(by_month):
        b = by_month[ym]
        dist = b["total_distance_km"]
        trips = b["total_trips"]
        cost_per_km = (b["total_operating_cost"] / dist) if dist else 0.0
        cost_per_trip = (b["total_operating_cost"] / trips) if trips else 0.0
        fuel_per_100 = (b["total_fuel_litres"] / dist * 100) if dist else 0.0
        fuel_cost_100 = (b["total_fuel_cost"] / dist * 100) if dist else 0.0
        den = weighted_den[ym]
        util = (weighted_util_num[ym] / den) if den else 0.0
        idle = (weighted_idle_num[ym] / den) if den else 0.0
        monthly.append(
            FleetEfficiencyMonthly(
                year_month=ym,
                active_devices=int(b["active_devices"]),
                active_vehicles=int(b["active_vehicles"]),
                total_trips=int(b["total_trips"]),
                total_distance_km=b["total_distance_km"],
                total_driving_hours=b["total_driving_hours"],
                total_operating_cost=b["total_operating_cost"],
                total_fuel_cost=b["total_fuel_cost"],
                total_maintenance_cost=b["total_maintenance_cost"],
                total_fuel_litres=b["total_fuel_litres"],
                cost_per_km=cost_per_km,
                cost_per_trip=cost_per_trip,
                fuel_litres_per_100km=fuel_per_100,
                fuel_cost_per_100km=fuel_cost_100,
                utilization_rate_pct=util,
                idle_time_pct=idle,
            )
        )

    kpi: FleetEfficiencyKpi | None = None
    best_worst: list[FleetEfficiencyRow] = []
    if monthly:
        latest = monthly[-1]
        prior = monthly[-2] if len(monthly) > 1 else None
        kpi = FleetEfficiencyKpi(
            year_month=latest.year_month,
            cost_per_km=latest.cost_per_km,
            cost_per_km_delta_pct=_pct_delta(
                latest.cost_per_km, prior.cost_per_km if prior else None
            ),
            utilization_rate_pct=latest.utilization_rate_pct,
            utilization_rate_delta_pct=_pct_delta(
                latest.utilization_rate_pct,
                prior.utilization_rate_pct if prior else None,
            ),
            fuel_litres_per_100km=latest.fuel_litres_per_100km,
            fuel_litres_per_100km_delta_pct=_pct_delta(
                latest.fuel_litres_per_100km,
                prior.fuel_litres_per_100km if prior else None,
            ),
            idle_time_pct=latest.idle_time_pct,
            idle_time_pct_delta_pct=_pct_delta(
                latest.idle_time_pct, prior.idle_time_pct if prior else None
            ),
            active_devices=latest.active_devices,
            total_trips=latest.total_trips,
        )
        # Per-tenant rows in the latest month, sorted by cost_per_km ascending.
        best_worst = sorted(
            [r for r in rows if r.year_month == latest.year_month],
            key=lambda r: (r.cost_per_km if r.cost_per_km is not None else float("inf")),
        )

    return FleetEfficiencyDashboardResponse(
        rows=rows, monthly=monthly, kpi=kpi, best_worst_latest=best_worst
    )


# ---------------------------------------------------------------------------
# Safety scorecard
# ---------------------------------------------------------------------------

_ADDITIVE_SAFETY_COLS = (
    "active_devices",
    "total_distance_km",
    "total_trips",
    "total_overspeed",
    "total_harsh_events",
    "harsh_brake_count",
    "harsh_accel_count",
    "harsh_corner_count",
    "scored_devices",
    "high_risk_devices",
    "critical_risk_devices",
    "high_or_critical_devices",
)


def _abs_delta(current: float | None, prior: float | None) -> float | None:
    if current is None or prior is None:
        return None
    return current - prior


def fetch_safety_scorecard(
    conn: Connection, f: DashboardFilters
) -> SafetyScorecardDashboardResponse:
    sql = f"""
        SELECT *
          FROM marts.v_safety_scorecard_dashboard
         WHERE 1=1
           {f.tenant_clause()}
           {f.month_clause('year_month')}
         ORDER BY year_month, tenant_id
    """
    raw = conn.execute(text(sql), f.params()).mappings().all()
    rows = [SafetyScorecardRow(**dict(r)) for r in raw]

    by_month: dict[str, dict[str, float]] = defaultdict(
        lambda: dict.fromkeys(_ADDITIVE_SAFETY_COLS, 0.0)
    )
    for r in rows:
        b = by_month[r.year_month]
        for c in _ADDITIVE_SAFETY_COLS:
            v = getattr(r, c)
            if v is not None:
                b[c] += v

    monthly: list[SafetyScorecardMonthly] = []
    for ym in sorted(by_month):
        b = by_month[ym]
        dist = b["total_distance_km"]
        os_rate = (b["total_overspeed"] / dist * 1000) if dist else 0.0
        harsh_rate = (b["total_harsh_events"] / dist * 1000) if dist else 0.0
        # Composite safety_score formula mirrors v_safety_scorecard_dashboard
        # exactly. Weights 0.4 (overspeed) and 0.1 (harsh) are calibrated for
        # the observed per-1000km distributions in this fleet so scores land
        # in the 40–90 band; an older draft used 2× / 4× which made the
        # fleet aggregate (~200 harsh/1000km) blow past the 100-point budget
        # and clamp every score to 0. Re-tune in lockstep with the SQL view
        # if the underlying event generator is replaced with a real feed.
        safety_score = max(0.0, min(100.0, 100.0 - 0.4 * os_rate - 0.1 * harsh_rate))
        scored = b["scored_devices"]
        hr_pct = (b["high_or_critical_devices"] / scored * 100) if scored else 0.0
        monthly.append(
            SafetyScorecardMonthly(
                year_month=ym,
                active_devices=int(b["active_devices"]),
                total_distance_km=b["total_distance_km"],
                total_overspeed=int(b["total_overspeed"]),
                total_harsh_events=int(b["total_harsh_events"]),
                harsh_brake_count=int(b["harsh_brake_count"]),
                harsh_accel_count=int(b["harsh_accel_count"]),
                harsh_corner_count=int(b["harsh_corner_count"]),
                scored_devices=int(b["scored_devices"]),
                high_risk_devices=int(b["high_risk_devices"]),
                critical_risk_devices=int(b["critical_risk_devices"]),
                high_or_critical_devices=int(b["high_or_critical_devices"]),
                overspeed_rate_per_1000km=os_rate,
                harsh_events_per_1000km=harsh_rate,
                safety_score=safety_score,
                high_risk_device_pct=hr_pct,
            )
        )

    kpi: SafetyScorecardKpi | None = None
    if monthly:
        latest = monthly[-1]
        prior = monthly[-2] if len(monthly) > 1 else None
        kpi = SafetyScorecardKpi(
            year_month=latest.year_month,
            overspeed_rate_per_1000km=latest.overspeed_rate_per_1000km,
            overspeed_rate_delta=_abs_delta(
                latest.overspeed_rate_per_1000km,
                prior.overspeed_rate_per_1000km if prior else None,
            ),
            harsh_events_per_1000km=latest.harsh_events_per_1000km,
            harsh_events_delta=_abs_delta(
                latest.harsh_events_per_1000km,
                prior.harsh_events_per_1000km if prior else None,
            ),
            high_or_critical_devices=latest.high_or_critical_devices,
            high_or_critical_delta=(
                int(latest.high_or_critical_devices or 0)
                - int(prior.high_or_critical_devices or 0)
                if prior
                else None
            ),
            safety_score=latest.safety_score,
            safety_score_delta=_abs_delta(
                latest.safety_score, prior.safety_score if prior else None
            ),
            active_devices=latest.active_devices,
            total_distance_km=latest.total_distance_km,
        )

    return SafetyScorecardDashboardResponse(rows=rows, monthly=monthly, kpi=kpi)


# ---------------------------------------------------------------------------
# Predictive alerts (proactive + live stream in a single payload)
# ---------------------------------------------------------------------------


def _bucket_count(seq: list, key: str) -> list[AlertCount]:
    """Count occurrences of `obj.<key>` and return as a stable AlertCount list."""
    counts: dict[str, int] = defaultdict(int)
    for obj in seq:
        v = getattr(obj, key, None)
        if v is None:
            continue
        counts[str(v)] += 1
    return [
        AlertCount(key=k, count=n)
        for k, n in sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    ]


def fetch_predictive_alerts(
    conn: Connection, f: DashboardFilters
) -> PredictiveAlertsDashboardResponse:
    # Proactive alerts — no date column on this view (alerts are recent
    # by construction); tenant filter only.
    alerts_sql = f"""
        SELECT *
          FROM marts.v_predictive_alerts_dashboard
         WHERE 1=1
           {f.tenant_clause()}
         ORDER BY detected_at DESC
    """
    alerts = [
        PredictiveAlertRow(**dict(r))
        for r in conn.execute(text(alerts_sql), f.params()).mappings().all()
    ]

    # Live stream — view already filters to the last 24h.
    stream_sql = f"""
        SELECT *
          FROM marts.v_real_time_alerts_stream
         WHERE 1=1
           {f.tenant_clause()}
         ORDER BY priority_score DESC, detected_at DESC
         LIMIT 500
    """
    stream = [
        LiveAlertRow(**dict(r))
        for r in conn.execute(text(stream_sql), f.params()).mappings().all()
    ]

    # Summaries — compute "last 24h" using a tz-aware NOW so the cutoff
    # matches what the SQL view considers "recent".
    from datetime import UTC, datetime, timedelta

    cutoff = datetime.now(UTC) - timedelta(hours=24)
    last_24h = sum(1 for a in alerts if a.detected_at >= cutoff)
    high_or_crit = sum(1 for a in alerts if a.severity in ("high", "critical"))
    unique_devices = len({a.device_id for a in alerts if a.device_id is not None})

    alerts_summary = PredictiveAlertsSummary(
        total=len(alerts),
        high_or_critical=high_or_crit,
        last_24h=last_24h,
        unique_devices=unique_devices,
        by_severity=_bucket_count(alerts, "severity"),
        by_type=_bucket_count(alerts, "alert_type"),
    )

    if stream:
        priorities = [s.priority_score or 0 for s in stream]
        avg_prio = sum(priorities) / len(priorities)
    else:
        avg_prio = 0.0
    stream_summary = LiveAlertsSummary(
        total=len(stream),
        high_or_critical=sum(1 for s in stream if s.severity in ("high", "critical")),
        avg_priority=round(avg_prio, 1),
        unique_devices=len({s.device_id for s in stream if s.device_id is not None}),
        by_category=_bucket_count(stream, "alert_category"),
    )

    return PredictiveAlertsDashboardResponse(
        alerts=alerts,
        stream=stream,
        alerts_summary=alerts_summary,
        stream_summary=stream_summary,
    )


# ---------------------------------------------------------------------------
# Tenant billing
# ---------------------------------------------------------------------------


def fetch_tenant_billing(
    conn: Connection, f: DashboardFilters
) -> TenantBillingDashboardResponse:
    sql = f"""
        SELECT *
          FROM marts.v_tenant_billing_dashboard
         WHERE 1=1
           {f.tenant_clause()}
           {f.month_clause('year_month')}
         ORDER BY year_month, tenant_id
    """
    rows = [
        TenantBillingRow(**dict(r))
        for r in conn.execute(text(sql), f.params()).mappings().all()
    ]

    # Sum per-month across tenants. Each metric in TenantBillingMonthly is
    # additive (counts, GB, currency), so a straight SUM is correct.
    by_month: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "active_devices": 0.0,
            "total_trips_processed": 0.0,
            "data_volume_gb": 0.0,
            "estimated_revenue": 0.0,
        }
    )
    for r in rows:
        b = by_month[r.year_month]
        b["active_devices"] += r.active_devices or 0
        b["total_trips_processed"] += r.total_trips_processed or 0
        b["data_volume_gb"] += r.data_volume_gb or 0.0
        b["estimated_revenue"] += r.estimated_revenue or 0.0

    monthly = [
        TenantBillingMonthly(
            year_month=ym,
            active_devices=int(by_month[ym]["active_devices"]),
            total_trips_processed=int(by_month[ym]["total_trips_processed"]),
            data_volume_gb=by_month[ym]["data_volume_gb"],
            estimated_revenue=by_month[ym]["estimated_revenue"],
        )
        for ym in sorted(by_month)
    ]

    kpi: TenantBillingKpi | None = None
    tier_breakdown: list[TenantBillingTier] = []
    latest_month: str | None = None
    if monthly:
        # Pick the latest *complete* month for the KPI strip / MoM chart.
        # The full `monthly` list still flows into the time-series and raw
        # tables, but using a partial month as "current" would crash the
        # MoM-growth bars to -60 / -90 % across the board (the chart the
        # user reported as "not correctly showed up").
        all_yms = [m.year_month for m in monthly]
        complete_yms = _filter_complete_months(conn, all_yms, f)
        kpi_yms = complete_yms or all_yms  # fallback for very early datasets
        latest_ym = kpi_yms[-1]
        latest_month = latest_ym
        latest_rows = [r for r in rows if r.year_month == latest_ym]
        # Prior month must also be complete to make the delta meaningful.
        prior_ym = kpi_yms[-2] if len(kpi_yms) > 1 else None
        prior_rows = [r for r in rows if r.year_month == prior_ym] if prior_ym else []

        total_tenants = len({r.tenant_id for r in latest_rows if r.tenant_id is not None})
        prior_tenants = (
            len({r.tenant_id for r in prior_rows if r.tenant_id is not None})
            if prior_ym
            else None
        )
        total_devices = int(sum(r.active_devices or 0 for r in latest_rows))
        prior_devices = (
            int(sum(r.active_devices or 0 for r in prior_rows)) if prior_ym else None
        )
        total_revenue = float(sum(r.estimated_revenue or 0 for r in latest_rows))
        prior_revenue = (
            float(sum(r.estimated_revenue or 0 for r in prior_rows)) if prior_ym else None
        )
        total_storage = float(sum(r.data_volume_gb or 0 for r in latest_rows))
        prior_storage = (
            float(sum(r.data_volume_gb or 0 for r in prior_rows)) if prior_ym else None
        )

        kpi = TenantBillingKpi(
            year_month=latest_ym,
            total_tenants=total_tenants,
            total_tenants_delta=(
                total_tenants - prior_tenants if prior_tenants is not None else None
            ),
            total_devices=total_devices,
            total_devices_delta=(
                total_devices - prior_devices if prior_devices is not None else None
            ),
            total_revenue=total_revenue,
            total_revenue_delta_pct=_pct_delta(total_revenue, prior_revenue),
            total_storage_gb=total_storage,
            total_storage_delta_pct=_pct_delta(total_storage, prior_storage),
        )

        # Pricing tier rollup for the latest month.
        tiers: dict[int, dict[str, float]] = defaultdict(
            lambda: {"tenants": set(), "devices": 0.0, "revenue": 0.0}  # type: ignore[dict-item]
        )
        for r in latest_rows:
            if r.pricing_tier is None:
                continue
            t = tiers[r.pricing_tier]
            t["tenants"].add(r.tenant_id)  # type: ignore[union-attr]
            t["devices"] += r.active_devices or 0
            t["revenue"] += r.estimated_revenue or 0.0
        tier_breakdown = [
            TenantBillingTier(
                pricing_tier=tier,
                tenants=len(data["tenants"]),  # type: ignore[arg-type]
                devices=int(data["devices"]),
                revenue=float(data["revenue"]),
            )
            for tier, data in sorted(tiers.items())
        ]

    return TenantBillingDashboardResponse(
        rows=rows,
        monthly=monthly,
        kpi=kpi,
        tier_breakdown=tier_breakdown,
        latest_month=latest_month,
    )
