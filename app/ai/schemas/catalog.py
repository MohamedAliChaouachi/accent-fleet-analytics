"""
Schema catalog — the *whitelist* of tables/views the LLM is allowed to
reference, plus enough metadata for the prompt builder to teach the model
about their shape.

Why hand-curated and not introspected from the live DB?

1. Security. Introspecting `information_schema` would leak every table to
   the LLM, including internal staging tables, audit tables, and the auth
   schema. The catalog is the boundary: anything not listed here is
   rejected by :mod:`app.ai.security.sql_guard`.

2. Quality. Marts/views are pre-aggregated and named for analytics
   consumers; raw `staging.*` tables are not. Forcing the LLM through the
   catalog steers it toward the queries that actually answer the
   question on a single small table rather than reconstructing it from
   ten joins.

3. Stability. Marts contracts are stable; raw tables churn. A prompt
   built from the live schema would drift every time we add a column.

If you add a new analytics view, register it here. Set ``tenant_scoped``
based on whether the view has a ``tenant_id`` column — that flag drives
the SQL guard's automatic tenant-filter enforcement.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str  # postgres type, free text — only shown in the prompt
    description: str = ""


@dataclass(frozen=True)
class TableSpec:
    """One catalog entry.

    ``fqname`` is the fully qualified ``schema.table`` (or ``schema.view``)
    name. The guard matches against this — partially qualified names from
    the LLM (e.g. bare ``v_executive_dashboard``) are rejected before
    reaching execution, which makes accidental cross-schema lookups
    impossible.
    """

    fqname: str
    description: str
    grain: str  # e.g. "tenant × year_month" — narrative, for the prompt
    tenant_scoped: bool
    columns: tuple[ColumnSpec, ...] = field(default_factory=tuple)

    @property
    def schema(self) -> str:
        return self.fqname.split(".", 1)[0]

    @property
    def name(self) -> str:
        return self.fqname.split(".", 1)[1]

    def column_names(self) -> set[str]:
        return {c.name for c in self.columns}


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------
# Mirrors app/schemas/dashboards.py, which is the authoritative spec the
# /v1/dashboards/* endpoints already serve to the React client. Keep these
# in sync when adding columns to either side.

_EXECUTIVE = TableSpec(
    fqname="marts.v_executive_dashboard",
    description=(
        "Monthly executive KPIs per tenant — operating cost, total trips, "
        "distance, alerts, and month-over-month deltas. The standard "
        "answer surface for fleet-wide volume and cost questions."
    ),
    grain="one row per tenant × year_month",
    tenant_scoped=True,
    columns=(
        ColumnSpec("tenant_id", "int"),
        ColumnSpec("year_month", "text", "Calendar month as 'YYYY-MM'."),
        ColumnSpec("active_vehicles", "int"),
        ColumnSpec("active_devices", "int"),
        ColumnSpec("total_trips", "int"),
        ColumnSpec("total_distance_km", "numeric"),
        ColumnSpec("total_maintenance_cost", "numeric"),
        ColumnSpec("total_fuel_cost", "numeric"),
        ColumnSpec("total_operating_cost", "numeric"),
        ColumnSpec("cost_per_km", "numeric"),
        ColumnSpec("total_alerts", "int"),
        ColumnSpec("panic_alerts", "int"),
        ColumnSpec("total_overspeed", "int"),
        ColumnSpec("total_harsh_events", "int"),
        ColumnSpec("distance_km_mom_delta", "numeric", "Month-over-month % change."),
        ColumnSpec("operating_cost_mom_delta", "numeric"),
        ColumnSpec("alerts_mom_delta", "numeric"),
        ColumnSpec("distance_km_3mo_avg", "numeric"),
        ColumnSpec("cost_per_km_3mo_avg", "numeric"),
    ),
)

_OPERATIONS = TableSpec(
    fqname="marts.v_operational_dashboard",
    description=(
        "Daily operational metrics per tenant — trip counts, alerts by "
        "subtype, harsh-event rates, rolling 7-day averages. Use for "
        "questions about activity by day, alert mix, or driver-behaviour "
        "event rates."
    ),
    grain="one row per tenant × fleet_date",
    tenant_scoped=True,
    columns=(
        ColumnSpec("tenant_id", "int"),
        ColumnSpec("fleet_date", "date"),
        ColumnSpec("active_devices", "int"),
        ColumnSpec("total_trips", "int"),
        ColumnSpec("total_distance_km", "numeric"),
        ColumnSpec("total_driving_hours", "numeric"),
        ColumnSpec("total_stops", "int"),
        ColumnSpec("total_alerts", "int"),
        ColumnSpec("speed_alerts", "int"),
        ColumnSpec("geofence_alerts", "int"),
        ColumnSpec("panic_alerts", "int"),
        ColumnSpec("maintenance_alerts", "int"),
        ColumnSpec("other_alerts", "int"),
        ColumnSpec("overspeed_events", "int"),
        ColumnSpec("harsh_brake_events", "int"),
        ColumnSpec("harsh_accel_events", "int"),
        ColumnSpec("harsh_corner_events", "int"),
        ColumnSpec("total_harsh_events", "int"),
        ColumnSpec("alerts_per_100km", "numeric"),
        ColumnSpec("overspeed_per_100km", "numeric"),
        ColumnSpec("harsh_events_per_100km", "numeric"),
        ColumnSpec("panic_share_pct", "numeric"),
        ColumnSpec("distance_km_7d_avg", "numeric"),
        ColumnSpec("alerts_7d_avg", "numeric"),
    ),
)

_MAINTENANCE = TableSpec(
    fqname="marts.v_maintenance_dashboard",
    description=(
        "Monthly maintenance + fuel costs per vehicle. Use for questions "
        "about top-cost vehicles, fuel efficiency, cost-per-km, or "
        "repair-hour outliers. `cost_rank_in_tenant` is precomputed so "
        "'top N most expensive vehicles' is a single ORDER BY filter."
    ),
    grain="one row per tenant × vehicle × year_month",
    tenant_scoped=True,
    columns=(
        ColumnSpec("tenant_id", "int"),
        ColumnSpec("vehicle_id", "int"),
        ColumnSpec("matricule", "text", "License plate."),
        ColumnSpec("vehicle_mark", "text", "Make/brand."),
        ColumnSpec("vehicle_class", "text"),
        ColumnSpec("year_month", "text"),
        ColumnSpec("total_distance_km", "numeric"),
        ColumnSpec("active_days", "int"),
        ColumnSpec("maintenance_events", "int"),
        ColumnSpec("offense_events", "int"),
        ColumnSpec("sinistre_events", "int"),
        ColumnSpec("reparation_events", "int"),
        ColumnSpec("maintenance_cost_total", "numeric"),
        ColumnSpec("maintenance_labor_total", "numeric"),
        ColumnSpec("reparation_amount_total", "numeric"),
        ColumnSpec("fuel_cost_total", "numeric"),
        ColumnSpec("total_cost", "numeric"),
        ColumnSpec("maintenance_share_pct", "numeric"),
        ColumnSpec("cost_per_km", "numeric"),
        ColumnSpec("fuel_l_per_100km", "numeric"),
        ColumnSpec("avg_repair_hours", "numeric"),
        ColumnSpec("max_repair_hours", "numeric"),
        ColumnSpec(
            "cost_rank_in_tenant",
            "int",
            "1 = most expensive vehicle in the tenant for that month.",
        ),
    ),
)

_FLEET_RISK = TableSpec(
    fqname="marts.v_fleet_risk_dashboard",
    description=(
        "Fleet-level risk roll-up per tenant — count of critical / high / "
        "moderate / low devices and aggregate risk drivers."
    ),
    grain="one row per tenant",
    tenant_scoped=True,
    columns=(
        ColumnSpec("tenant_id", "int"),
        ColumnSpec("tenant_label", "text"),
        ColumnSpec("devices_scored", "int"),
        ColumnSpec("critical_devices", "int"),
        ColumnSpec("high_devices", "int"),
        ColumnSpec("moderate_devices", "int"),
        ColumnSpec("low_devices", "int"),
        ColumnSpec("avg_risk_score", "numeric"),
        ColumnSpec("max_risk_score", "numeric"),
        ColumnSpec("fleet_overspeed_3m", "numeric"),
        ColumnSpec("fleet_distance_3m", "numeric"),
        ColumnSpec("fleet_overspeed_per_100km", "numeric"),
        ColumnSpec("fleet_highest_avg_max_speed", "numeric"),
        ColumnSpec("fleet_avg_night_ratio", "numeric"),
    ),
)

_DEVICE_RISK = TableSpec(
    fqname="marts.v_device_risk_profile",
    description=(
        "Per-device rolling 3-month risk profile. `risk_category` is one "
        "of 'low' | 'moderate' | 'high' | 'critical'. Use for individual "
        "driver/device deep-dives and top-N risk lists."
    ),
    grain="one row per device",
    tenant_scoped=True,
    columns=(
        ColumnSpec("tenant_id", "int"),
        ColumnSpec("device_id", "int"),
        ColumnSpec("latest_month", "text", "Latest month with data, 'YYYY-MM'."),
        ColumnSpec("trips_3m", "int"),
        ColumnSpec("distance_3m", "numeric"),
        ColumnSpec("overspeed_3m", "int"),
        ColumnSpec("severe_overspeed_3m", "int"),
        ColumnSpec("alerts_3m", "int"),
        ColumnSpec("risk_score", "numeric"),
        ColumnSpec("risk_category", "text"),
    ),
)

_DEVICE_MONTHLY = TableSpec(
    fqname="marts.mart_device_monthly_behavior",
    description=(
        "Per-device monthly behaviour fact — descriptive driving stats. "
        "Use for individual-vehicle historical trends."
    ),
    grain="one row per device × year_month",
    tenant_scoped=True,
    columns=(
        ColumnSpec("tenant_id", "int"),
        ColumnSpec("device_id", "int"),
        ColumnSpec("year_month", "date", "First-of-month date."),
        ColumnSpec("total_trips", "int"),
        ColumnSpec("total_distance_km", "numeric"),
        ColumnSpec("overspeed_count", "int"),
        ColumnSpec("overspeed_per_100km", "numeric"),
        ColumnSpec("high_speed_trip_ratio", "numeric"),
        ColumnSpec("night_trip_ratio", "numeric"),
        ColumnSpec("avg_max_speed_kmh", "numeric"),
    ),
)

_CLUSTER_ASSIGNMENT = TableSpec(
    fqname="marts.fact_device_cluster_assignment",
    description=(
        "K-means cluster assignment per device × month, with the risk "
        "score that drove the cluster pick. Use for behaviour-segment "
        "questions ('which cluster is fastest?')."
    ),
    grain="one row per device × year_month × model_version",
    tenant_scoped=True,
    columns=(
        ColumnSpec("tenant_id", "int"),
        ColumnSpec("device_id", "int"),
        ColumnSpec("year_month", "date"),
        ColumnSpec("cluster_id", "int"),
        ColumnSpec("distance_to_centroid", "numeric"),
        ColumnSpec("model_version", "text"),
        ColumnSpec("risk_score", "numeric"),
        ColumnSpec("risk_category", "text"),
    ),
)

CATALOG: dict[str, TableSpec] = {
    t.fqname: t
    for t in (
        _EXECUTIVE,
        _OPERATIONS,
        _MAINTENANCE,
        _FLEET_RISK,
        _DEVICE_RISK,
        _DEVICE_MONTHLY,
        _CLUSTER_ASSIGNMENT,
    )
}


def allowed_fqnames() -> set[str]:
    """Set of fully qualified names the SQL guard will admit."""
    return set(CATALOG.keys())


def tenant_scoped_fqnames() -> set[str]:
    """Subset of CATALOG that requires a `tenant_id = :tenant_id` predicate."""
    return {fq for fq, t in CATALOG.items() if t.tenant_scoped}
