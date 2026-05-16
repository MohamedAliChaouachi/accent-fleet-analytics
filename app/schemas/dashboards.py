"""
Response schemas for the per-page dashboard endpoints (/v1/dashboards/*).

Shapes are designed so the React client can render directly from the JSON
without re-aggregating: the service layer pre-computes the same monthly
roll-ups, KPI strips, and crosstabs the Streamlit pages currently build
in-page with pandas. Raw view rows are returned alongside so the React
"Detail" tables match the Streamlit "Raw table" sections 1:1.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Executive overview — marts.v_executive_dashboard
# ---------------------------------------------------------------------------


class ExecutiveMonthlyRow(BaseModel):
    """One row of v_executive_dashboard (per tenant × year_month)."""

    tenant_id: int | None = None
    year_month: str
    active_vehicles: int | None = None
    active_devices: int | None = None
    total_trips: int | None = None
    total_distance_km: float | None = None
    total_maintenance_cost: float | None = None
    total_fuel_cost: float | None = None
    total_operating_cost: float | None = None
    cost_per_km: float | None = None
    total_alerts: int | None = None
    panic_alerts: int | None = None
    total_overspeed: int | None = None
    total_harsh_events: int | None = None
    distance_km_mom_delta: float | None = None
    operating_cost_mom_delta: float | None = None
    alerts_mom_delta: float | None = None
    distance_km_3mo_avg: float | None = None
    cost_per_km_3mo_avg: float | None = None


class ExecutiveMonthlyAggregate(BaseModel):
    """One month, summed across tenants in scope. cost_per_km recomputed from totals."""

    year_month: str
    active_vehicles: int | None = None
    active_devices: int | None = None
    total_trips: int | None = None
    total_distance_km: float | None = None
    total_maintenance_cost: float | None = None
    total_fuel_cost: float | None = None
    total_operating_cost: float | None = None
    total_alerts: int | None = None
    panic_alerts: int | None = None
    total_overspeed: int | None = None
    total_harsh_events: int | None = None
    cost_per_km: float | None = None


class ExecutiveKpi(BaseModel):
    """KPI strip for the latest month in scope (matches Streamlit page header)."""

    year_month: str
    tenants_in_latest_month: int = Field(..., ge=0)
    active_devices: int | None = None
    total_trips: int | None = None
    total_distance_km: float | None = None
    cost_per_km: float | None = None


class ExecutiveDashboardResponse(BaseModel):
    rows: list[ExecutiveMonthlyRow]
    monthly: list[ExecutiveMonthlyAggregate]
    kpi: ExecutiveKpi | None = None


# ---------------------------------------------------------------------------
# Operations — marts.v_operational_dashboard
# ---------------------------------------------------------------------------


class OperationsDailyRow(BaseModel):
    """One row of v_operational_dashboard (per tenant × fleet_date)."""

    tenant_id: int | None = None
    fleet_date: str  # YYYY-MM-DD
    active_devices: int | None = None
    total_trips: int | None = None
    total_distance_km: float | None = None
    total_driving_hours: float | None = None
    total_stops: int | None = None
    total_alerts: int | None = None
    speed_alerts: int | None = None
    geofence_alerts: int | None = None
    panic_alerts: int | None = None
    maintenance_alerts: int | None = None
    other_alerts: int | None = None
    overspeed_events: int | None = None
    harsh_brake_events: int | None = None
    harsh_accel_events: int | None = None
    harsh_corner_events: int | None = None
    total_harsh_events: int | None = None
    alerts_per_100km: float | None = None
    overspeed_per_100km: float | None = None
    harsh_events_per_100km: float | None = None
    panic_share_pct: float | None = None
    distance_km_7d_avg: float | None = None
    alerts_7d_avg: float | None = None


class OperationsKpi(BaseModel):
    total_trips: int = 0
    total_distance_km: float = 0.0
    total_alerts: int = 0
    total_overspeed: int = 0


class OperationsDashboardResponse(BaseModel):
    rows: list[OperationsDailyRow]
    kpi: OperationsKpi


# ---------------------------------------------------------------------------
# Maintenance — marts.v_maintenance_dashboard
# ---------------------------------------------------------------------------


class MaintenanceRow(BaseModel):
    """One row of v_maintenance_dashboard (per tenant × vehicle × year_month)."""

    tenant_id: int | None = None
    vehicle_id: int | None = None
    matricule: str | None = None
    vehicle_mark: str | None = None
    vehicle_class: str | None = None
    year_month: str
    total_distance_km: float | None = None
    active_days: int | None = None
    maintenance_events: int | None = None
    offense_events: int | None = None
    sinistre_events: int | None = None
    reparation_events: int | None = None
    maintenance_cost_total: float | None = None
    maintenance_labor_total: float | None = None
    reparation_amount_total: float | None = None
    fuel_cost_total: float | None = None
    total_cost: float | None = None
    maintenance_share_pct: float | None = None
    cost_per_km: float | None = None
    fuel_l_per_100km: float | None = None
    avg_repair_hours: float | None = None
    max_repair_hours: float | None = None
    cost_rank_in_tenant: int | None = None


class MaintenanceKpi(BaseModel):
    maintenance_events: int = 0
    total_cost: float = 0.0


class MaintenanceDashboardResponse(BaseModel):
    rows: list[MaintenanceRow]
    kpi: MaintenanceKpi
    top_cost_vehicles: list[MaintenanceRow]


# ---------------------------------------------------------------------------
# Risk & behavior — v_fleet_risk_dashboard + v_device_risk_profile
#                   + fact_device_cluster_assignment
# ---------------------------------------------------------------------------


class FleetRiskRow(BaseModel):
    tenant_id: int | None = None
    tenant_label: str | None = None
    devices_scored: int | None = None
    critical_devices: int | None = None
    high_devices: int | None = None
    moderate_devices: int | None = None
    low_devices: int | None = None
    avg_risk_score: float | None = None
    max_risk_score: float | None = None
    fleet_overspeed_3m: float | None = None
    fleet_distance_3m: float | None = None
    fleet_overspeed_per_100km: float | None = None
    fleet_highest_avg_max_speed: float | None = None
    fleet_avg_night_ratio: float | None = None


class DeviceRiskRow(BaseModel):
    tenant_id: int | None = None
    device_id: int
    latest_month: str | None = None
    trips_3m: int | None = None
    distance_3m: float | None = None
    overspeed_3m: int | None = None
    severe_overspeed_3m: int | None = None
    alerts_3m: int | None = None
    risk_score: float | None = None
    risk_category: str | None = None


class ClusterAssignmentRow(BaseModel):
    tenant_id: int | None = None
    device_id: int
    year_month: str | None = None
    cluster_id: int
    distance_to_centroid: float | None = None
    model_version: str | None = None
    risk_score: float | None = None
    risk_category: str | None = None


class RiskCategoryCount(BaseModel):
    category: str
    device_count: int


class ClusterRiskCrossRow(BaseModel):
    cluster_id: int
    risk_category: str
    devices: int


class ClusterSize(BaseModel):
    cluster_id: int
    devices: int


class RiskDashboardResponse(BaseModel):
    fleet: list[FleetRiskRow]
    devices: list[DeviceRiskRow]
    category_counts: list[RiskCategoryCount]
    clusters: list[ClusterAssignmentRow]
    cluster_sizes: list[ClusterSize]
    cluster_risk_crosstab: list[ClusterRiskCrossRow]
    cluster_model_versions: list[str]
