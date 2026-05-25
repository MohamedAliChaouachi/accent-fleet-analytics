"""
Response schemas for the per-page dashboard endpoints (/v1/dashboards/*).

Shapes are designed so the React client can render directly from the JSON
without re-aggregating: the service layer pre-computes the same monthly
roll-ups, KPI strips, and crosstabs the Streamlit pages currently build
in-page with pandas. Raw view rows are returned alongside so the React
"Detail" tables match the Streamlit "Raw table" sections 1:1.
"""

from __future__ import annotations

from datetime import datetime

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


# ---------------------------------------------------------------------------
# Fleet efficiency — marts.v_fleet_efficiency_dashboard
# ---------------------------------------------------------------------------


class FleetEfficiencyRow(BaseModel):
    """One row of v_fleet_efficiency_dashboard (per tenant × year_month)."""

    tenant_id: int | None = None
    year_month: str
    active_devices: int | None = None
    active_vehicles: int | None = None
    total_trips: int | None = None
    total_distance_km: float | None = None
    total_driving_hours: float | None = None
    total_operating_cost: float | None = None
    total_fuel_cost: float | None = None
    total_maintenance_cost: float | None = None
    cost_per_km: float | None = None
    cost_per_trip: float | None = None
    fuel_cost_per_100km: float | None = None
    maintenance_cost_per_km: float | None = None
    total_fuel_litres: float | None = None
    fuel_litres_per_100km: float | None = None
    avg_cost_per_litre: float | None = None
    utilization_rate_pct: float | None = None
    idle_time_pct: float | None = None
    trips_per_device_per_day: float | None = None
    avg_trip_duration_min: float | None = None
    avg_distance_per_device: float | None = None
    distance_trend_mom_pct: float | None = None
    cost_trend_mom_pct: float | None = None


class FleetEfficiencyMonthly(BaseModel):
    """One month, fleet-aggregate. Ratios recomputed from totals."""

    year_month: str
    active_devices: int | None = None
    active_vehicles: int | None = None
    total_trips: int | None = None
    total_distance_km: float | None = None
    total_driving_hours: float | None = None
    total_operating_cost: float | None = None
    total_fuel_cost: float | None = None
    total_maintenance_cost: float | None = None
    total_fuel_litres: float | None = None
    cost_per_km: float | None = None
    cost_per_trip: float | None = None
    fuel_litres_per_100km: float | None = None
    fuel_cost_per_100km: float | None = None
    utilization_rate_pct: float | None = None
    idle_time_pct: float | None = None


class FleetEfficiencyKpi(BaseModel):
    """Latest-month KPI strip with prior-month deltas already computed."""

    year_month: str
    cost_per_km: float | None = None
    cost_per_km_delta_pct: float | None = None
    utilization_rate_pct: float | None = None
    utilization_rate_delta_pct: float | None = None
    fuel_litres_per_100km: float | None = None
    fuel_litres_per_100km_delta_pct: float | None = None
    idle_time_pct: float | None = None
    idle_time_pct_delta_pct: float | None = None
    active_devices: int | None = None
    total_trips: int | None = None


class FleetEfficiencyDashboardResponse(BaseModel):
    rows: list[FleetEfficiencyRow]
    monthly: list[FleetEfficiencyMonthly]
    kpi: FleetEfficiencyKpi | None = None
    # latest-month per-tenant rows sorted by cost_per_km ascending — the
    # client splits into best/worst by slicing head/tail to avoid two queries.
    best_worst_latest: list[FleetEfficiencyRow] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Safety scorecard — marts.v_safety_scorecard_dashboard
# ---------------------------------------------------------------------------


class SafetyScorecardRow(BaseModel):
    """One row of v_safety_scorecard_dashboard (per tenant × year_month)."""

    tenant_id: int | None = None
    year_month: str
    active_devices: int | None = None
    total_distance_km: float | None = None
    total_trips: int | None = None
    total_overspeed: int | None = None
    total_harsh_events: int | None = None
    overspeed_rate_per_1000km: float | None = None
    overspeed_severity_score: float | None = None
    overspeed_trend_mom_pct: float | None = None
    harsh_events_per_1000km: float | None = None
    harsh_brake_count: int | None = None
    harsh_accel_count: int | None = None
    harsh_corner_count: int | None = None
    harsh_brake_rate: float | None = None
    harsh_accel_rate: float | None = None
    harsh_corner_rate: float | None = None
    rush_hour_trip_pct: float | None = None
    night_trip_pct: float | None = None
    weekend_harsh_pct: float | None = None
    avg_risk_score: float | None = None
    scored_devices: int | None = None
    high_risk_devices: int | None = None
    critical_risk_devices: int | None = None
    high_or_critical_devices: int | None = None
    high_risk_device_pct: float | None = None
    safety_score: float | None = None
    safety_score_trend: float | None = None


class SafetyScorecardMonthly(BaseModel):
    """One month, fleet-aggregate. Rates recomputed from totals."""

    year_month: str
    active_devices: int | None = None
    total_distance_km: float | None = None
    total_overspeed: int | None = None
    total_harsh_events: int | None = None
    harsh_brake_count: int | None = None
    harsh_accel_count: int | None = None
    harsh_corner_count: int | None = None
    scored_devices: int | None = None
    high_risk_devices: int | None = None
    critical_risk_devices: int | None = None
    high_or_critical_devices: int | None = None
    overspeed_rate_per_1000km: float | None = None
    harsh_events_per_1000km: float | None = None
    safety_score: float | None = None
    high_risk_device_pct: float | None = None


class SafetyScorecardKpi(BaseModel):
    """Latest-month KPI strip with prior-month deltas (absolute, not %)."""

    year_month: str
    overspeed_rate_per_1000km: float | None = None
    overspeed_rate_delta: float | None = None
    harsh_events_per_1000km: float | None = None
    harsh_events_delta: float | None = None
    high_or_critical_devices: int | None = None
    high_or_critical_delta: int | None = None
    safety_score: float | None = None
    safety_score_delta: float | None = None
    active_devices: int | None = None
    total_distance_km: float | None = None


class SafetyScorecardDashboardResponse(BaseModel):
    rows: list[SafetyScorecardRow]
    monthly: list[SafetyScorecardMonthly]
    kpi: SafetyScorecardKpi | None = None


# ---------------------------------------------------------------------------
# Predictive alerts — marts.v_predictive_alerts_dashboard
#                   + marts.v_real_time_alerts_stream
# ---------------------------------------------------------------------------


class PredictiveAlertRow(BaseModel):
    """One row of v_predictive_alerts_dashboard."""

    alert_id: str
    tenant_id: int | None = None
    device_id: int | None = None
    alert_type: str
    severity: str
    detected_at: datetime
    alert_message: str | None = None
    alert_value: float | None = None
    assigned_to: str | None = None
    status: str | None = None
    resolution_time_hours: float | None = None


class LiveAlertRow(BaseModel):
    """One row of v_real_time_alerts_stream."""

    alert_id: str
    tenant_id: int | None = None
    device_id: int | None = None
    alert_category: str
    severity: str
    detected_at: datetime
    alert_message: str | None = None
    alert_value: float | None = None
    latest_trip_time: datetime | None = None
    distance_km_24h: float | None = None
    device_risk_score: float | None = None
    device_risk_category: str | None = None
    device_cluster: int | None = None
    priority_score: float | None = None


class AlertCount(BaseModel):
    key: str
    count: int


class PredictiveAlertsSummary(BaseModel):
    """Pre-aggregated counts so the React page doesn't recompute on every render."""

    total: int = 0
    high_or_critical: int = 0
    last_24h: int = 0
    unique_devices: int = 0
    by_severity: list[AlertCount] = Field(default_factory=list)
    by_type: list[AlertCount] = Field(default_factory=list)


class LiveAlertsSummary(BaseModel):
    total: int = 0
    high_or_critical: int = 0
    avg_priority: float = 0.0
    unique_devices: int = 0
    by_category: list[AlertCount] = Field(default_factory=list)


class PredictiveAlertsDashboardResponse(BaseModel):
    alerts: list[PredictiveAlertRow]
    stream: list[LiveAlertRow]
    alerts_summary: PredictiveAlertsSummary
    stream_summary: LiveAlertsSummary


# ---------------------------------------------------------------------------
# Tenant billing — marts.v_tenant_billing_dashboard
# ---------------------------------------------------------------------------


class TenantBillingRow(BaseModel):
    """One row of v_tenant_billing_dashboard (per tenant × year_month)."""

    tenant_id: int | None = None
    tenant_name: str | None = None
    year_month: str
    active_devices: int | None = None
    active_vehicles: int | None = None
    total_devices_registered: int | None = None
    active_devices_registered: int | None = None
    total_trips_processed: int | None = None
    total_distance_km: float | None = None
    total_alerts_generated: int | None = None
    total_harsh_events: int | None = None
    total_overspeed_events: int | None = None
    total_events: int | None = None
    data_volume_gb: float | None = None
    db_storage_mb: float | None = None
    avg_daily_events: float | None = None
    events_per_device: float | None = None
    alerts_per_trip: float | None = None
    data_per_device_mb: float | None = None
    pricing_tier: int | None = None
    price_per_device: float | None = None
    estimated_revenue: float | None = None
    estimated_infra_cost: float | None = None
    devices_mom_growth_pct: float | None = None
    trips_mom_growth_pct: float | None = None
    alerts_mom_growth_pct: float | None = None


class TenantBillingMonthly(BaseModel):
    """One month, summed across tenants in scope."""

    year_month: str
    active_devices: int | None = None
    total_trips_processed: int | None = None
    data_volume_gb: float | None = None
    estimated_revenue: float | None = None


class TenantBillingKpi(BaseModel):
    """Latest-month totals + prior-month deltas."""

    year_month: str
    total_tenants: int = 0
    total_tenants_delta: int | None = None
    total_devices: int = 0
    total_devices_delta: int | None = None
    total_revenue: float = 0.0
    total_revenue_delta_pct: float | None = None
    total_storage_gb: float = 0.0
    total_storage_delta_pct: float | None = None


class TenantBillingTier(BaseModel):
    pricing_tier: int
    tenants: int
    devices: int
    revenue: float


class TenantBillingDashboardResponse(BaseModel):
    rows: list[TenantBillingRow]
    monthly: list[TenantBillingMonthly]
    kpi: TenantBillingKpi | None = None
    tier_breakdown: list[TenantBillingTier] = Field(default_factory=list)
    latest_month: str | None = None
