// Hand-written mirrors of the Pydantic models in app/schemas/*.
// Codegen via openapi-typescript is a follow-up; for v1 the surface is
// small enough that a hand-written file is easier to scan when wiring
// pages. Keep field names byte-identical to the API JSON.

// ---------- auth ----------
export interface TokenPair {
  access_token: string;
  refresh_token: string;
  expires_at: number; // unix-epoch seconds
  token_type: "bearer";
}

export type UserRole = "tenant_user" | "tenant_admin" | "superadmin";

export interface MeResponse {
  user_id: number;
  email: string;
  tenant_id: number | null;
  tenant_name: string | null;
  role: UserRole;
  last_login_at: string | null;
}

// ---------- scoring ----------
export interface FeatureVector {
  device_id?: number | null;
  month?: string | null;
  overspeed_per_100km?: number | null;
  overspeed_count?: number | null;
  overspeed_severity_high?: number | null;
  overspeed_severity_extreme?: number | null;
  high_speed_trip_ratio?: number | null;
  speed_alert_per_100km?: number | null;
  night_trip_ratio?: number | null;
  avg_max_speed_kmh?: number | null;
  [extra: string]: number | string | null | undefined;
}

export interface RiskScoreResponse {
  risk_score: number;
  category: "low" | "moderate" | "high" | "critical";
  components: Record<string, number>;
  version: string;
}

export interface ClusterScoreResponse {
  cluster_id: number;
  distance: number;
  model_name: string;
  model_version: string;
}

// ---------- dashboards: executive ----------
export interface ExecutiveMonthlyRow {
  tenant_id: number | null;
  year_month: string;
  active_vehicles: number | null;
  active_devices: number | null;
  total_trips: number | null;
  total_distance_km: number | null;
  total_maintenance_cost: number | null;
  total_fuel_cost: number | null;
  total_operating_cost: number | null;
  cost_per_km: number | null;
  total_alerts: number | null;
  panic_alerts: number | null;
  total_overspeed: number | null;
  total_harsh_events: number | null;
  distance_km_mom_delta: number | null;
  operating_cost_mom_delta: number | null;
  alerts_mom_delta: number | null;
  distance_km_3mo_avg: number | null;
  cost_per_km_3mo_avg: number | null;
}

export interface ExecutiveMonthlyAggregate {
  year_month: string;
  active_vehicles: number | null;
  active_devices: number | null;
  total_trips: number | null;
  total_distance_km: number | null;
  total_maintenance_cost: number | null;
  total_fuel_cost: number | null;
  total_operating_cost: number | null;
  total_alerts: number | null;
  panic_alerts: number | null;
  total_overspeed: number | null;
  total_harsh_events: number | null;
  cost_per_km: number | null;
}

export interface ExecutiveKpi {
  year_month: string;
  tenants_in_latest_month: number;
  active_devices: number | null;
  total_trips: number | null;
  total_distance_km: number | null;
  cost_per_km: number | null;
}

export interface ExecutiveDashboardResponse {
  rows: ExecutiveMonthlyRow[];
  monthly: ExecutiveMonthlyAggregate[];
  kpi: ExecutiveKpi | null;
}

// ---------- dashboards: operations ----------
export interface OperationsDailyRow {
  tenant_id: number | null;
  fleet_date: string;
  active_devices: number | null;
  total_trips: number | null;
  total_distance_km: number | null;
  total_driving_hours: number | null;
  total_stops: number | null;
  total_alerts: number | null;
  speed_alerts: number | null;
  geofence_alerts: number | null;
  panic_alerts: number | null;
  maintenance_alerts: number | null;
  other_alerts: number | null;
  overspeed_events: number | null;
  harsh_brake_events: number | null;
  harsh_accel_events: number | null;
  harsh_corner_events: number | null;
  total_harsh_events: number | null;
  alerts_per_100km: number | null;
  overspeed_per_100km: number | null;
  harsh_events_per_100km: number | null;
  panic_share_pct: number | null;
  distance_km_7d_avg: number | null;
  alerts_7d_avg: number | null;
}

export interface OperationsKpi {
  total_trips: number;
  total_distance_km: number;
  total_alerts: number;
  total_overspeed: number;
}

export interface OperationsDashboardResponse {
  rows: OperationsDailyRow[];
  kpi: OperationsKpi;
}

// ---------- dashboards: maintenance ----------
export interface MaintenanceRow {
  tenant_id: number | null;
  vehicle_id: number | null;
  matricule: string | null;
  vehicle_mark: string | null;
  vehicle_class: string | null;
  year_month: string;
  total_distance_km: number | null;
  active_days: number | null;
  maintenance_events: number | null;
  offense_events: number | null;
  sinistre_events: number | null;
  reparation_events: number | null;
  maintenance_cost_total: number | null;
  maintenance_labor_total: number | null;
  reparation_amount_total: number | null;
  fuel_cost_total: number | null;
  total_cost: number | null;
  maintenance_share_pct: number | null;
  cost_per_km: number | null;
  fuel_l_per_100km: number | null;
  avg_repair_hours: number | null;
  max_repair_hours: number | null;
  cost_rank_in_tenant: number | null;
}

export interface MaintenanceKpi {
  maintenance_events: number;
  total_cost: number;
}

export interface MaintenanceDashboardResponse {
  rows: MaintenanceRow[];
  kpi: MaintenanceKpi;
  top_cost_vehicles: MaintenanceRow[];
}

// ---------- dashboards: risk ----------
export interface FleetRiskRow {
  tenant_id: number | null;
  tenant_label: string | null;
  devices_scored: number | null;
  critical_devices: number | null;
  high_devices: number | null;
  moderate_devices: number | null;
  low_devices: number | null;
  avg_risk_score: number | null;
  max_risk_score: number | null;
  fleet_overspeed_3m: number | null;
  fleet_distance_3m: number | null;
  fleet_overspeed_per_100km: number | null;
  fleet_highest_avg_max_speed: number | null;
  fleet_avg_night_ratio: number | null;
}

export interface DeviceRiskRow {
  tenant_id: number | null;
  device_id: number;
  latest_month: string | null;
  trips_3m: number | null;
  distance_3m: number | null;
  overspeed_3m: number | null;
  severe_overspeed_3m: number | null;
  alerts_3m: number | null;
  risk_score: number | null;
  risk_category: "low" | "moderate" | "high" | "critical" | null;
}

export interface ClusterAssignmentRow {
  tenant_id: number | null;
  device_id: number;
  year_month: string | null;
  cluster_id: number;
  distance_to_centroid: number | null;
  model_version: string | null;
  risk_score: number | null;
  risk_category: DeviceRiskRow["risk_category"];
}

export interface RiskCategoryCount {
  category: string;
  device_count: number;
}

export interface ClusterSize {
  cluster_id: number;
  devices: number;
}

export interface ClusterRiskCrossRow {
  cluster_id: number;
  risk_category: string;
  devices: number;
}

export interface RiskDashboardResponse {
  fleet: FleetRiskRow[];
  devices: DeviceRiskRow[];
  category_counts: RiskCategoryCount[];
  clusters: ClusterAssignmentRow[];
  cluster_sizes: ClusterSize[];
  cluster_risk_crosstab: ClusterRiskCrossRow[];
  cluster_model_versions: string[];
}

// ---------- devices (existing endpoints, used by Risk drill-down) ----------
export interface DeviceMonthRow {
  tenant_id: number | null;
  device_id: number;
  year_month: string;
  total_trips: number | null;
  total_distance_km: number | null;
  overspeed_count: number | null;
  overspeed_per_100km: number | null;
  high_speed_trip_ratio: number | null;
  night_trip_ratio: number | null;
  avg_max_speed_kmh: number | null;
}

export interface DeviceProfileResponse {
  device_id: number;
  rolling: {
    tenant_id: number | null;
    device_id: number;
    latest_month: string | null;
    trips_3m: number | null;
    distance_3m: number | null;
    overspeed_3m: number | null;
    severe_overspeed_3m: number | null;
    alerts_3m: number | null;
    risk_score: number | null;
    risk_category: string | null;
  } | null;
  history: DeviceMonthRow[];
}

// ---------- shared filter payload ----------
export interface DashboardFilters {
  start: string; // YYYY-MM-DD
  end: string; // YYYY-MM-DD
  tenant_ids: number[];
}
