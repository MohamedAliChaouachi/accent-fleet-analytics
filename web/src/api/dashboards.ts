import { request } from "./client";
import type {
  DashboardFilters,
  ExecutiveDashboardResponse,
  FleetEfficiencyDashboardResponse,
  MaintenanceDashboardResponse,
  OperationsDashboardResponse,
  PredictiveAlertsDashboardResponse,
  RiskDashboardResponse,
  SafetyScorecardDashboardResponse,
  TenantBillingDashboardResponse,
} from "./types";

function asQuery(f: DashboardFilters) {
  return {
    start: f.start,
    end: f.end,
    tenant_ids: f.tenant_ids.length ? f.tenant_ids : undefined,
  };
}

export function fetchExecutive(f: DashboardFilters, signal?: AbortSignal) {
  return request<ExecutiveDashboardResponse>("/dashboards/executive", {
    query: asQuery(f),
    signal,
  });
}

export function fetchOperations(f: DashboardFilters, signal?: AbortSignal) {
  return request<OperationsDashboardResponse>("/dashboards/operations", {
    query: asQuery(f),
    signal,
  });
}

export function fetchMaintenance(f: DashboardFilters, signal?: AbortSignal) {
  return request<MaintenanceDashboardResponse>("/dashboards/maintenance", {
    query: asQuery(f),
    signal,
  });
}

export function fetchRisk(f: DashboardFilters, signal?: AbortSignal) {
  return request<RiskDashboardResponse>("/dashboards/risk", {
    query: asQuery(f),
    signal,
  });
}

export function fetchFleetEfficiency(f: DashboardFilters, signal?: AbortSignal) {
  return request<FleetEfficiencyDashboardResponse>("/dashboards/fleet-efficiency", {
    query: asQuery(f),
    signal,
  });
}

export function fetchSafetyScorecard(f: DashboardFilters, signal?: AbortSignal) {
  return request<SafetyScorecardDashboardResponse>("/dashboards/safety-scorecard", {
    query: asQuery(f),
    signal,
  });
}

export function fetchPredictiveAlerts(f: DashboardFilters, signal?: AbortSignal) {
  return request<PredictiveAlertsDashboardResponse>("/dashboards/predictive-alerts", {
    query: asQuery(f),
    signal,
  });
}

export function fetchTenantBilling(f: DashboardFilters, signal?: AbortSignal) {
  return request<TenantBillingDashboardResponse>("/dashboards/tenant-billing", {
    query: asQuery(f),
    signal,
  });
}
