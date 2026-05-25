import { useQueries, useQuery } from "@tanstack/react-query";
import {
  fetchExecutive,
  fetchPredictiveAlerts,
  fetchRisk,
  fetchSafetyScorecard,
} from "@/api/dashboards";
import type {
  ExecutiveDashboardResponse,
  ExecutiveMonthlyRow,
  PredictiveAlertsDashboardResponse,
  RiskDashboardResponse,
  SafetyScorecardDashboardResponse,
} from "@/api/types";
import { useFilters } from "@/filters/FiltersContext";
import { KpiCard } from "@/components/KpiCard";
import { Panel } from "@/components/Panel";
import { PageHeader } from "@/components/PageHeader";
import { StateMessage } from "@/components/StateMessage";
import { DataTable, type ColumnDef } from "@/components/DataTable";
import { LineChart } from "@/components/charts/LineChart";
import { BarChart } from "@/components/charts/BarChart";
import { RISK_COLORS } from "@/lib/colors";
import { fmtDec, fmtInt } from "@/lib/format";

const ROW_COLUMNS: ReadonlyArray<ColumnDef<ExecutiveMonthlyRow>> = [
  { key: "year_month", header: "Month", accessor: (r) => r.year_month },
  {
    key: "tenant_id",
    header: "Tenant",
    accessor: (r) => r.tenant_id ?? "—",
    align: "right",
  },
  {
    key: "active_devices",
    header: "Active devices",
    accessor: (r) => fmtInt(r.active_devices),
    align: "right",
  },
  {
    key: "total_trips",
    header: "Trips",
    accessor: (r) => fmtInt(r.total_trips),
    align: "right",
  },
  {
    key: "total_distance_km",
    header: "Distance (km)",
    accessor: (r) => fmtInt(r.total_distance_km),
    align: "right",
  },
  {
    key: "total_operating_cost",
    header: "Operating cost",
    accessor: (r) => fmtInt(r.total_operating_cost),
    align: "right",
  },
  {
    key: "cost_per_km",
    header: "Cost / km",
    accessor: (r) => fmtDec(r.cost_per_km),
    align: "right",
  },
];

export function ExecutiveOverview() {
  const { filters } = useFilters();
  const { data, isPending, isError, error } = useQuery<ExecutiveDashboardResponse>({
    queryKey: ["dashboards", "executive", filters],
    queryFn: ({ signal }) => fetchExecutive(filters, signal),
  });

  // Fan out to the v2.0 endpoints so the top "health strip" can show
  // composite KPIs without bloating the executive view payload. These
  // pages already fetch their own data when navigated to, so paying
  // for them here costs little after the first hit (TanStack cache).
  const sideQueries = useQueries({
    queries: [
      {
        queryKey: ["dashboards", "safety-scorecard", filters],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          fetchSafetyScorecard(filters, signal),
      },
      {
        queryKey: ["dashboards", "risk", filters],
        queryFn: ({ signal }: { signal?: AbortSignal }) => fetchRisk(filters, signal),
      },
      {
        queryKey: ["dashboards", "predictive-alerts", filters],
        queryFn: ({ signal }: { signal?: AbortSignal }) =>
          fetchPredictiveAlerts(filters, signal),
      },
    ],
  });
  const [safetyQ, riskQ, alertsQ] = sideQueries;
  const safety = safetyQ.data as SafetyScorecardDashboardResponse | undefined;
  const risk = riskQ.data as RiskDashboardResponse | undefined;
  const alerts = alertsQ.data as PredictiveAlertsDashboardResponse | undefined;

  return (
    <section>
      <PageHeader
        title="Executive overview"
        caption={
          <>
            Fleet-wide KPIs sourced from{" "}
            <code className="rounded bg-slate-200 px-1 py-0.5">marts.v_executive_dashboard</code>{" "}
            with composite health KPIs from safety / risk / alerts.
          </>
        }
      />

      {isPending ? <StateMessage>Loading executive overview…</StateMessage> : null}

      {isError ? (
        <StateMessage tone="error">
          Failed to load executive overview: {(error as Error).message}
        </StateMessage>
      ) : null}

      {data ? (
        <Content data={data} safety={safety} risk={risk} alerts={alerts} />
      ) : null}
    </section>
  );
}

interface CompositeSources {
  safety?: SafetyScorecardDashboardResponse;
  risk?: RiskDashboardResponse;
  alerts?: PredictiveAlertsDashboardResponse;
}

function avgRiskAcrossFleet(risk?: RiskDashboardResponse): number | null {
  if (!risk || risk.fleet.length === 0) return null;
  // Device-weighted average so a tiny tenant with avg=90 doesn't drag the
  // composite up to a misleading number.
  let weight = 0;
  let sum = 0;
  for (const r of risk.fleet) {
    const w = r.devices_scored ?? 0;
    const s = r.avg_risk_score ?? null;
    if (w > 0 && s !== null) {
      weight += w;
      sum += s * w;
    }
  }
  return weight > 0 ? sum / weight : null;
}

function Content({
  data,
  safety,
  risk,
  alerts,
}: { data: ExecutiveDashboardResponse } & CompositeSources) {
  if (!data.kpi || data.monthly.length === 0) {
    return (
      <StateMessage tone="warning">
        No data for the current filters. Try widening the date range or clearing the
        tenant filter.
      </StateMessage>
    );
  }

  const { kpi, monthly } = data;
  const safetyScore = safety?.kpi?.safety_score ?? null;
  const avgRisk = avgRiskAcrossFleet(risk);
  const activeAlerts = alerts?.alerts_summary.total ?? null;
  const highCritical = alerts?.alerts_summary.high_or_critical ?? null;

  return (
    <div className="space-y-6">
      {/* Fleet health composite strip — pulls from the v2.0 endpoints. */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-5">
        <KpiCard
          label="Fleet health"
          value={safetyScore !== null ? `${fmtInt(safetyScore)} / 100` : "—"}
          trend={
            safety?.kpi?.safety_score_delta == null
              ? undefined
              : { delta: safety.kpi.safety_score_delta, label: "vs prior" }
          }
          accent={RISK_COLORS.low}
          hint="Safety score (higher = safer)"
        />
        <KpiCard
          label="Avg risk score"
          value={avgRisk !== null ? fmtDec(avgRisk) : "—"}
          accent={RISK_COLORS.high}
          hint="Device-weighted, 0–100"
        />
        <KpiCard
          label="Active devices"
          value={fmtInt(kpi.active_devices)}
          accent="#1f3a5f"
        />
        <KpiCard
          label="Cost / km"
          value={fmtDec(kpi.cost_per_km)}
          accent="#2a9df4"
        />
        <KpiCard
          label="Active alerts"
          value={fmtInt(activeAlerts)}
          accent={RISK_COLORS.critical}
          hint={
            highCritical !== null
              ? `${fmtInt(highCritical)} high / critical`
              : undefined
          }
        />
      </div>

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <KpiCard label="Active devices" value={fmtInt(kpi.active_devices)} />
        <KpiCard label="Total trips" value={fmtInt(kpi.total_trips)} />
        <KpiCard label="Distance (km)" value={fmtInt(kpi.total_distance_km)} />
        <KpiCard label="Cost / km" value={fmtDec(kpi.cost_per_km)} />
      </div>

      <p className="text-xs text-slate-500">
        KPIs above are the fleet total for <strong>{kpi.year_month}</strong>, summed across{" "}
        {kpi.tenants_in_latest_month} tenant(s) in scope.
      </p>

      <Panel title="Trip volume over time">
        <LineChart
          data={monthly as unknown as Array<Record<string, unknown>>}
          xKey="year_month"
          series={[{ dataKey: "total_trips", label: "Trips" }]}
          yFormatter={(v) => fmtInt(v)}
        />
      </Panel>

      <Panel title="Operating cost trend">
        <BarChart
          data={monthly as unknown as Array<Record<string, unknown>>}
          xKey="year_month"
          series={[{ dataKey: "total_operating_cost", label: "Operating cost" }]}
          yFormatter={(v) => fmtInt(v)}
        />
      </Panel>

      <Panel
        title="Raw table"
        description="One row per tenant × month, ordered by year_month then tenant_id."
      >
        <DataTable
          rows={data.rows}
          columns={ROW_COLUMNS}
          rowKey={(r) => `${r.year_month}::${r.tenant_id ?? "all"}`}
        />
      </Panel>
    </div>
  );
}
