import { useMemo } from "react";
import { useQueries, useQuery } from "@tanstack/react-query";
import {
  Activity,
  AlertTriangle,
  Banknote,
  Cpu,
  Gauge,
  Route,
  ShieldCheck,
} from "lucide-react";
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
import { PageContainer } from "@/components/shell";
import { KpiCard, Panel, Skeleton, Badge } from "@/components/ui";
import { StateMessage } from "@/components/StateMessage";
import { DataTable, type ColumnDef } from "@/components/DataTable";
import { LineChart } from "@/components/charts/LineChart";
import { BarChart } from "@/components/charts/BarChart";
import { fmtDec, fmtInt } from "@/lib/format";

// Numbers as a sparkline series — pull the last N months of one metric
// from the monthly rows. Keeps the KPI sparks coherent with the charts
// below: same data, no second API call.
function trail<T>(
  rows: ReadonlyArray<T>,
  pick: (r: T) => number | null | undefined,
  n = 8,
): number[] {
  return rows
    .slice(-n)
    .map(pick)
    .filter((v): v is number => typeof v === "number" && Number.isFinite(v));
}

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
  // pages already fetch their own data when navigated to, so paying for
  // them here costs little after the first hit (TanStack cache).
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
    <PageContainer
      title="Executive overview"
      actions={<Badge variant="accent">Live</Badge>}
    >
      {isPending ? <LoadingSkeleton /> : null}

      {isError ? (
        <StateMessage tone="error">
          Failed to load executive overview: {(error as Error).message}
        </StateMessage>
      ) : null}

      {data ? (
        <Content data={data} safety={safety} risk={risk} alerts={alerts} />
      ) : null}
    </PageContainer>
  );
}

function LoadingSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-5">
        {Array.from({ length: 5 }).map((_, i) => (
          <KpiCard key={i} label="" value="" loading />
        ))}
      </div>
      <Skeleton className="h-72 w-full rounded-lg" />
      <Skeleton className="h-72 w-full rounded-lg" />
    </div>
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

  // Derive sparkline series once so they re-use the same trimming.
  const tripsTrail = useMemo(
    () => trail(monthly, (r) => r.total_trips),
    [monthly],
  );
  const distanceTrail = useMemo(
    () => trail(monthly, (r) => r.total_distance_km),
    [monthly],
  );
  const costPerKmTrail = useMemo(
    () => trail(monthly, (r) => r.cost_per_km),
    [monthly],
  );
  const devicesTrail = useMemo(
    () => trail(monthly, (r) => r.active_devices),
    [monthly],
  );

  return (
    <div className="space-y-6">
      {/* Fleet health composite strip — pulls from the v2.0 endpoints. */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-5">
        <KpiCard
          label="Fleet health"
          value={safetyScore !== null ? `${fmtInt(safetyScore)} / 100` : "—"}
          icon={<ShieldCheck />}
          tone="success"
          trend={
            safety?.kpi?.safety_score_delta == null
              ? undefined
              : { delta: safety.kpi.safety_score_delta, label: "vs prior" }
          }
          hint="Safety score (higher = safer)"
          loading={!safety}
        />
        <KpiCard
          label="Avg risk score"
          value={avgRisk !== null ? fmtDec(avgRisk) : "—"}
          icon={<Gauge />}
          tone="warning"
          hint="Device-weighted, 0–100"
          loading={!risk}
        />
        <KpiCard
          label="Active devices"
          value={fmtInt(kpi.active_devices)}
          icon={<Cpu />}
          tone="primary"
          sparkline={devicesTrail}
        />
        <KpiCard
          label="Cost / km"
          value={fmtDec(kpi.cost_per_km)}
          icon={<Banknote />}
          tone="accent"
          sparkline={costPerKmTrail}
        />
        <KpiCard
          label="Active alerts"
          value={fmtInt(activeAlerts)}
          icon={<AlertTriangle />}
          tone="danger"
          hint={
            highCritical !== null
              ? `${fmtInt(highCritical)} high / critical`
              : undefined
          }
          loading={!alerts}
        />
      </div>

      {/* Operational KPI strip — fleet totals for latest month. */}
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <KpiCard
          label="Active devices"
          value={fmtInt(kpi.active_devices)}
          icon={<Cpu />}
          sparkline={devicesTrail}
        />
        <KpiCard
          label="Total trips"
          value={fmtInt(kpi.total_trips)}
          icon={<Activity />}
          sparkline={tripsTrail}
        />
        <KpiCard
          label="Distance (km)"
          value={fmtInt(kpi.total_distance_km)}
          icon={<Route />}
          sparkline={distanceTrail}
        />
        <KpiCard
          label="Cost / km"
          value={fmtDec(kpi.cost_per_km)}
          icon={<Banknote />}
          sparkline={costPerKmTrail}
        />
      </div>

      <p className="text-xs text-muted-foreground">
        KPIs above are the fleet total for{" "}
        <strong className="text-foreground">{kpi.year_month}</strong>, summed across{" "}
        {kpi.tenants_in_latest_month} tenant(s) in scope.
      </p>

      <div className="grid gap-6 lg:grid-cols-2">
        <Panel
          title="Trip volume over time"
          description="Monthly trip counts across all tenants in scope."
          tone="primary"
        >
          <LineChart
            data={monthly as unknown as Array<Record<string, unknown>>}
            xKey="year_month"
            series={[{ dataKey: "total_trips", label: "Trips" }]}
            yFormatter={(v) => fmtInt(v)}
          />
        </Panel>

        <Panel
          title="Operating cost trend"
          description="Monthly aggregate spend across the fleet."
          tone="accent"
        >
          <BarChart
            data={monthly as unknown as Array<Record<string, unknown>>}
            xKey="year_month"
            series={[{ dataKey: "total_operating_cost", label: "Operating cost" }]}
            yFormatter={(v) => fmtInt(v)}
          />
        </Panel>
      </div>

      <Panel
        title="Tenant × month detail"
        description="One row per tenant × month, ordered by year_month then tenant_id."
        actions={<Badge variant="outline">{data.rows.length} rows</Badge>}
        flush
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
