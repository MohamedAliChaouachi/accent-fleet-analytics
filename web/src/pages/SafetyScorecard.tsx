import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Gauge, ShieldAlert, ShieldCheck, TriangleAlert } from "lucide-react";
import { fetchSafetyScorecard } from "@/api/dashboards";
import type {
  SafetyScorecardDashboardResponse,
  SafetyScorecardMonthly,
  SafetyScorecardRow,
} from "@/api/types";
import { useFilters } from "@/filters/FiltersContext";
import { PageContainer } from "@/components/shell";
import { Badge, KpiCard, Panel, Skeleton } from "@/components/ui";
import { StateMessage } from "@/components/StateMessage";
import { DataTable, type ColumnDef } from "@/components/DataTable";
import { LineChart } from "@/components/charts/LineChart";
import { BarChart } from "@/components/charts/BarChart";
import { PieChart } from "@/components/charts/PieChart";
import { RISK_COLORS } from "@/lib/colors";
import { fmtDec, fmtInt } from "@/lib/format";

// KpiCard uses % MoM by convention; safety deltas are absolute (rate per
// 1000 km, score points). We invert lower-is-better deltas so the card's
// "positive=green" rule still reads correctly.
function invertAbs(d: number | null | undefined): number | undefined {
  if (d === null || d === undefined) return undefined;
  return -d;
}

export function SafetyScorecard() {
  const { filters } = useFilters();
  const { data, isPending, isError, error } = useQuery<SafetyScorecardDashboardResponse>({
    queryKey: ["dashboards", "safety-scorecard", filters],
    queryFn: ({ signal }) => fetchSafetyScorecard(filters, signal),
  });

  return (
    <PageContainer
      title="Safety scorecard"
      actions={<Badge variant="warning">Monthly rollup</Badge>}
    >
      {isPending ? <LoadingSkeleton /> : null}
      {isError ? (
        <StateMessage tone="error">
          Failed to load safety scorecard: {(error as Error).message}
        </StateMessage>
      ) : null}
      {data ? <Content data={data} /> : null}
    </PageContainer>
  );
}

function LoadingSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <KpiCard key={i} label="" value="" loading />
        ))}
      </div>
      <Skeleton className="h-72 w-full rounded-lg" />
    </div>
  );
}

function Content({ data }: { data: SafetyScorecardDashboardResponse }) {
  const { kpi, monthly, rows } = data;

  const harshSeries = useMemo(() => buildHarshTypeSeries(monthly), [monthly]);
  const latestPerTenant = useMemo(() => {
    if (!kpi) return [];
    return rows.filter((r) => r.year_month === kpi.year_month);
  }, [rows, kpi]);
  const riskDistribution = useMemo(() => {
    const scored = latestPerTenant.reduce((s, r) => s + (r.scored_devices ?? 0), 0);
    const hoc = latestPerTenant.reduce((s, r) => s + (r.high_or_critical_devices ?? 0), 0);
    const safer = Math.max(scored - hoc, 0);
    return [
      { name: "High / critical", value: hoc, color: RISK_COLORS.critical },
      { name: "Moderate / low", value: safer, color: RISK_COLORS.low },
    ];
  }, [latestPerTenant]);

  if (!kpi || monthly.length === 0) {
    return (
      <StateMessage tone="warning">
        No safety data for the current filters. Widen the date range or clear the tenant
        filter.
      </StateMessage>
    );
  }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KpiCard
          label="Overspeed / 1000 km"
          value={fmtDec(kpi.overspeed_rate_per_1000km)}
          icon={<Gauge />}
          tone="warning"
          trend={
            kpi.overspeed_rate_delta === null
              ? undefined
              : { delta: invertAbs(kpi.overspeed_rate_delta) ?? 0, label: "vs prior" }
          }
        />
        <KpiCard
          label="Harsh / 1000 km"
          value={fmtDec(kpi.harsh_events_per_1000km)}
          icon={<TriangleAlert />}
          tone="danger"
          trend={
            kpi.harsh_events_delta === null
              ? undefined
              : { delta: invertAbs(kpi.harsh_events_delta) ?? 0, label: "vs prior" }
          }
        />
        <KpiCard
          label="High-risk devices"
          value={fmtInt(kpi.high_or_critical_devices)}
          icon={<ShieldAlert />}
          tone="danger"
          trend={
            kpi.high_or_critical_delta === null
              ? undefined
              : { delta: invertAbs(kpi.high_or_critical_delta) ?? 0, label: "vs prior" }
          }
        />
        <KpiCard
          label="Safety score"
          value={`${fmtInt(kpi.safety_score)}`}
          icon={<ShieldCheck />}
          tone="primary"
          trend={
            kpi.safety_score_delta === null
              ? undefined
              : { delta: kpi.safety_score_delta ?? 0, label: "vs prior" }
          }
        />
      </div>

      <p className="text-xs text-muted-foreground">
        Latest: <strong className="text-foreground">{kpi.year_month}</strong> ·{" "}
        {fmtInt(kpi.active_devices)} active devices · {fmtInt(kpi.total_distance_km)} km driven
      </p>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        <Panel
          title="Overspeed rate"
          description="Events per 1000 km, monthly."
          tone="warning"
        >
          <LineChart
            data={monthly as unknown as Array<Record<string, unknown>>}
            xKey="year_month"
            series={[
              { dataKey: "overspeed_rate_per_1000km", label: "Overspeed / 1000km" },
            ]}
            yFormatter={(v) => fmtDec(v)}
          />
        </Panel>
        <Panel
          title="Harsh events by type"
          description="Stacked monthly counts."
          tone="warning"
        >
          {harshSeries.data.length === 0 ? (
            <p className="text-sm text-muted-foreground">No harsh-event data.</p>
          ) : (
            <BarChart
              data={harshSeries.data as unknown as Array<Record<string, unknown>>}
              xKey="year_month"
              series={harshSeries.series}
              yFormatter={(v) => fmtInt(v)}
              legend
            />
          )}
        </Panel>
        <Panel
          title="Risk distribution (latest)"
          description="Latest-month risk split."
          tone="primary"
        >
          {riskDistribution.every((d) => d.value === 0) ? (
            <p className="text-sm text-muted-foreground">No scored devices yet.</p>
          ) : (
            <PieChart data={riskDistribution} donut />
          )}
        </Panel>
      </div>

      <Panel
        title={`Per-tenant risk · ${kpi.year_month}`}
        description="One row per tenant in the latest month."
        actions={<Badge variant="outline">{latestPerTenant.length} tenants</Badge>}
        flush
      >
        <DataTable
          rows={latestPerTenant}
          columns={PER_TENANT_COLUMNS}
          rowKey={(r) => r.tenant_id ?? "all"}
        />
      </Panel>

      <Panel
        title="Raw table"
        description="Per-tenant × month rows."
        actions={<Badge variant="outline">{rows.length} rows</Badge>}
        flush
      >
        <DataTable
          rows={rows}
          columns={RAW_COLUMNS}
          rowKey={(r) => `${r.year_month}::${r.tenant_id ?? "all"}`}
          maxHeight="32rem"
        />
      </Panel>
    </div>
  );
}

function buildHarshTypeSeries(monthly: ReadonlyArray<SafetyScorecardMonthly>) {
  const data = monthly.map((m) => ({
    year_month: m.year_month,
    brake: m.harsh_brake_count ?? 0,
    accel: m.harsh_accel_count ?? 0,
    corner: m.harsh_corner_count ?? 0,
  }));
  const series = [
    { dataKey: "brake", label: "Brake", color: RISK_COLORS.critical, stackId: "harsh" },
    { dataKey: "accel", label: "Accel", color: RISK_COLORS.high, stackId: "harsh" },
    { dataKey: "corner", label: "Corner", color: RISK_COLORS.moderate, stackId: "harsh" },
  ];
  return { data, series };
}

const PER_TENANT_COLUMNS: ReadonlyArray<ColumnDef<SafetyScorecardRow>> = [
  { key: "tenant_id", header: "Tenant", accessor: (r) => r.tenant_id ?? "—", align: "right" },
  {
    key: "active_devices",
    header: "Devices",
    accessor: (r) => fmtInt(r.active_devices),
    align: "right",
  },
  {
    key: "total_overspeed",
    header: "Overspeed",
    accessor: (r) => fmtInt(r.total_overspeed),
    align: "right",
  },
  {
    key: "total_harsh_events",
    header: "Harsh",
    accessor: (r) => fmtInt(r.total_harsh_events),
    align: "right",
  },
  {
    key: "high_or_critical_devices",
    header: "High+critical",
    accessor: (r) => fmtInt(r.high_or_critical_devices),
    align: "right",
  },
  {
    key: "safety_score",
    header: "Safety score",
    accessor: (r) => fmtDec(r.safety_score),
    align: "right",
  },
];

const RAW_COLUMNS: ReadonlyArray<ColumnDef<SafetyScorecardRow>> = [
  { key: "year_month", header: "Month", accessor: (r) => r.year_month },
  { key: "tenant_id", header: "Tenant", accessor: (r) => r.tenant_id ?? "—", align: "right" },
  {
    key: "overspeed_rate_per_1000km",
    header: "Overspeed / 1000km",
    accessor: (r) => fmtDec(r.overspeed_rate_per_1000km),
    align: "right",
  },
  {
    key: "harsh_events_per_1000km",
    header: "Harsh / 1000km",
    accessor: (r) => fmtDec(r.harsh_events_per_1000km),
    align: "right",
  },
  {
    key: "safety_score",
    header: "Safety score",
    accessor: (r) => fmtDec(r.safety_score),
    align: "right",
  },
  {
    key: "high_or_critical_devices",
    header: "High+critical",
    accessor: (r) => fmtInt(r.high_or_critical_devices),
    align: "right",
  },
];
