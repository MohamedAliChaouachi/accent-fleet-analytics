import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Coins, Droplets, Gauge, TimerReset } from "lucide-react";
import { fetchFleetEfficiency } from "@/api/dashboards";
import type {
  FleetEfficiencyDashboardResponse,
  FleetEfficiencyRow,
} from "@/api/types";
import { useFilters } from "@/filters/FiltersContext";
import { PageContainer } from "@/components/shell";
import { Badge, KpiCard, Panel, Skeleton } from "@/components/ui";
import { StateMessage } from "@/components/StateMessage";
import { DataTable, type ColumnDef } from "@/components/DataTable";
import { LineChart } from "@/components/charts/LineChart";
import { BarChart } from "@/components/charts/BarChart";
import { fmtDec, fmtInt } from "@/lib/format";

// Cost per km / fuel L per 100 km are "lower is better" — flip the sign
// before handing to KpiCard's trend (which colours positive deltas green).
function invertDelta(d: number | null | undefined): number | undefined {
  if (d === null || d === undefined) return undefined;
  return -d;
}

const PER_TENANT_COLUMNS: ReadonlyArray<ColumnDef<FleetEfficiencyRow>> = [
  { key: "tenant_id", header: "Tenant", accessor: (r) => r.tenant_id ?? "—", align: "right" },
  {
    key: "active_devices",
    header: "Devices",
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
    key: "cost_per_km",
    header: "Cost / km",
    accessor: (r) => fmtDec(r.cost_per_km),
    align: "right",
  },
  {
    key: "fuel_litres_per_100km",
    header: "L / 100km",
    accessor: (r) => fmtDec(r.fuel_litres_per_100km),
    align: "right",
  },
  {
    key: "utilization_rate_pct",
    header: "Utilization %",
    accessor: (r) => fmtDec(r.utilization_rate_pct),
    align: "right",
  },
];

export function FleetEfficiency() {
  const { filters } = useFilters();
  const { data, isPending, isError, error } = useQuery<FleetEfficiencyDashboardResponse>({
    queryKey: ["dashboards", "fleet-efficiency", filters],
    queryFn: ({ signal }) => fetchFleetEfficiency(filters, signal),
  });

  return (
    <PageContainer
      title="Fleet efficiency"
      description={
        <>
          Cost-per-km, utilization, fuel economy — sourced from{" "}
          <code className="rounded bg-muted px-1.5 py-0.5 font-mono text-2xs text-foreground">
            marts.v_fleet_efficiency_dashboard
          </code>
          .
        </>
      }
      actions={<Badge variant="accent">Monthly rollup</Badge>}
    >
      {isPending ? <LoadingSkeleton /> : null}
      {isError ? (
        <StateMessage tone="error">
          Failed to load fleet efficiency: {(error as Error).message}
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
      <Skeleton className="h-72 w-full rounded-lg" />
    </div>
  );
}

function Content({ data }: { data: FleetEfficiencyDashboardResponse }) {
  const { kpi, monthly, best_worst_latest: rows } = data;

  const best5 = useMemo(() => rows.slice(0, 10), [rows]);
  // Slice tail for worst, then reverse so the worst is first (most negative
  // efficiency at the top of the list).
  const worst5 = useMemo(() => [...rows.slice(-10)].reverse(), [rows]);

  if (!kpi || monthly.length === 0) {
    return (
      <StateMessage tone="warning">
        No efficiency data for the current filters. Try widening the date range or
        clearing the tenant filter.
      </StateMessage>
    );
  }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KpiCard
          label="Cost / km (DA)"
          value={fmtDec(kpi.cost_per_km)}
          icon={<Coins />}
          tone="primary"
          trend={
            kpi.cost_per_km_delta_pct === null
              ? undefined
              : { delta: invertDelta(kpi.cost_per_km_delta_pct) ?? 0, label: "MoM" }
          }
        />
        <KpiCard
          label="Utilization %"
          value={`${fmtDec(kpi.utilization_rate_pct)}%`}
          icon={<Gauge />}
          tone="accent"
          trend={
            kpi.utilization_rate_delta_pct === null
              ? undefined
              : { delta: kpi.utilization_rate_delta_pct ?? 0, label: "MoM" }
          }
        />
        <KpiCard
          label="Fuel L / 100km"
          value={fmtDec(kpi.fuel_litres_per_100km)}
          icon={<Droplets />}
          tone="accent"
          trend={
            kpi.fuel_litres_per_100km_delta_pct === null
              ? undefined
              : { delta: invertDelta(kpi.fuel_litres_per_100km_delta_pct) ?? 0, label: "MoM" }
          }
        />
        <KpiCard
          label="Idle %"
          value={`${fmtDec(kpi.idle_time_pct)}%`}
          icon={<TimerReset />}
          tone="warning"
          trend={
            kpi.idle_time_pct_delta_pct === null
              ? undefined
              : { delta: invertDelta(kpi.idle_time_pct_delta_pct) ?? 0, label: "MoM" }
          }
        />
      </div>

      <p className="text-xs text-muted-foreground">
        Latest: <strong className="text-foreground">{kpi.year_month}</strong> ·{" "}
        {fmtInt(kpi.active_devices)} active devices · {fmtInt(kpi.total_trips)} trips
      </p>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <Panel
          title="Cost per km"
          description="Monthly trend across the selected tenants."
          tone="primary"
        >
          <LineChart
            data={monthly as unknown as Array<Record<string, unknown>>}
            xKey="year_month"
            series={[{ dataKey: "cost_per_km", label: "Cost / km" }]}
            yFormatter={(v) => fmtDec(v)}
          />
        </Panel>
        <Panel
          title="Utilization trend"
          description="Monthly active-device utilization."
          tone="accent"
        >
          <BarChart
            data={monthly as unknown as Array<Record<string, unknown>>}
            xKey="year_month"
            series={[{ dataKey: "utilization_rate_pct", label: "Utilization %" }]}
            yFormatter={(v) => fmtDec(v)}
          />
        </Panel>
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <Panel
          title={`Best efficiency · ${kpi.year_month}`}
          description="Ordered by cost/km ascending (lower is better)."
          tone="accent"
          flush
        >
          <DataTable
            rows={best5}
            columns={PER_TENANT_COLUMNS}
            rowKey={(r) => r.tenant_id ?? "all"}
          />
        </Panel>
        <Panel
          title={`Worst efficiency · ${kpi.year_month}`}
          description="Ordered by cost/km descending — investigate cost drivers."
          tone="warning"
          flush
        >
          <DataTable
            rows={worst5}
            columns={PER_TENANT_COLUMNS}
            rowKey={(r) => r.tenant_id ?? "all"}
          />
        </Panel>
      </div>

      <Panel
        title="Per-tenant detail"
        description="One row per tenant × month; ratios recomputed from totals at the fleet level."
        actions={<Badge variant="outline">{data.rows.length} rows</Badge>}
        flush
      >
        <DataTable
          rows={data.rows}
          columns={RAW_COLUMNS}
          rowKey={(r) => `${r.year_month}::${r.tenant_id ?? "all"}`}
          maxHeight="32rem"
        />
      </Panel>
    </div>
  );
}

const RAW_COLUMNS: ReadonlyArray<ColumnDef<FleetEfficiencyRow>> = [
  { key: "year_month", header: "Month", accessor: (r) => r.year_month },
  { key: "tenant_id", header: "Tenant", accessor: (r) => r.tenant_id ?? "—", align: "right" },
  {
    key: "active_devices",
    header: "Devices",
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
    key: "cost_per_km",
    header: "Cost / km",
    accessor: (r) => fmtDec(r.cost_per_km),
    align: "right",
  },
  {
    key: "cost_per_trip",
    header: "Cost / trip",
    accessor: (r) => fmtDec(r.cost_per_trip),
    align: "right",
  },
  {
    key: "fuel_litres_per_100km",
    header: "L / 100km",
    accessor: (r) => fmtDec(r.fuel_litres_per_100km),
    align: "right",
  },
  {
    key: "utilization_rate_pct",
    header: "Utilization %",
    accessor: (r) => fmtDec(r.utilization_rate_pct),
    align: "right",
  },
  {
    key: "idle_time_pct",
    header: "Idle %",
    accessor: (r) => fmtDec(r.idle_time_pct),
    align: "right",
  },
];
