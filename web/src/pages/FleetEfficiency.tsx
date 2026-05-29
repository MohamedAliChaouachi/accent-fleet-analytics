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

  // Split the sorted-ascending list into two non-overlapping halves so Best
  // and Worst never show the same tenants — capped at 5 per side so big
  // fleets don't produce wall-of-text panels. With an odd row count the
  // median tenant is dropped (neither best nor worst); with very few
  // tenants both sides may end up empty and the panels collapse to a hint.
  const { best5, worst5 } = useMemo(() => {
    const half = Math.floor(rows.length / 2);
    const k = Math.min(5, half);
    return {
      best5: rows.slice(0, k),
      worst5: rows.slice(rows.length - k).reverse(),
    };
  }, [rows]);
  const tooFewForSplit = best5.length === 0;

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
          label="Cost / km (DT)"
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

      {tooFewForSplit ? (
        <StateMessage tone="info">
          Only {rows.length} tenant{rows.length === 1 ? "" : "s"} in scope for{" "}
          {kpi.year_month} — need at least 2 to split best vs worst. See the
          per-tenant detail table below.
        </StateMessage>
      ) : (
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
          <Panel
            title={`Best efficiency · ${kpi.year_month}`}
            description={`Lowest ${best5.length} cost/km tenant${best5.length === 1 ? "" : "s"} of ${rows.length}.`}
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
            description={`Highest ${worst5.length} cost/km tenant${worst5.length === 1 ? "" : "s"} of ${rows.length} — investigate cost drivers.`}
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
      )}

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
