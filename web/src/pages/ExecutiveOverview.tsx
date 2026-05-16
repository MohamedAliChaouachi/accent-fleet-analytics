import { useQuery } from "@tanstack/react-query";
import { fetchExecutive } from "@/api/dashboards";
import type {
  ExecutiveDashboardResponse,
  ExecutiveMonthlyRow,
} from "@/api/types";
import { useFilters } from "@/filters/FiltersContext";
import { KpiCard } from "@/components/KpiCard";
import { Panel } from "@/components/Panel";
import { PageHeader } from "@/components/PageHeader";
import { StateMessage } from "@/components/StateMessage";
import { DataTable, type ColumnDef } from "@/components/DataTable";
import { LineChart } from "@/components/charts/LineChart";
import { BarChart } from "@/components/charts/BarChart";
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

  return (
    <section>
      <PageHeader
        title="Executive overview"
        caption={
          <>
            Fleet-wide KPIs sourced from{" "}
            <code className="rounded bg-slate-200 px-1 py-0.5">marts.v_executive_dashboard</code>.
          </>
        }
      />

      {isPending ? <StateMessage>Loading executive overview…</StateMessage> : null}

      {isError ? (
        <StateMessage tone="error">
          Failed to load executive overview: {(error as Error).message}
        </StateMessage>
      ) : null}

      {data ? <Content data={data} /> : null}
    </section>
  );
}

function Content({ data }: { data: ExecutiveDashboardResponse }) {
  if (!data.kpi || data.monthly.length === 0) {
    return (
      <StateMessage tone="warning">
        No data for the current filters. Try widening the date range or clearing the
        tenant filter.
      </StateMessage>
    );
  }

  const { kpi, monthly } = data;

  return (
    <div className="space-y-6">
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
