import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchMaintenance } from "@/api/dashboards";
import type {
  MaintenanceDashboardResponse,
  MaintenanceRow,
} from "@/api/types";
import { useFilters } from "@/filters/FiltersContext";
import { KpiCard } from "@/components/KpiCard";
import { Panel } from "@/components/Panel";
import { PageHeader } from "@/components/PageHeader";
import { StateMessage } from "@/components/StateMessage";
import { DataTable, type ColumnDef } from "@/components/DataTable";
import { BarChart } from "@/components/charts/BarChart";
import { fmtDec, fmtInt } from "@/lib/format";

const ROW_COLUMNS: ReadonlyArray<ColumnDef<MaintenanceRow>> = [
  { key: "year_month", header: "Month", accessor: (r) => r.year_month },
  { key: "matricule", header: "Matricule", accessor: (r) => r.matricule ?? "—" },
  { key: "vehicle_mark", header: "Mark", accessor: (r) => r.vehicle_mark ?? "—" },
  { key: "vehicle_class", header: "Class", accessor: (r) => r.vehicle_class ?? "—" },
  {
    key: "maintenance_events",
    header: "Maint. events",
    accessor: (r) => fmtInt(r.maintenance_events),
    align: "right",
  },
  {
    key: "total_cost",
    header: "Total cost",
    accessor: (r) => fmtInt(r.total_cost),
    align: "right",
  },
  {
    key: "cost_per_km",
    header: "Cost / km",
    accessor: (r) => fmtDec(r.cost_per_km),
    align: "right",
  },
  {
    key: "fuel_l_per_100km",
    header: "Fuel L / 100km",
    accessor: (r) => fmtDec(r.fuel_l_per_100km),
    align: "right",
  },
];

export function Maintenance() {
  const { filters } = useFilters();
  const { data, isPending, isError, error } = useQuery<MaintenanceDashboardResponse>({
    queryKey: ["dashboards", "maintenance", filters],
    queryFn: ({ signal }) => fetchMaintenance(filters, signal),
  });

  return (
    <section>
      <PageHeader
        title="Maintenance"
        caption={
          <>
            Upcoming work, fueling, fault counts from{" "}
            <code className="rounded bg-slate-200 px-1 py-0.5">marts.v_maintenance_dashboard</code>.
          </>
        }
      />

      {isPending ? <StateMessage>Loading maintenance…</StateMessage> : null}
      {isError ? (
        <StateMessage tone="error">
          Failed to load maintenance: {(error as Error).message}
        </StateMessage>
      ) : null}
      {data ? <Content data={data} /> : null}
    </section>
  );
}

function Content({ data }: { data: MaintenanceDashboardResponse }) {
  // Recharts y-axis treats category strings as unique keys; some
  // matricules can repeat across months in the top-cost list. Aggregate
  // up to one entry per matricule (or vehicle_id fallback) so the
  // horizontal bar chart renders one bar per vehicle.
  const topRows = useMemo(() => {
    const out = new Map<string, { label: string; total_cost: number }>();
    for (const row of data.top_cost_vehicles) {
      const label = row.matricule ?? (row.vehicle_id !== null ? `#${row.vehicle_id}` : "—");
      const prev = out.get(label)?.total_cost ?? 0;
      out.set(label, { label, total_cost: prev + (row.total_cost ?? 0) });
    }
    return Array.from(out.values()).sort((a, b) => a.total_cost - b.total_cost);
  }, [data.top_cost_vehicles]);

  if (data.rows.length === 0) {
    return (
      <StateMessage tone="warning">
        No maintenance rows in the selected window.
      </StateMessage>
    );
  }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-2">
        <KpiCard label="Maintenance events" value={fmtInt(data.kpi.maintenance_events)} />
        <KpiCard label="Total cost" value={fmtInt(data.kpi.total_cost)} />
      </div>

      <Panel
        title="Highest-cost vehicles"
        description="Top 20 vehicles by total_cost in the selected window."
      >
        {topRows.length === 0 ? (
          <p className="text-sm text-slate-500">No top-cost vehicles for this filter.</p>
        ) : (
          <BarChart
            data={topRows as unknown as Array<Record<string, unknown>>}
            xKey="label"
            series={[{ dataKey: "total_cost", label: "Total cost" }]}
            layout="vertical"
            height={Math.max(280, topRows.length * 22)}
            yFormatter={(v) => fmtInt(v)}
          />
        )}
      </Panel>

      <Panel title="Detail" description="One row per tenant × vehicle × month.">
        <DataTable
          rows={data.rows}
          columns={ROW_COLUMNS}
          rowKey={(r) =>
            `${r.year_month}::${r.tenant_id ?? "all"}::${r.vehicle_id ?? r.matricule ?? "?"}`
          }
        />
      </Panel>
    </div>
  );
}
