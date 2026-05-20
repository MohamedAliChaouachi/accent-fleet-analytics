import { useQuery } from "@tanstack/react-query";
import { fetchOperations } from "@/api/dashboards";
import type {
  OperationsDailyRow,
  OperationsDashboardResponse,
} from "@/api/types";
import { useFilters } from "@/filters/FiltersContext";
import { KpiCard } from "@/components/KpiCard";
import { Panel } from "@/components/Panel";
import { PageHeader } from "@/components/PageHeader";
import { StateMessage } from "@/components/StateMessage";
import { DataTable, type ColumnDef } from "@/components/DataTable";
import { BarChart } from "@/components/charts/BarChart";
import { LineChart } from "@/components/charts/LineChart";
import { fmtDec, fmtInt } from "@/lib/format";

const ROW_COLUMNS: ReadonlyArray<ColumnDef<OperationsDailyRow>> = [
  { key: "fleet_date", header: "Date", accessor: (r) => r.fleet_date },
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
    key: "total_alerts",
    header: "Alerts",
    accessor: (r) => fmtInt(r.total_alerts),
    align: "right",
  },
  {
    key: "overspeed_events",
    header: "Overspeed",
    accessor: (r) => fmtInt(r.overspeed_events),
    align: "right",
  },
  {
    key: "total_harsh_events",
    header: "Harsh events",
    accessor: (r) => fmtInt(r.total_harsh_events),
    align: "right",
  },
  {
    key: "harsh_events_per_100km",
    header: "Harsh / 100km",
    accessor: (r) => fmtDec(r.harsh_events_per_100km),
    align: "right",
  },
];

export function Operations() {
  const { filters } = useFilters();
  const { data, isPending, isError, error } = useQuery<OperationsDashboardResponse>({
    queryKey: ["dashboards", "operations", filters],
    queryFn: ({ signal }) => fetchOperations(filters, signal),
  });

  return (
    <section>
      <PageHeader
        title="Operations"
        caption={
          <>
            Trip volume, harsh events, overspeed hotspots from{" "}
            <code className="rounded bg-slate-200 px-1 py-0.5">marts.v_operational_dashboard</code>.
          </>
        }
      />

      {isPending ? <StateMessage>Loading operations…</StateMessage> : null}
      {isError ? (
        <StateMessage tone="error">
          Failed to load operations: {(error as Error).message}
        </StateMessage>
      ) : null}
      {data ? <Content data={data} /> : null}
    </section>
  );
}

function Content({ data }: { data: OperationsDashboardResponse }) {
  if (data.rows.length === 0) {
    return (
      <StateMessage tone="warning">
        No operational rows in the selected window. Try a wider date range.
      </StateMessage>
    );
  }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <KpiCard label="Trips" value={fmtInt(data.kpi.total_trips)} />
        <KpiCard label="Distance (km)" value={fmtInt(data.kpi.total_distance_km)} />
        <KpiCard label="Alerts" value={fmtInt(data.kpi.total_alerts)} />
        <KpiCard label="Overspeed" value={fmtInt(data.kpi.total_overspeed)} />
      </div>

      <Panel title="Daily trip count">
        <BarChart
          data={data.rows as unknown as Array<Record<string, unknown>>}
          xKey="fleet_date"
          series={[{ dataKey: "total_trips", label: "Trips" }]}
          yFormatter={(v) => fmtInt(v)}
        />
      </Panel>

      <Panel title="Daily harsh-event volume">
        <LineChart
          data={data.rows as unknown as Array<Record<string, unknown>>}
          xKey="fleet_date"
          series={[
            { dataKey: "total_harsh_events", label: "Total harsh events" },
            { dataKey: "harsh_events_per_100km", label: "Harsh / 100km" },
          ]}
          yFormatter={(v) => fmtDec(v)}
        />
      </Panel>

      <Panel title="Raw table" description="One row per tenant × fleet_date, ordered by date.">
        <DataTable
          rows={data.rows}
          columns={ROW_COLUMNS}
          rowKey={(r) => `${r.fleet_date}::${r.tenant_id ?? "all"}`}
        />
      </Panel>
    </div>
  );
}
