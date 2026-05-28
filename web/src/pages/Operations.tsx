import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Activity,
  AlertTriangle,
  Gauge,
  Route,
} from "lucide-react";
import { fetchOperations } from "@/api/dashboards";
import type {
  OperationsDailyRow,
  OperationsDashboardResponse,
} from "@/api/types";
import { useFilters } from "@/filters/FiltersContext";
import { PageContainer } from "@/components/shell";
import { Badge, KpiCard, Panel, Skeleton } from "@/components/ui";
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

// Last N daily values of a metric — drives KPI sparklines so the cards
// show movement at a glance without a second API call.
function trail<T>(
  rows: ReadonlyArray<T>,
  pick: (r: T) => number | null | undefined,
  n = 14,
): number[] {
  return rows
    .slice(-n)
    .map(pick)
    .filter((v): v is number => typeof v === "number" && Number.isFinite(v));
}

export function Operations() {
  const { filters } = useFilters();
  const { data, isPending, isError, error } = useQuery<OperationsDashboardResponse>({
    queryKey: ["dashboards", "operations", filters],
    queryFn: ({ signal }) => fetchOperations(filters, signal),
  });

  return (
    <PageContainer
      title="Operations"
      description={
        <>
          Trip volume, harsh events, and overspeed hotspots from{" "}
          <code className="rounded bg-muted px-1.5 py-0.5 font-mono text-2xs text-foreground">
            marts.v_operational_dashboard
          </code>
          .
        </>
      }
      actions={<Badge variant="accent">Daily rollup</Badge>}
    >
      {isPending ? <LoadingSkeleton /> : null}
      {isError ? (
        <StateMessage tone="error">
          Failed to load operations: {(error as Error).message}
        </StateMessage>
      ) : null}
      {data ? <Content data={data} /> : null}
    </PageContainer>
  );
}

function LoadingSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <KpiCard key={i} label="" value="" loading />
        ))}
      </div>
      <Skeleton className="h-72 w-full rounded-lg" />
      <Skeleton className="h-72 w-full rounded-lg" />
    </div>
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

  const tripsTrail = useMemo(
    () => trail(data.rows, (r) => r.total_trips),
    [data.rows],
  );
  const distanceTrail = useMemo(
    () => trail(data.rows, (r) => r.total_distance_km),
    [data.rows],
  );
  const alertsTrail = useMemo(
    () => trail(data.rows, (r) => r.total_alerts),
    [data.rows],
  );
  const overspeedTrail = useMemo(
    () => trail(data.rows, (r) => r.overspeed_events),
    [data.rows],
  );

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <KpiCard
          label="Trips"
          value={fmtInt(data.kpi.total_trips)}
          icon={<Activity />}
          tone="primary"
          sparkline={tripsTrail}
        />
        <KpiCard
          label="Distance (km)"
          value={fmtInt(data.kpi.total_distance_km)}
          icon={<Route />}
          tone="accent"
          sparkline={distanceTrail}
        />
        <KpiCard
          label="Alerts"
          value={fmtInt(data.kpi.total_alerts)}
          icon={<AlertTriangle />}
          tone="warning"
          sparkline={alertsTrail}
        />
        <KpiCard
          label="Overspeed"
          value={fmtInt(data.kpi.total_overspeed)}
          icon={<Gauge />}
          tone="danger"
          sparkline={overspeedTrail}
        />
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <Panel
          title="Daily trip count"
          description="Across all tenants in scope."
          tone="primary"
        >
          <BarChart
            data={data.rows as unknown as Array<Record<string, unknown>>}
            xKey="fleet_date"
            series={[{ dataKey: "total_trips", label: "Trips" }]}
            yFormatter={(v) => fmtInt(v)}
          />
        </Panel>

        <Panel
          title="Daily harsh-event volume"
          description="Total harsh events alongside the rate per 100 km."
          tone="warning"
        >
          <LineChart
            data={data.rows as unknown as Array<Record<string, unknown>>}
            xKey="fleet_date"
            series={[
              { dataKey: "total_harsh_events", label: "Total harsh events" },
              { dataKey: "harsh_events_per_100km", label: "Harsh / 100km" },
            ]}
            yFormatter={(v) => fmtDec(v)}
            legend
          />
        </Panel>
      </div>

      <Panel
        title="Tenant × day detail"
        description="One row per tenant × fleet_date, ordered by date."
        actions={<Badge variant="outline">{data.rows.length} rows</Badge>}
        flush
      >
        <DataTable
          rows={data.rows}
          columns={ROW_COLUMNS}
          rowKey={(r) => `${r.fleet_date}::${r.tenant_id ?? "all"}`}
          maxHeight="32rem"
        />
      </Panel>
    </div>
  );
}
