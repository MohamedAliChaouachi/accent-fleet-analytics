import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchRisk } from "@/api/dashboards";
import { fetchDeviceProfile } from "@/api/devices";
import type {
  DeviceProfileResponse,
  DeviceRiskRow,
  RiskDashboardResponse,
} from "@/api/types";
import { useFilters } from "@/filters/FiltersContext";
import { KpiCard } from "@/components/KpiCard";
import { Panel } from "@/components/Panel";
import { PageHeader } from "@/components/PageHeader";
import { StateMessage } from "@/components/StateMessage";
import { DataTable, type ColumnDef } from "@/components/DataTable";
import { BarChart } from "@/components/charts/BarChart";
import { LineChart } from "@/components/charts/LineChart";
import { RISK_COLORS, seriesColor } from "@/lib/colors";
import { fmtDec, fmtInt } from "@/lib/format";

const RISK_ORDER = ["low", "moderate", "high", "critical"] as const;

const TOP_DEVICE_COLUMNS: ReadonlyArray<ColumnDef<DeviceRiskRow>> = [
  {
    key: "device_id",
    header: "Device",
    accessor: (r) => r.device_id,
    align: "right",
  },
  {
    key: "tenant_id",
    header: "Tenant",
    accessor: (r) => r.tenant_id ?? "—",
    align: "right",
  },
  { key: "latest_month", header: "Latest month", accessor: (r) => r.latest_month ?? "—" },
  {
    key: "risk_score",
    header: "Risk score",
    accessor: (r) => fmtDec(r.risk_score),
    align: "right",
  },
  {
    key: "risk_category",
    header: "Category",
    accessor: (r) => (
      <span
        className="inline-block rounded px-2 py-0.5 text-xs font-medium text-white"
        style={{ backgroundColor: RISK_COLORS[r.risk_category ?? "low"] ?? "#94a3b8" }}
      >
        {r.risk_category ?? "—"}
      </span>
    ),
  },
  {
    key: "trips_3m",
    header: "Trips (3m)",
    accessor: (r) => fmtInt(r.trips_3m),
    align: "right",
  },
  {
    key: "distance_3m",
    header: "Distance (3m)",
    accessor: (r) => fmtInt(r.distance_3m),
    align: "right",
  },
  {
    key: "overspeed_3m",
    header: "Overspeed (3m)",
    accessor: (r) => fmtInt(r.overspeed_3m),
    align: "right",
  },
];

export function RiskAndBehavior() {
  const { filters } = useFilters();
  const { data, isPending, isError, error } = useQuery<RiskDashboardResponse>({
    queryKey: ["dashboards", "risk", filters],
    queryFn: ({ signal }) => fetchRisk(filters, signal),
  });

  return (
    <section>
      <PageHeader
        title="Risk & behavior"
        caption="Fleet-level + per-device 3-month rolling risk with cluster overlay."
      />

      {isPending ? <StateMessage>Loading risk dashboard…</StateMessage> : null}
      {isError ? (
        <StateMessage tone="error">
          Failed to load risk dashboard: {(error as Error).message}
        </StateMessage>
      ) : null}
      {data ? <Content data={data} /> : null}
    </section>
  );
}

function Content({ data }: { data: RiskDashboardResponse }) {
  // Order risk categories canonically so the chart legend isn't whatever
  // happens to come back from the GROUP BY.
  const orderedCounts = useMemo(() => {
    const map = new Map(data.category_counts.map((c) => [c.category, c.device_count]));
    return RISK_ORDER.filter((cat) => map.has(cat)).map((cat) => ({
      category: cat,
      device_count: map.get(cat) ?? 0,
    }));
  }, [data.category_counts]);

  // Pivot crosstab into Recharts shape: one row per cluster_id, one
  // column per risk_category. Stacking is then automatic.
  const crosstab = useMemo(() => {
    const byCluster = new Map<number, Record<string, number>>();
    for (const row of data.cluster_risk_crosstab) {
      const bucket = byCluster.get(row.cluster_id) ?? {};
      bucket[row.risk_category] = (bucket[row.risk_category] ?? 0) + row.devices;
      byCluster.set(row.cluster_id, bucket);
    }
    return Array.from(byCluster.entries())
      .sort(([a], [b]) => a - b)
      .map(([cluster_id, buckets]) => ({ cluster_id, ...buckets }));
  }, [data.cluster_risk_crosstab]);

  const top20 = data.devices.slice(0, 20);
  const clusterCount = new Set(data.clusters.map((c) => c.cluster_id)).size;

  if (data.devices.length === 0) {
    return (
      <StateMessage tone="warning">
        No risk data for the current filters. Widen the date range or remove the tenant
        filter — the view derives from a 3-month rolling window, so the earliest data must
        be at least 1 month old.
      </StateMessage>
    );
  }

  return (
    <div className="space-y-6">
      <Panel title="Risk category distribution">
        {orderedCounts.length === 0 ? (
          <p className="text-sm text-slate-500">No category counts available.</p>
        ) : (
          <BarChart
            data={orderedCounts as unknown as Array<Record<string, unknown>>}
            xKey="category"
            series={[{ dataKey: "device_count", label: "Devices" }]}
            rowColors={orderedCounts.map((c) => RISK_COLORS[c.category] ?? "#94a3b8")}
            yFormatter={(v) => fmtInt(v)}
            height={240}
          />
        )}
      </Panel>

      <Panel
        title="Top 20 highest-risk devices"
        description="Ordered by risk_score across the filter window."
      >
        <DataTable
          rows={top20}
          columns={TOP_DEVICE_COLUMNS}
          rowKey={(r) => r.device_id}
        />
      </Panel>

      <Panel
        title="Device clusters"
        description="From marts.fact_device_cluster_assignment — populated by the batch scorer at the end of every incremental flow."
      >
        {data.clusters.length === 0 ? (
          <StateMessage>
            No cluster assignments yet. Train the model with{" "}
            <code className="rounded bg-slate-200 px-1 py-0.5">python scripts/train_clustering.py</code>{" "}
            (or wait for the next incremental flow that follows a successful training run).
          </StateMessage>
        ) : (
          <div className="grid grid-cols-1 gap-6 md:grid-cols-3">
            <div className="space-y-3">
              <KpiCard label="Clusters" value={fmtInt(clusterCount)} />
              <KpiCard label="Scored devices" value={fmtInt(data.clusters.length)} />
              {data.cluster_model_versions.length ? (
                <p className="text-xs text-slate-500">
                  Model version(s):{" "}
                  <code className="rounded bg-slate-200 px-1 py-0.5">
                    {data.cluster_model_versions.join(", ")}
                  </code>
                </p>
              ) : null}
            </div>
            <div className="md:col-span-2">
              <BarChart
                data={data.cluster_sizes as unknown as Array<Record<string, unknown>>}
                xKey="cluster_id"
                series={[{ dataKey: "devices", label: "Devices" }]}
                rowColors={data.cluster_sizes.map((_, i) => seriesColor(i))}
                yFormatter={(v) => fmtInt(v)}
                height={260}
              />
            </div>
          </div>
        )}
      </Panel>

      {crosstab.length ? (
        <Panel
          title="Cluster × risk-category crosstab"
          description="Stacked: do clusters track risk categories?"
        >
          <BarChart
            data={crosstab as unknown as Array<Record<string, unknown>>}
            xKey="cluster_id"
            series={RISK_ORDER.map((cat) => ({
              dataKey: cat,
              label: cat,
              color: RISK_COLORS[cat] ?? "#94a3b8",
              stackId: "risk",
            }))}
            yFormatter={(v) => fmtInt(v)}
            legend
            height={300}
          />
        </Panel>
      ) : null}

      <DeviceDrillDown devices={top20} />

      {data.fleet.length ? (
        <Panel title="Fleet risk summary" description="One row per tenant.">
          <DataTable
            rows={data.fleet}
            columns={FLEET_COLUMNS}
            rowKey={(r) => r.tenant_id ?? "all"}
          />
        </Panel>
      ) : null}
    </div>
  );
}

const FLEET_COLUMNS: ReadonlyArray<
  ColumnDef<RiskDashboardResponse["fleet"][number]>
> = [
  { key: "tenant_id", header: "Tenant", accessor: (r) => r.tenant_label ?? r.tenant_id ?? "—" },
  {
    key: "devices_scored",
    header: "Scored",
    accessor: (r) => fmtInt(r.devices_scored),
    align: "right",
  },
  {
    key: "critical",
    header: "Critical",
    accessor: (r) => fmtInt(r.critical_devices),
    align: "right",
  },
  { key: "high", header: "High", accessor: (r) => fmtInt(r.high_devices), align: "right" },
  {
    key: "moderate",
    header: "Moderate",
    accessor: (r) => fmtInt(r.moderate_devices),
    align: "right",
  },
  { key: "low", header: "Low", accessor: (r) => fmtInt(r.low_devices), align: "right" },
  {
    key: "avg_risk_score",
    header: "Avg risk",
    accessor: (r) => fmtDec(r.avg_risk_score),
    align: "right",
  },
  {
    key: "max_risk_score",
    header: "Max risk",
    accessor: (r) => fmtDec(r.max_risk_score),
    align: "right",
  },
];

function DeviceDrillDown({ devices }: { devices: ReadonlyArray<DeviceRiskRow> }) {
  const [deviceId, setDeviceId] = useState<number | null>(devices[0]?.device_id ?? null);

  const { data, isPending, isError, error } = useQuery<DeviceProfileResponse>({
    queryKey: ["device", "profile", deviceId],
    queryFn: ({ signal }) =>
      fetchDeviceProfile(deviceId as number, 12, signal),
    enabled: deviceId !== null,
  });

  return (
    <Panel
      title="Per-device drill-down"
      description="Monthly behavior history for the selected device."
      rightSlot={
        <select
          className="rounded-md border border-slate-300 bg-white px-2 py-1 text-sm"
          value={deviceId ?? ""}
          onChange={(e) => setDeviceId(parseInt(e.target.value, 10))}
        >
          {devices.map((d) => (
            <option key={d.device_id} value={d.device_id}>
              #{d.device_id}
              {d.risk_category ? ` (${d.risk_category})` : ""}
            </option>
          ))}
        </select>
      }
    >
      {deviceId === null ? (
        <p className="text-sm text-slate-500">No device selected.</p>
      ) : isPending ? (
        <StateMessage>Loading device profile…</StateMessage>
      ) : isError ? (
        <StateMessage tone="error">
          Failed to load device: {(error as Error).message}
        </StateMessage>
      ) : !data || data.history.length === 0 ? (
        <StateMessage tone="warning">No monthly history for this device.</StateMessage>
      ) : (
        <div className="space-y-4">
          <LineChart
            data={data.history as unknown as Array<Record<string, unknown>>}
            xKey="year_month"
            series={[
              { dataKey: "overspeed_per_100km", label: "Overspeed / 100km" },
              { dataKey: "night_trip_ratio", label: "Night trip ratio" },
            ]}
            yFormatter={(v) => fmtDec(v)}
          />
          <DataTable
            rows={data.history}
            columns={DRILL_COLUMNS}
            rowKey={(r) => r.year_month}
          />
        </div>
      )}
    </Panel>
  );
}

const DRILL_COLUMNS: ReadonlyArray<
  ColumnDef<DeviceProfileResponse["history"][number]>
> = [
  { key: "year_month", header: "Month", accessor: (r) => r.year_month },
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
    key: "overspeed_per_100km",
    header: "Overspeed / 100km",
    accessor: (r) => fmtDec(r.overspeed_per_100km),
    align: "right",
  },
  {
    key: "high_speed_trip_ratio",
    header: "High-speed ratio",
    accessor: (r) => fmtDec(r.high_speed_trip_ratio),
    align: "right",
  },
  {
    key: "night_trip_ratio",
    header: "Night ratio",
    accessor: (r) => fmtDec(r.night_trip_ratio),
    align: "right",
  },
  {
    key: "avg_max_speed_kmh",
    header: "Avg max speed",
    accessor: (r) => fmtDec(r.avg_max_speed_kmh),
    align: "right",
  },
];
