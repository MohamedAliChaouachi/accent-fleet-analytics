import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchRisk } from "@/api/dashboards";
import { fetchDeviceProfile } from "@/api/devices";
import type {
  ClusterAssignmentRow,
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
import { ClusterBadge } from "@/components/ClusterBadge";
import { RiskBadge } from "@/components/RiskBadge";
import { RISK_COLORS, seriesColor } from "@/lib/colors";
import { clusterPersona } from "@/lib/clusters";
import { fmtDec, fmtInt } from "@/lib/format";

const RISK_ORDER = ["low", "moderate", "high", "critical"] as const;

// --- helpers --------------------------------------------------------------

// Index cluster assignments by device id so the table & drill-down can show
// the current persona without an extra round-trip. We pick the most-recent
// year_month per device to handle multi-month windows.
function buildDeviceClusterMap(
  rows: ReadonlyArray<ClusterAssignmentRow>,
): Map<number, ClusterAssignmentRow> {
  const out = new Map<number, ClusterAssignmentRow>();
  for (const r of rows) {
    const prev = out.get(r.device_id);
    if (!prev) {
      out.set(r.device_id, r);
      continue;
    }
    if ((r.year_month ?? "") > (prev.year_month ?? "")) out.set(r.device_id, r);
  }
  return out;
}

// --- page -----------------------------------------------------------------

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

      {isPending ? <StateMessage>Loading risk dashboard\u2026</StateMessage> : null}
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
  const deviceClusterMap = useMemo(
    () => buildDeviceClusterMap(data.clusters),
    [data.clusters],
  );

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
      .map(([cluster_id, buckets]) => ({
        cluster_id,
        cluster_name: clusterPersona(cluster_id).short,
        ...buckets,
      }));
  }, [data.cluster_risk_crosstab]);

  const top20 = data.devices.slice(0, 20);
  const clusterCount = new Set(data.clusters.map((c) => c.cluster_id)).size;
  const totalDevices = data.devices.length;
  const criticalCount = orderedCounts.find((c) => c.category === "critical")?.device_count ?? 0;
  const highCount = orderedCounts.find((c) => c.category === "high")?.device_count ?? 0;

  if (data.devices.length === 0) {
    return (
      <StateMessage tone="warning">
        No risk data for the current filters. Widen the date range or remove the tenant
        filter \u2014 the view derives from a 3-month rolling window, so the earliest data
        must be at least 1 month old.
      </StateMessage>
    );
  }

  return (
    <div className="space-y-6">
      {/* High-signal KPI strip --------------------------------------------- */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KpiCard
          label="Scored devices"
          value={fmtInt(totalDevices)}
          accent="#1f3a5f"
        />
        <KpiCard
          label="Critical + high"
          value={fmtInt(criticalCount + highCount)}
          hint={`${fmtInt(criticalCount)} critical \u00b7 ${fmtInt(highCount)} high`}
          accent={RISK_COLORS.high}
        />
        <KpiCard
          label="Behaviour groups"
          value={fmtInt(clusterCount)}
          hint="K-Means personas"
          accent="#9b59b6"
        />
        <KpiCard
          label="Model version"
          value={data.cluster_model_versions[0] ?? "\u2014"}
          hint={
            data.cluster_model_versions.length > 1
              ? `+${data.cluster_model_versions.length - 1} more`
              : "current clustering model"
          }
          accent="#2a9df4"
        />
      </div>

      <Panel title="Risk category distribution" accent={RISK_COLORS.critical}>
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
        title="Behaviour groups (clusters)"
        description="Each persona is derived from the K-Means model on monthly driving features. Hover a chip for the description."
        accent="#9b59b6"
      >
        {data.clusters.length === 0 ? (
          <StateMessage>
            No cluster assignments yet. Train the model with{" "}
            <code className="rounded bg-slate-200 px-1 py-0.5">
              python scripts/train_clustering.py
            </code>{" "}
            (or wait for the next incremental flow that follows a successful training run).
          </StateMessage>
        ) : (
          <ClusterPersonaGrid sizes={data.cluster_sizes} />
        )}
      </Panel>

      <Panel
        title="Top 20 highest-risk devices"
        description="Ordered by risk_score across the filter window. Click a row to drill down."
        accent={RISK_COLORS.high}
      >
        <DataTable
          rows={top20}
          columns={makeTopDeviceColumns(deviceClusterMap)}
          rowKey={(r) => r.device_id}
        />
      </Panel>

      {crosstab.length ? (
        <Panel
          title="Cluster \u00d7 risk-category crosstab"
          description="Stacked: do behaviour groups track risk categories?"
          accent="#9b59b6"
        >
          <BarChart
            data={crosstab as unknown as Array<Record<string, unknown>>}
            xKey="cluster_name"
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

      <DeviceDrillDown devices={data.devices} clusterMap={deviceClusterMap} />

      {data.fleet.length ? (
        <Panel title="Fleet risk summary" description="One row per tenant." accent="#1f3a5f">
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

// --- cluster persona grid --------------------------------------------------

function ClusterPersonaGrid({
  sizes,
}: {
  sizes: ReadonlyArray<{ cluster_id: number; devices: number }>;
}) {
  if (sizes.length === 0) {
    return <p className="text-sm text-slate-500">No cluster assignments.</p>;
  }
  const total = sizes.reduce((s, c) => s + c.devices, 0);
  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {sizes.map((c, i) => {
        const persona = clusterPersona(c.cluster_id);
        const pct = total > 0 ? (c.devices / total) * 100 : 0;
        return (
          <div
            key={c.cluster_id}
            className="group relative overflow-hidden rounded-xl border border-slate-200 bg-white p-4 transition hover:-translate-y-0.5 hover:shadow-md"
            style={{
              background: `linear-gradient(180deg, ${persona.color}0d 0%, #ffffff 70%)`,
            }}
          >
            <div className="flex items-center gap-3">
              <span
                className="flex h-10 w-10 items-center justify-center rounded-lg text-lg"
                style={{ backgroundColor: `${persona.color}1f`, color: persona.color }}
                aria-hidden
              >
                {persona.icon}
              </span>
              <div className="min-w-0 flex-1">
                <p className="truncate text-sm font-semibold text-slate-900">
                  {persona.name}
                </p>
                <p className="text-[11px] uppercase tracking-wider text-slate-500">
                  Cluster #{c.cluster_id} \u00b7 {fmtInt(c.devices)} devices ({pct.toFixed(0)}%)
                </p>
              </div>
            </div>
            <p className="mt-3 line-clamp-2 text-xs leading-snug text-slate-600">
              {persona.description}
            </p>
            <div className="mt-3 h-1.5 w-full rounded-full bg-slate-100">
              <div
                className="h-1.5 rounded-full"
                style={{
                  width: `${pct}%`,
                  backgroundColor: persona.color,
                  boxShadow: `0 0 0 1px ${persona.color}33`,
                }}
              />
            </div>
            <p className="mt-2 text-[11px] text-slate-500">
              <span className="font-medium text-slate-600">Action:</span> {persona.recommendation}
            </p>
            <span
              aria-hidden
              className="pointer-events-none absolute -right-6 -top-6 h-16 w-16 rounded-full opacity-0 transition group-hover:opacity-100"
              style={{ backgroundColor: `${persona.color}1a` }}
            />
            {/* swatch so the bar in the chart and this card visibly agree */}
            <span
              aria-hidden
              className="absolute right-3 top-3 inline-block h-2 w-2 rounded-full"
              style={{ backgroundColor: seriesColor(i) }}
              title="Chart series color"
            />
          </div>
        );
      })}
    </div>
  );
}

// --- top-device table columns (factory because it needs the cluster map) ---

function makeTopDeviceColumns(
  clusterMap: Map<number, ClusterAssignmentRow>,
): ReadonlyArray<ColumnDef<DeviceRiskRow>> {
  return [
    {
      key: "device_id",
      header: "Device",
      accessor: (r) => <span className="font-mono text-slate-800">#{r.device_id}</span>,
      align: "right",
    },
    {
      key: "tenant_id",
      header: "Tenant",
      accessor: (r) => r.tenant_id ?? "\u2014",
      align: "right",
    },
    { key: "latest_month", header: "Latest month", accessor: (r) => r.latest_month ?? "\u2014" },
    {
      key: "risk_score",
      header: "Risk",
      accessor: (r) => (
        <RiskScoreCell score={r.risk_score} category={r.risk_category} />
      ),
      align: "right",
    },
    {
      key: "risk_category",
      header: "Category",
      accessor: (r) => <RiskBadge category={r.risk_category} />,
    },
    {
      key: "cluster",
      header: "Behaviour",
      accessor: (r) => <ClusterBadge clusterId={clusterMap.get(r.device_id)?.cluster_id} />,
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
}

// Inline mini-bar so the score reads at-a-glance, not just as a number.
function RiskScoreCell({
  score,
  category,
}: {
  score: number | null;
  category: DeviceRiskRow["risk_category"];
}) {
  if (score === null || score === undefined)
    return <span className="text-slate-400">\u2014</span>;
  const pct = Math.max(0, Math.min(100, score));
  const color = RISK_COLORS[category ?? "low"] ?? "#94a3b8";
  return (
    <div className="flex items-center justify-end gap-2">
      <div className="h-1.5 w-16 rounded-full bg-slate-100">
        <div
          className="h-1.5 rounded-full"
          style={{ width: `${pct}%`, backgroundColor: color }}
        />
      </div>
      <span className="w-10 text-right font-semibold tabular-nums text-slate-800">
        {fmtDec(score)}
      </span>
    </div>
  );
}

const FLEET_COLUMNS: ReadonlyArray<
  ColumnDef<RiskDashboardResponse["fleet"][number]>
> = [
  { key: "tenant_id", header: "Tenant", accessor: (r) => r.tenant_label ?? r.tenant_id ?? "\u2014" },
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

// --- device drill-down ----------------------------------------------------

function DeviceDrillDown({
  devices,
  clusterMap,
}: {
  devices: ReadonlyArray<DeviceRiskRow>;
  clusterMap: Map<number, ClusterAssignmentRow>;
}) {
  const [deviceId, setDeviceId] = useState<number | null>(devices[0]?.device_id ?? null);
  const [search, setSearch] = useState("");

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return devices;
    return devices.filter((d) => {
      if (String(d.device_id).includes(q)) return true;
      if ((d.risk_category ?? "").toLowerCase().includes(q)) return true;
      const persona = clusterPersona(clusterMap.get(d.device_id)?.cluster_id);
      return persona.name.toLowerCase().includes(q);
    });
  }, [devices, search, clusterMap]);

  const selected = devices.find((d) => d.device_id === deviceId) ?? null;
  const selectedCluster = deviceId !== null ? clusterMap.get(deviceId) : undefined;

  const { data, isPending, isError, error } = useQuery<DeviceProfileResponse>({
    queryKey: ["device", "profile", deviceId],
    queryFn: ({ signal }) => fetchDeviceProfile(deviceId as number, 12, signal),
    enabled: deviceId !== null,
  });

  return (
    <Panel
      title="Per-device drill-down"
      description="Pick a device on the left. The card on the right summarises its current risk + behaviour group, with monthly history below."
      accent="#2a9df4"
    >
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-[280px,1fr]">
        {/* Device picker -------------------------------------------------- */}
        <div className="rounded-lg border border-slate-200 bg-slate-50/50">
          <div className="border-b border-slate-200 p-2">
            <label className="relative block">
              <span className="sr-only">Search devices</span>
              <input
                type="search"
                placeholder="Search device id, risk, persona\u2026"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="w-full rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm placeholder:text-slate-400 focus:border-sky-500 focus:outline-none focus:ring-1 focus:ring-sky-500"
              />
            </label>
            <p className="mt-1 px-1 text-[11px] text-slate-500">
              {filtered.length} of {devices.length} devices
            </p>
          </div>
          <ul className="max-h-[420px] overflow-y-auto">
            {filtered.length === 0 ? (
              <li className="p-3 text-sm text-slate-500">No matches.</li>
            ) : (
              filtered.map((d) => {
                const isActive = d.device_id === deviceId;
                const cluster = clusterMap.get(d.device_id);
                return (
                  <li key={d.device_id}>
                    <button
                      type="button"
                      onClick={() => setDeviceId(d.device_id)}
                      className={`flex w-full items-center justify-between gap-2 border-l-2 px-3 py-2 text-left text-sm transition ${
                        isActive
                          ? "border-sky-500 bg-white"
                          : "border-transparent hover:bg-white"
                      }`}
                    >
                      <div className="min-w-0">
                        <p className="truncate font-mono text-slate-800">
                          #{d.device_id}
                        </p>
                        <p className="truncate text-[11px] text-slate-500">
                          {d.latest_month ?? "\u2014"} \u00b7 score {fmtDec(d.risk_score)}
                        </p>
                      </div>
                      <div className="flex flex-shrink-0 items-center gap-1">
                        <RiskBadge category={d.risk_category} />
                        <ClusterBadge clusterId={cluster?.cluster_id} showName={false} />
                      </div>
                    </button>
                  </li>
                );
              })
            )}
          </ul>
        </div>

        {/* Detail pane ---------------------------------------------------- */}
        <div className="space-y-4">
          {deviceId === null ? (
            <p className="text-sm text-slate-500">Select a device to see details.</p>
          ) : (
            <>
              <DeviceHeroCard
                device={selected}
                cluster={selectedCluster}
                latestHistory={data?.history.at(-1) ?? null}
              />
              {isPending ? (
                <StateMessage>Loading device profile\u2026</StateMessage>
              ) : isError ? (
                <StateMessage tone="error">
                  Failed to load device: {(error as Error).message}
                </StateMessage>
              ) : !data || data.history.length === 0 ? (
                <StateMessage tone="warning">No monthly history for this device.</StateMessage>
              ) : (
                <>
                  <div className="rounded-lg border border-slate-200 bg-white p-3">
                    <p className="mb-2 px-1 text-[11px] font-medium uppercase tracking-wider text-slate-500">
                      Monthly behaviour trend
                    </p>
                    <LineChart
                      data={data.history as unknown as Array<Record<string, unknown>>}
                      xKey="year_month"
                      series={[
                        { dataKey: "overspeed_per_100km", label: "Overspeed / 100km" },
                        { dataKey: "night_trip_ratio", label: "Night trip ratio" },
                      ]}
                      yFormatter={(v) => fmtDec(v)}
                    />
                  </div>
                  <DataTable
                    rows={data.history}
                    columns={DRILL_COLUMNS}
                    rowKey={(r) => r.year_month}
                  />
                </>
              )}
            </>
          )}
        </div>
      </div>
    </Panel>
  );
}

// Hero card that surfaces the selected device's current risk + cluster +
// suggested action. This is the single piece the dashboard was missing.
function DeviceHeroCard({
  device,
  cluster,
  latestHistory,
}: {
  device: DeviceRiskRow | null;
  cluster: ClusterAssignmentRow | undefined;
  latestHistory: DeviceProfileResponse["history"][number] | null;
}) {
  if (!device) return null;
  const persona = clusterPersona(cluster?.cluster_id);
  const riskColor = RISK_COLORS[device.risk_category ?? "low"] ?? "#94a3b8";
  const score = device.risk_score ?? 0;
  const scorePct = Math.max(0, Math.min(100, score));

  // Build a short list of "why" — features that look unusual. These are the
  // same fields the risk score weights, so it doubles as a hint at what's
  // pushing the score up.
  const drivers = latestHistory
    ? [
        { label: "Overspeed / 100km", value: latestHistory.overspeed_per_100km },
        { label: "High-speed trip ratio", value: latestHistory.high_speed_trip_ratio },
        { label: "Night trip ratio", value: latestHistory.night_trip_ratio },
        { label: "Avg max speed (km/h)", value: latestHistory.avg_max_speed_kmh },
      ].filter((d) => d.value !== null && d.value !== undefined)
    : [];

  return (
    <div
      className="relative overflow-hidden rounded-xl border border-slate-200 bg-white p-5 shadow-sm"
      style={{
        background: `linear-gradient(135deg, ${riskColor}10 0%, ${persona.color}10 100%), #ffffff`,
      }}
    >
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-[11px] font-medium uppercase tracking-wider text-slate-500">
            Device
          </p>
          <p className="mt-0.5 font-mono text-2xl font-semibold text-slate-900">
            #{device.device_id}
          </p>
          <p className="mt-1 text-xs text-slate-500">
            Latest month: <span className="font-medium text-slate-700">{device.latest_month ?? "\u2014"}</span>
            {cluster?.model_version ? (
              <>
                {" \u00b7 "}model{" "}
                <code className="rounded bg-slate-100 px-1 py-0.5 text-[10px]">
                  {cluster.model_version}
                </code>
              </>
            ) : null}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <RiskBadge category={device.risk_category} size="lg" />
          <ClusterBadge clusterId={cluster?.cluster_id} size="lg" />
        </div>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-3">
        {/* Risk gauge ---------------------------------------------------- */}
        <div className="rounded-lg border border-slate-200 bg-white p-3">
          <p className="text-[11px] font-medium uppercase tracking-wider text-slate-500">
            Risk score
          </p>
          <div className="mt-1 flex items-baseline gap-2">
            <span className="text-3xl font-semibold tabular-nums text-slate-900">
              {fmtDec(device.risk_score)}
            </span>
            <span className="text-xs text-slate-400">/ 100</span>
          </div>
          <div className="mt-2 h-2 w-full rounded-full bg-slate-100">
            <div
              className="h-2 rounded-full"
              style={{ width: `${scorePct}%`, backgroundColor: riskColor }}
            />
          </div>
          <div className="mt-2 flex justify-between text-[10px] uppercase tracking-wider text-slate-400">
            <span>low</span>
            <span>moderate</span>
            <span>high</span>
            <span>critical</span>
          </div>
        </div>

        {/* Cluster persona ---------------------------------------------- */}
        <div
          className="rounded-lg border border-slate-200 p-3"
          style={{ backgroundColor: `${persona.color}0a` }}
        >
          <p className="text-[11px] font-medium uppercase tracking-wider text-slate-500">
            Behaviour group
          </p>
          <div className="mt-1 flex items-center gap-2">
            <span
              className="flex h-8 w-8 items-center justify-center rounded-md text-base"
              style={{ backgroundColor: `${persona.color}1f`, color: persona.color }}
              aria-hidden
            >
              {persona.icon}
            </span>
            <p className="text-base font-semibold text-slate-900">{persona.name}</p>
          </div>
          <p className="mt-2 text-xs leading-snug text-slate-600">{persona.description}</p>
        </div>

        {/* Recommended action ------------------------------------------- */}
        <div className="rounded-lg border border-slate-200 bg-white p-3">
          <p className="text-[11px] font-medium uppercase tracking-wider text-slate-500">
            Recommended action
          </p>
          <p className="mt-1 text-sm font-medium text-slate-900">{persona.recommendation}</p>
          {drivers.length ? (
            <ul className="mt-2 space-y-0.5 text-xs text-slate-600">
              {drivers.slice(0, 3).map((d) => (
                <li key={d.label} className="flex justify-between gap-2">
                  <span className="truncate text-slate-500">{d.label}</span>
                  <span className="font-mono tabular-nums text-slate-800">
                    {fmtDec(d.value)}
                  </span>
                </li>
              ))}
            </ul>
          ) : null}
        </div>
      </div>
    </div>
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
