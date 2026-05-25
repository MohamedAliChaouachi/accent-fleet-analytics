import { useMemo, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { fetchPredictiveAlerts } from "@/api/dashboards";
import type {
  LiveAlertRow,
  PredictiveAlertRow,
  PredictiveAlertsDashboardResponse,
} from "@/api/types";
import { useFilters } from "@/filters/FiltersContext";
import { KpiCard } from "@/components/KpiCard";
import { Panel } from "@/components/Panel";
import { PageHeader } from "@/components/PageHeader";
import { StateMessage } from "@/components/StateMessage";
import { DataTable, type ColumnDef } from "@/components/DataTable";
import { BarChart } from "@/components/charts/BarChart";
import { PieChart } from "@/components/charts/PieChart";
import { RISK_COLORS } from "@/lib/colors";
import { fmtDec, fmtInt } from "@/lib/format";

const SEVERITY_ICON: Record<string, string> = {
  critical: "🔴",
  high: "🟠",
  medium: "🟡",
  low: "🟢",
};
const SEVERITY_OPTIONS = ["critical", "high", "medium", "low"] as const;
const ALERT_TYPE_OPTIONS = [
  "high_risk_prediction",
  "maintenance_due",
  "fuel_anomaly",
  "inactive_device",
  "cluster_migration",
  "harsh_event_spike",
] as const;

function fmtDateTime(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toISOString().slice(0, 16).replace("T", " ");
}

export function PredictiveAlerts() {
  const { filters } = useFilters();
  const queryClient = useQueryClient();
  const [tab, setTab] = useState<"alerts" | "stream">("alerts");
  const [sevFilter, setSevFilter] = useState<Set<string>>(new Set(SEVERITY_OPTIONS));
  const [typeFilter, setTypeFilter] = useState<Set<string>>(new Set(ALERT_TYPE_OPTIONS));

  const { data, isPending, isError, error } = useQuery<PredictiveAlertsDashboardResponse>({
    queryKey: ["dashboards", "predictive-alerts", filters],
    queryFn: ({ signal }) => fetchPredictiveAlerts(filters, signal),
  });

  function onRefresh() {
    queryClient.invalidateQueries({ queryKey: ["dashboards", "predictive-alerts"] });
  }

  return (
    <section>
      <PageHeader
        title="Predictive alerts"
        caption="Proactive alerts + last-24h live stream. Auto-refreshes via the TanStack Query staleTime (5 min); use the button for a manual refresh."
      />

      <div className="mb-4 flex items-center gap-3">
        <button
          type="button"
          onClick={onRefresh}
          className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:border-slate-400 hover:bg-slate-50"
        >
          ↻ Refresh now
        </button>
        <div className="ml-auto inline-flex rounded-md border border-slate-200 bg-white p-0.5 text-xs">
          {(["alerts", "stream"] as const).map((t) => (
            <button
              key={t}
              type="button"
              onClick={() => setTab(t)}
              className={`rounded px-3 py-1 transition ${
                tab === t ? "bg-brand text-white" : "text-slate-600 hover:bg-slate-100"
              }`}
            >
              {t === "alerts" ? "Active alerts (proactive)" : "Live stream (last 24h)"}
            </button>
          ))}
        </div>
      </div>

      {isPending ? <StateMessage>Loading alerts…</StateMessage> : null}
      {isError ? (
        <StateMessage tone="error">
          Failed to load alerts: {(error as Error).message}
        </StateMessage>
      ) : null}

      {data && tab === "alerts" ? (
        <ProactiveTab
          data={data}
          sevFilter={sevFilter}
          setSevFilter={setSevFilter}
          typeFilter={typeFilter}
          setTypeFilter={setTypeFilter}
        />
      ) : null}
      {data && tab === "stream" ? <StreamTab data={data} /> : null}
    </section>
  );
}

interface ProactiveProps {
  data: PredictiveAlertsDashboardResponse;
  sevFilter: Set<string>;
  setSevFilter: (s: Set<string>) => void;
  typeFilter: Set<string>;
  setTypeFilter: (s: Set<string>) => void;
}

function ProactiveTab({
  data,
  sevFilter,
  setSevFilter,
  typeFilter,
  setTypeFilter,
}: ProactiveProps) {
  const filtered = useMemo(
    () =>
      data.alerts.filter(
        (a) => sevFilter.has(a.severity) && typeFilter.has(a.alert_type),
      ),
    [data.alerts, sevFilter, typeFilter],
  );
  const summary = data.alerts_summary;

  if (data.alerts.length === 0) {
    return (
      <StateMessage tone="info">
        No active alerts — system is healthy. (If you expected alerts, check the
        ingestion pipeline and verify the v_predictive_alerts_dashboard view exists.)
      </StateMessage>
    );
  }

  return (
    <div className="space-y-6">
      {/* Filters ------------------------------------------------------- */}
      <Panel title="Filters" padding="sm">
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          <FilterChips
            label="Severity"
            options={[...SEVERITY_OPTIONS]}
            selected={sevFilter}
            onChange={setSevFilter}
            renderLabel={(s) => `${SEVERITY_ICON[s] ?? ""} ${s}`}
          />
          <FilterChips
            label="Alert type"
            options={[...ALERT_TYPE_OPTIONS]}
            selected={typeFilter}
            onChange={setTypeFilter}
          />
        </div>
      </Panel>

      {/* KPIs ---------------------------------------------------------- */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KpiCard label="Active alerts" value={fmtInt(summary.total)} accent="#1f3a5f" />
        <KpiCard
          label="High / critical"
          value={fmtInt(summary.high_or_critical)}
          accent={RISK_COLORS.critical}
        />
        <KpiCard
          label="Detected last 24h"
          value={fmtInt(summary.last_24h)}
          accent={RISK_COLORS.high}
        />
        <KpiCard
          label="Unique devices"
          value={fmtInt(summary.unique_devices)}
          accent="#2a9df4"
        />
      </div>

      {/* Alert table -------------------------------------------------- */}
      <Panel
        title="Active alerts"
        description={`${filtered.length} after filters · ${data.alerts.length} total`}
      >
        <DataTable
          rows={filtered.slice(0, 200)}
          columns={PROACTIVE_COLUMNS}
          rowKey={(r) => r.alert_id}
        />
        {filtered.length > 200 ? (
          <p className="mt-2 text-xs text-slate-500">
            Showing first 200 of {filtered.length} alerts. Refine filters to narrow.
          </p>
        ) : null}
      </Panel>

      {/* Analytics breakdowns ----------------------------------------- */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Panel title="By type" accent="#9b59b6">
          {summary.by_type.length === 0 ? (
            <p className="text-sm text-slate-500">No alerts.</p>
          ) : (
            <PieChart
              data={summary.by_type.map((b) => ({ name: b.key, value: b.count }))}
            />
          )}
        </Panel>
        <Panel title="By severity" accent={RISK_COLORS.critical}>
          {summary.by_severity.length === 0 ? (
            <p className="text-sm text-slate-500">No alerts.</p>
          ) : (
            <BarChart
              data={
                summary.by_severity as unknown as Array<Record<string, unknown>>
              }
              xKey="key"
              series={[{ dataKey: "count", label: "Count" }]}
              rowColors={summary.by_severity.map(
                (b) => RISK_COLORS[b.key] ?? "#94a3b8",
              )}
              yFormatter={(v) => fmtInt(v)}
            />
          )}
        </Panel>
      </div>
    </div>
  );
}

function StreamTab({ data }: { data: PredictiveAlertsDashboardResponse }) {
  const summary = data.stream_summary;
  if (data.stream.length === 0) {
    return (
      <StateMessage tone="info">
        Nothing in the last 24 hours — quiet fleet.
      </StateMessage>
    );
  }
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KpiCard
          label="Events (last 24h)"
          value={fmtInt(summary.total)}
          accent="#1f3a5f"
        />
        <KpiCard
          label="Critical / high"
          value={fmtInt(summary.high_or_critical)}
          accent={RISK_COLORS.critical}
        />
        <KpiCard
          label="Avg priority"
          value={fmtDec(summary.avg_priority)}
          accent={RISK_COLORS.high}
        />
        <KpiCard
          label="Unique devices"
          value={fmtInt(summary.unique_devices)}
          accent="#2a9df4"
        />
      </div>

      <Panel title="Live events" description="Top 200 by priority score.">
        <DataTable
          rows={data.stream.slice(0, 200)}
          columns={STREAM_COLUMNS}
          rowKey={(r) => r.alert_id}
        />
      </Panel>

      <Panel title="Events by category" accent="#9b59b6">
        {summary.by_category.length === 0 ? (
          <p className="text-sm text-slate-500">No events.</p>
        ) : (
          <BarChart
            data={summary.by_category as unknown as Array<Record<string, unknown>>}
            xKey="key"
            series={[{ dataKey: "count", label: "Events" }]}
            yFormatter={(v) => fmtInt(v)}
          />
        )}
      </Panel>
    </div>
  );
}

// --- shared chip filter ---------------------------------------------------
interface FilterChipsProps {
  label: string;
  options: ReadonlyArray<string>;
  selected: Set<string>;
  onChange: (next: Set<string>) => void;
  renderLabel?: (opt: string) => string;
}
function FilterChips({
  label,
  options,
  selected,
  onChange,
  renderLabel,
}: FilterChipsProps) {
  function toggle(opt: string) {
    const next = new Set(selected);
    if (next.has(opt)) next.delete(opt);
    else next.add(opt);
    onChange(next);
  }
  return (
    <div>
      <p className="mb-2 text-[11px] font-medium uppercase tracking-wider text-slate-500">
        {label}
      </p>
      <div className="flex flex-wrap gap-1.5">
        {options.map((o) => {
          const active = selected.has(o);
          return (
            <button
              key={o}
              type="button"
              onClick={() => toggle(o)}
              className={`rounded-full border px-3 py-1 text-xs transition ${
                active
                  ? "border-sky-500 bg-sky-50 text-sky-700"
                  : "border-slate-200 bg-white text-slate-600 hover:border-slate-300"
              }`}
            >
              {renderLabel ? renderLabel(o) : o}
            </button>
          );
        })}
      </div>
    </div>
  );
}

// --- columns --------------------------------------------------------------

function SeverityCell({ severity }: { severity: string }) {
  const color = RISK_COLORS[severity] ?? "#94a3b8";
  return (
    <span
      className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium"
      style={{ backgroundColor: `${color}1a`, color }}
    >
      <span>{SEVERITY_ICON[severity] ?? "⚪"}</span>
      {severity}
    </span>
  );
}

const PROACTIVE_COLUMNS: ReadonlyArray<ColumnDef<PredictiveAlertRow>> = [
  { key: "severity", header: "Severity", accessor: (r) => <SeverityCell severity={r.severity} /> },
  { key: "alert_type", header: "Type", accessor: (r) => r.alert_type },
  { key: "tenant_id", header: "Tenant", accessor: (r) => r.tenant_id ?? "—", align: "right" },
  {
    key: "device_id",
    header: "Device",
    accessor: (r) => (
      <span className="font-mono text-slate-800">{r.device_id ? `#${r.device_id}` : "—"}</span>
    ),
    align: "right",
  },
  {
    key: "detected_at",
    header: "Detected",
    accessor: (r) => fmtDateTime(r.detected_at),
  },
  {
    key: "alert_message",
    header: "Message",
    accessor: (r) => <span className="text-slate-700">{r.alert_message ?? "—"}</span>,
  },
  { key: "assigned_to", header: "Assignee", accessor: (r) => r.assigned_to ?? "—" },
];

const STREAM_COLUMNS: ReadonlyArray<ColumnDef<LiveAlertRow>> = [
  { key: "severity", header: "Severity", accessor: (r) => <SeverityCell severity={r.severity} /> },
  {
    key: "priority_score",
    header: "Priority",
    accessor: (r) => <span className="font-semibold tabular-nums">{fmtDec(r.priority_score)}</span>,
    align: "right",
  },
  {
    key: "detected_at",
    header: "Detected",
    accessor: (r) => fmtDateTime(r.detected_at),
  },
  { key: "alert_category", header: "Category", accessor: (r) => r.alert_category },
  { key: "tenant_id", header: "Tenant", accessor: (r) => r.tenant_id ?? "—", align: "right" },
  {
    key: "device_id",
    header: "Device",
    accessor: (r) => (
      <span className="font-mono text-slate-800">{r.device_id ? `#${r.device_id}` : "—"}</span>
    ),
    align: "right",
  },
  {
    key: "alert_message",
    header: "Message",
    accessor: (r) => <span className="text-slate-700">{r.alert_message ?? "—"}</span>,
  },
  {
    key: "device_risk_category",
    header: "Device risk",
    accessor: (r) => r.device_risk_category ?? "—",
  },
];
