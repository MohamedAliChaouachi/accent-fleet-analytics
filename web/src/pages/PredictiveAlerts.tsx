import { useMemo, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AlertOctagon,
  BellRing,
  Clock3,
  Layers,
  RefreshCw,
} from "lucide-react";
import { fetchPredictiveAlerts } from "@/api/dashboards";
import type {
  LiveAlertRow,
  PredictiveAlertRow,
  PredictiveAlertsDashboardResponse,
} from "@/api/types";
import { useFilters } from "@/filters/FiltersContext";
import { PageContainer } from "@/components/shell";
import { Badge, Button, KpiCard, Panel, Skeleton } from "@/components/ui";
import { StateMessage } from "@/components/StateMessage";
import { DataTable, type ColumnDef } from "@/components/DataTable";
import { BarChart } from "@/components/charts/BarChart";
import { PieChart } from "@/components/charts/PieChart";
import { cn } from "@/lib/cn";
import { RISK_COLORS } from "@/lib/colors";
import { fmtDec, fmtInt } from "@/lib/format";

const SEVERITY_ICON: Record<string, string> = {
  critical: "●",
  high: "●",
  medium: "●",
  low: "●",
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
    <PageContainer
      title="Predictive alerts"
      description="Proactive alerts plus a last-24h live stream. Auto-refreshes every 5 minutes; tap refresh for a manual pull."
      actions={
        <div className="flex items-center gap-2">
          <Badge variant="ai">AI-driven</Badge>
          <Button variant="outline" size="sm" onClick={onRefresh}>
            <RefreshCw className="size-3.5" />
            Refresh
          </Button>
        </div>
      }
    >
      <div className="mb-4 inline-flex rounded-md border border-border bg-card p-0.5 text-xs">
        {(["alerts", "stream"] as const).map((t) => (
          <button
            key={t}
            type="button"
            onClick={() => setTab(t)}
            className={cn(
              "rounded px-3 py-1.5 font-medium transition-colors",
              tab === t
                ? "bg-primary text-primary-foreground"
                : "text-muted-foreground hover:bg-secondary/60 hover:text-foreground",
            )}
          >
            {t === "alerts" ? "Active alerts (proactive)" : "Live stream (last 24h)"}
          </button>
        ))}
      </div>

      {isPending ? <LoadingSkeleton /> : null}
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
      <Panel title="Filters" tone="default">
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          <FilterChips
            label="Severity"
            options={[...SEVERITY_OPTIONS]}
            selected={sevFilter}
            onChange={setSevFilter}
          />
          <FilterChips
            label="Alert type"
            options={[...ALERT_TYPE_OPTIONS]}
            selected={typeFilter}
            onChange={setTypeFilter}
          />
        </div>
      </Panel>

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KpiCard
          label="Active alerts"
          value={fmtInt(summary.total)}
          icon={<BellRing />}
          tone="primary"
        />
        <KpiCard
          label="High / critical"
          value={fmtInt(summary.high_or_critical)}
          icon={<AlertOctagon />}
          tone="danger"
        />
        <KpiCard
          label="Detected last 24h"
          value={fmtInt(summary.last_24h)}
          icon={<Clock3 />}
          tone="warning"
        />
        <KpiCard
          label="Unique devices"
          value={fmtInt(summary.unique_devices)}
          icon={<Layers />}
          tone="accent"
        />
      </div>

      <Panel
        title="Active alerts"
        description={`${filtered.length} after filters · ${data.alerts.length} total`}
        actions={<Badge variant="outline">{filtered.length}</Badge>}
        flush
      >
        <DataTable
          rows={filtered.slice(0, 200)}
          columns={PROACTIVE_COLUMNS}
          rowKey={(r) => r.alert_id}
          maxHeight="36rem"
        />
        {filtered.length > 200 ? (
          <p className="px-4 pb-3 pt-2 text-xs text-muted-foreground">
            Showing first 200 of {filtered.length} alerts. Refine filters to narrow.
          </p>
        ) : null}
      </Panel>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <Panel title="By type" description="Share of alerts by category." tone="ai">
          {summary.by_type.length === 0 ? (
            <p className="text-sm text-muted-foreground">No alerts.</p>
          ) : (
            <PieChart
              data={summary.by_type.map((b) => ({ name: b.key, value: b.count }))}
              donut
            />
          )}
        </Panel>
        <Panel
          title="By severity"
          description="Counts grouped by severity tier."
          tone="warning"
        >
          {summary.by_severity.length === 0 ? (
            <p className="text-sm text-muted-foreground">No alerts.</p>
          ) : (
            <BarChart
              data={
                summary.by_severity as unknown as Array<Record<string, unknown>>
              }
              xKey="key"
              series={[{ dataKey: "count", label: "Count" }]}
              rowColors={summary.by_severity.map(
                (b) => RISK_COLORS[b.key] ?? RISK_COLORS.low,
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
          icon={<BellRing />}
          tone="primary"
        />
        <KpiCard
          label="Critical / high"
          value={fmtInt(summary.high_or_critical)}
          icon={<AlertOctagon />}
          tone="danger"
        />
        <KpiCard
          label="Avg priority"
          value={fmtDec(summary.avg_priority)}
          icon={<Clock3 />}
          tone="warning"
        />
        <KpiCard
          label="Unique devices"
          value={fmtInt(summary.unique_devices)}
          icon={<Layers />}
          tone="accent"
        />
      </div>

      <Panel
        title="Live events"
        description="Top 200 by priority score."
        actions={<Badge variant="outline">{Math.min(data.stream.length, 200)}</Badge>}
        flush
      >
        <DataTable
          rows={data.stream.slice(0, 200)}
          columns={STREAM_COLUMNS}
          rowKey={(r) => r.alert_id}
          maxHeight="36rem"
        />
      </Panel>

      <Panel
        title="Events by category"
        description="24-hour distribution of stream categories."
        tone="ai"
      >
        {summary.by_category.length === 0 ? (
          <p className="text-sm text-muted-foreground">No events.</p>
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
}
function FilterChips({ label, options, selected, onChange }: FilterChipsProps) {
  function toggle(opt: string) {
    const next = new Set(selected);
    if (next.has(opt)) next.delete(opt);
    else next.add(opt);
    onChange(next);
  }
  return (
    <div>
      <p className="mb-2 text-2xs font-semibold uppercase tracking-wider text-muted-foreground">
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
              className={cn(
                "rounded-full border px-3 py-1 text-xs transition-colors",
                active
                  ? "border-accent bg-accent/15 text-accent-foreground"
                  : "border-border bg-card text-muted-foreground hover:border-accent/40 hover:text-foreground",
              )}
            >
              {o}
            </button>
          );
        })}
      </div>
    </div>
  );
}

// --- columns --------------------------------------------------------------

function SeverityCell({ severity }: { severity: string }) {
  const color = RISK_COLORS[severity] ?? RISK_COLORS.low;
  return (
    <span
      className="inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-medium"
      style={{ backgroundColor: `${color}26`, color }}
    >
      <span className="text-[10px]">{SEVERITY_ICON[severity] ?? "●"}</span>
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
      <span className="font-mono text-foreground">
        {r.device_id ? `#${r.device_id}` : "—"}
      </span>
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
    accessor: (r) => (
      <span className="text-foreground">{r.alert_message ?? "—"}</span>
    ),
  },
  { key: "assigned_to", header: "Assignee", accessor: (r) => r.assigned_to ?? "—" },
];

const STREAM_COLUMNS: ReadonlyArray<ColumnDef<LiveAlertRow>> = [
  { key: "severity", header: "Severity", accessor: (r) => <SeverityCell severity={r.severity} /> },
  {
    key: "priority_score",
    header: "Priority",
    accessor: (r) => (
      <span className="font-semibold tabular-nums">{fmtDec(r.priority_score)}</span>
    ),
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
      <span className="font-mono text-foreground">
        {r.device_id ? `#${r.device_id}` : "—"}
      </span>
    ),
    align: "right",
  },
  {
    key: "alert_message",
    header: "Message",
    accessor: (r) => (
      <span className="text-foreground">{r.alert_message ?? "—"}</span>
    ),
  },
  {
    key: "device_risk_category",
    header: "Device risk",
    accessor: (r) => r.device_risk_category ?? "—",
  },
];
