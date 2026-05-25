import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchTenantBilling } from "@/api/dashboards";
import type {
  TenantBillingDashboardResponse,
  TenantBillingRow,
  TenantBillingTier,
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

// Storage delta is "lower = better" only when you care about infra spend;
// for billing dashboards storage growth is a positive signal (more data,
// more revenue) so we leave it un-inverted. Same for tenant/device counts.
function toTrend(
  delta: number | null | undefined,
  label: string,
): { delta: number; label: string } | undefined {
  if (delta === null || delta === undefined) return undefined;
  return { delta, label };
}

export function TenantBilling() {
  const { filters } = useFilters();
  const { data, isPending, isError, error } = useQuery<TenantBillingDashboardResponse>({
    queryKey: ["dashboards", "tenant-billing", filters],
    queryFn: ({ signal }) => fetchTenantBilling(filters, signal),
  });

  return (
    <section>
      <PageHeader
        title="Tenant billing & usage"
        caption={
          <>
            Per-tenant usage, pricing tier, and estimated revenue — sourced from{" "}
            <code className="rounded bg-slate-200 px-1 py-0.5">
              marts.v_tenant_billing_dashboard
            </code>
            .
          </>
        }
      />
      {isPending ? <StateMessage>Loading tenant billing…</StateMessage> : null}
      {isError ? (
        <StateMessage tone="error">
          Failed to load tenant billing: {(error as Error).message}
        </StateMessage>
      ) : null}
      {data ? <Content data={data} /> : null}
    </section>
  );
}

function Content({ data }: { data: TenantBillingDashboardResponse }) {
  const { kpi, monthly, rows, tier_breakdown, latest_month } = data;

  const latestRows = useMemo(() => {
    if (!latest_month) return [];
    return [...rows]
      .filter((r) => r.year_month === latest_month)
      .sort(
        (a, b) => (b.estimated_revenue ?? 0) - (a.estimated_revenue ?? 0),
      );
  }, [rows, latest_month]);

  // Top tenants by current revenue — feeds the MoM growth grouped bar
  // below so it stays readable at scale (otherwise the chart is hairy).
  const topGrowth = useMemo(() => latestRows.slice(0, 10), [latestRows]);

  if (!kpi || monthly.length === 0) {
    return (
      <StateMessage tone="warning">
        No billing data for the current filters. Widen the date range or clear the tenant
        filter.
      </StateMessage>
    );
  }

  return (
    <div className="space-y-6">
      {/* KPI strip ----------------------------------------------------- */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KpiCard
          label="Total tenants"
          value={fmtInt(kpi.total_tenants)}
          trend={toTrend(kpi.total_tenants_delta, "vs prior")}
          accent="#1f3a5f"
        />
        <KpiCard
          label="Active devices"
          value={fmtInt(kpi.total_devices)}
          trend={toTrend(kpi.total_devices_delta, "vs prior")}
          accent="#2a9df4"
        />
        <KpiCard
          label="Est. revenue (DA)"
          value={fmtInt(kpi.total_revenue)}
          trend={toTrend(kpi.total_revenue_delta_pct, "MoM")}
          accent="#16a085"
        />
        <KpiCard
          label="Storage (GB)"
          value={fmtDec(kpi.total_storage_gb)}
          trend={toTrend(kpi.total_storage_delta_pct, "MoM")}
          accent="#9b59b6"
        />
      </div>

      <p className="text-xs text-slate-500">
        Latest: <strong>{kpi.year_month}</strong> · {fmtInt(kpi.total_tenants)} tenants ·{" "}
        {fmtInt(kpi.total_devices)} devices
      </p>

      {/* Usage trends -------------------------------------------------- */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <Panel title="Active devices" accent="#2a9df4">
          <BarChart
            data={monthly as unknown as Array<Record<string, unknown>>}
            xKey="year_month"
            series={[{ dataKey: "active_devices", label: "Active devices" }]}
            yFormatter={(v) => fmtInt(v)}
          />
        </Panel>
        <Panel title="Data volume (GB)" accent="#9b59b6">
          <LineChart
            data={monthly as unknown as Array<Record<string, unknown>>}
            xKey="year_month"
            series={[{ dataKey: "data_volume_gb", label: "Data volume" }]}
            yFormatter={(v) => fmtDec(v)}
          />
        </Panel>
        <Panel title="Estimated revenue" accent="#16a085">
          <LineChart
            data={monthly as unknown as Array<Record<string, unknown>>}
            xKey="year_month"
            series={[{ dataKey: "estimated_revenue", label: "Revenue" }]}
            yFormatter={(v) => fmtInt(v)}
          />
        </Panel>
      </div>

      {/* Per-tenant MoM growth grouped bar ----------------------------- */}
      <Panel
        title={`MoM growth · top ${topGrowth.length} by revenue · ${kpi.year_month}`}
        description="Devices / trips / alerts growth (%) for the current month. Negative = shrinking."
        accent="#34495e"
      >
        {topGrowth.length === 0 ? (
          <p className="text-sm text-slate-500">No tenants in scope for the latest month.</p>
        ) : (
          <BarChart
            data={
              topGrowth.map((r) => ({
                tenant: r.tenant_name ?? `#${r.tenant_id ?? "—"}`,
                devices: r.devices_mom_growth_pct ?? 0,
                trips: r.trips_mom_growth_pct ?? 0,
                alerts: r.alerts_mom_growth_pct ?? 0,
              })) as unknown as Array<Record<string, unknown>>
            }
            xKey="tenant"
            series={[
              { dataKey: "devices", label: "Devices %", color: "#2a9df4" },
              { dataKey: "trips", label: "Trips %", color: "#16a085" },
              { dataKey: "alerts", label: "Alerts %", color: "#e67e22" },
            ]}
            yFormatter={(v) => `${fmtDec(v)}%`}
            legend
          />
        )}
      </Panel>

      {/* Pricing tiers -------------------------------------------------- */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Panel
          title={`Pricing tier breakdown · ${kpi.year_month}`}
          description="Tenants and devices per tier."
          accent="#f39c12"
        >
          {tier_breakdown.length === 0 ? (
            <p className="text-sm text-slate-500">No tier data.</p>
          ) : (
            <DataTable
              rows={tier_breakdown}
              columns={TIER_COLUMNS}
              rowKey={(r) => `tier-${r.pricing_tier}`}
            />
          )}
        </Panel>
        <Panel title="Revenue by tier" accent="#16a085">
          {tier_breakdown.length === 0 ? (
            <p className="text-sm text-slate-500">No tier data.</p>
          ) : (
            <BarChart
              data={
                tier_breakdown.map((t) => ({
                  tier: `Tier ${t.pricing_tier}`,
                  revenue: t.revenue,
                })) as unknown as Array<Record<string, unknown>>
              }
              xKey="tier"
              series={[{ dataKey: "revenue", label: "Revenue (DA)" }]}
              yFormatter={(v) => fmtInt(v)}
            />
          )}
        </Panel>
      </div>

      {/* Per-tenant detail (latest month) ----------------------------- */}
      <Panel
        title={`Per-tenant billing · ${kpi.year_month}`}
        description="Sorted by estimated revenue (descending)."
      >
        <DataTable
          rows={latestRows}
          columns={PER_TENANT_COLUMNS}
          rowKey={(r) => r.tenant_id ?? `unknown-${r.year_month}`}
        />
      </Panel>

      {/* Raw rows ------------------------------------------------------ */}
      <Panel title="Raw table" description="Per-tenant × month rows.">
        <DataTable
          rows={rows}
          columns={RAW_COLUMNS}
          rowKey={(r) => `${r.year_month}::${r.tenant_id ?? "all"}`}
        />
      </Panel>
    </div>
  );
}

const TIER_COLUMNS: ReadonlyArray<ColumnDef<TenantBillingTier>> = [
  { key: "pricing_tier", header: "Tier", accessor: (r) => `Tier ${r.pricing_tier}` },
  { key: "tenants", header: "Tenants", accessor: (r) => fmtInt(r.tenants), align: "right" },
  { key: "devices", header: "Devices", accessor: (r) => fmtInt(r.devices), align: "right" },
  {
    key: "revenue",
    header: "Revenue (DA)",
    accessor: (r) => fmtInt(r.revenue),
    align: "right",
  },
];

const PER_TENANT_COLUMNS: ReadonlyArray<ColumnDef<TenantBillingRow>> = [
  {
    key: "tenant_name",
    header: "Tenant",
    accessor: (r) => r.tenant_name ?? (r.tenant_id !== null ? `#${r.tenant_id}` : "—"),
  },
  {
    key: "pricing_tier",
    header: "Tier",
    accessor: (r) => (r.pricing_tier !== null ? `Tier ${r.pricing_tier}` : "—"),
    align: "right",
  },
  {
    key: "active_devices",
    header: "Devices",
    accessor: (r) => fmtInt(r.active_devices),
    align: "right",
  },
  {
    key: "total_trips_processed",
    header: "Trips",
    accessor: (r) => fmtInt(r.total_trips_processed),
    align: "right",
  },
  {
    key: "data_volume_gb",
    header: "Data (GB)",
    accessor: (r) => fmtDec(r.data_volume_gb),
    align: "right",
  },
  {
    key: "estimated_revenue",
    header: "Revenue (DA)",
    accessor: (r) => fmtInt(r.estimated_revenue),
    align: "right",
  },
  {
    key: "estimated_infra_cost",
    header: "Infra cost",
    accessor: (r) => fmtInt(r.estimated_infra_cost),
    align: "right",
  },
];

const RAW_COLUMNS: ReadonlyArray<ColumnDef<TenantBillingRow>> = [
  { key: "year_month", header: "Month", accessor: (r) => r.year_month },
  {
    key: "tenant_name",
    header: "Tenant",
    accessor: (r) => r.tenant_name ?? (r.tenant_id !== null ? `#${r.tenant_id}` : "—"),
  },
  {
    key: "active_devices",
    header: "Devices",
    accessor: (r) => fmtInt(r.active_devices),
    align: "right",
  },
  {
    key: "total_trips_processed",
    header: "Trips",
    accessor: (r) => fmtInt(r.total_trips_processed),
    align: "right",
  },
  {
    key: "total_alerts_generated",
    header: "Alerts",
    accessor: (r) => fmtInt(r.total_alerts_generated),
    align: "right",
  },
  {
    key: "data_volume_gb",
    header: "Data (GB)",
    accessor: (r) => fmtDec(r.data_volume_gb),
    align: "right",
  },
  {
    key: "pricing_tier",
    header: "Tier",
    accessor: (r) => (r.pricing_tier !== null ? `Tier ${r.pricing_tier}` : "—"),
    align: "right",
  },
  {
    key: "estimated_revenue",
    header: "Revenue (DA)",
    accessor: (r) => fmtInt(r.estimated_revenue),
    align: "right",
  },
];
