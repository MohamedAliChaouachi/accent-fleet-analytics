import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Banknote, Building2, Database, Smartphone } from "lucide-react";
import { fetchTenantBilling } from "@/api/dashboards";
import type {
  TenantBillingDashboardResponse,
  TenantBillingRow,
  TenantBillingTier,
} from "@/api/types";
import { useFilters } from "@/filters/FiltersContext";
import { PageContainer } from "@/components/shell";
import { Badge, KpiCard, Panel, Skeleton } from "@/components/ui";
import { StateMessage } from "@/components/StateMessage";
import { DataTable, type ColumnDef } from "@/components/DataTable";
import { LineChart } from "@/components/charts/LineChart";
import { BarChart } from "@/components/charts/BarChart";
import { SERIES_PALETTE } from "@/lib/colors";
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
    <PageContainer
      title="Tenant billing & usage"
      actions={<Badge variant="accent">Monthly rollup</Badge>}
    >
      {isPending ? <LoadingSkeleton /> : null}
      {isError ? (
        <StateMessage tone="error">
          Failed to load tenant billing: {(error as Error).message}
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
    </div>
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
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KpiCard
          label="Total tenants"
          value={fmtInt(kpi.total_tenants)}
          icon={<Building2 />}
          tone="primary"
          trend={toTrend(kpi.total_tenants_delta, "vs prior")}
        />
        <KpiCard
          label="Active devices"
          value={fmtInt(kpi.total_devices)}
          icon={<Smartphone />}
          tone="accent"
          trend={toTrend(kpi.total_devices_delta, "vs prior")}
        />
        <KpiCard
          label="Est. revenue (DA)"
          value={fmtInt(kpi.total_revenue)}
          icon={<Banknote />}
          tone="primary"
          trend={toTrend(kpi.total_revenue_delta_pct, "MoM")}
        />
        <KpiCard
          label="Storage (GB)"
          value={fmtDec(kpi.total_storage_gb)}
          icon={<Database />}
          tone="ai"
          trend={toTrend(kpi.total_storage_delta_pct, "MoM")}
        />
      </div>

      <p className="text-xs text-muted-foreground">
        Latest: <strong className="text-foreground">{kpi.year_month}</strong> ·{" "}
        {fmtInt(kpi.total_tenants)} tenants · {fmtInt(kpi.total_devices)} devices
      </p>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        <Panel
          title="Active devices"
          description="Monthly active devices."
          tone="accent"
        >
          <BarChart
            data={monthly as unknown as Array<Record<string, unknown>>}
            xKey="year_month"
            series={[{ dataKey: "active_devices", label: "Active devices" }]}
            yFormatter={(v) => fmtInt(v)}
          />
        </Panel>
        <Panel
          title="Data volume (GB)"
          description="Monthly ingested data volume."
          tone="ai"
        >
          <LineChart
            data={monthly as unknown as Array<Record<string, unknown>>}
            xKey="year_month"
            series={[{ dataKey: "data_volume_gb", label: "Data volume" }]}
            yFormatter={(v) => fmtDec(v)}
          />
        </Panel>
        <Panel
          title="Estimated revenue"
          description="DA per month."
          tone="primary"
        >
          <LineChart
            data={monthly as unknown as Array<Record<string, unknown>>}
            xKey="year_month"
            series={[{ dataKey: "estimated_revenue", label: "Revenue" }]}
            yFormatter={(v) => fmtInt(v)}
          />
        </Panel>
      </div>

      <Panel
        title={`MoM growth · top ${topGrowth.length} by revenue · ${kpi.year_month}`}
        description="Devices / trips / alerts growth (%) for the current month. Negative = shrinking."
        tone="primary"
      >
        {topGrowth.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            No tenants in scope for the latest month.
          </p>
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
              { dataKey: "devices", label: "Devices %", color: SERIES_PALETTE[0] },
              { dataKey: "trips", label: "Trips %", color: SERIES_PALETTE[1] },
              { dataKey: "alerts", label: "Alerts %", color: SERIES_PALETTE[2] },
            ]}
            yFormatter={(v) => `${fmtDec(v)}%`}
            legend
          />
        )}
      </Panel>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <Panel
          title={`Pricing tier breakdown · ${kpi.year_month}`}
          description="Tenants and devices per tier."
          tone="warning"
          flush
        >
          {tier_breakdown.length === 0 ? (
            <p className="px-4 py-3 text-sm text-muted-foreground">No tier data.</p>
          ) : (
            <DataTable
              rows={tier_breakdown}
              columns={TIER_COLUMNS}
              rowKey={(r) => `tier-${r.pricing_tier}`}
            />
          )}
        </Panel>
        <Panel
          title="Revenue by tier"
          description="Estimated revenue per pricing tier."
          tone="primary"
        >
          {tier_breakdown.length === 0 ? (
            <p className="text-sm text-muted-foreground">No tier data.</p>
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

      <Panel
        title={`Per-tenant billing · ${kpi.year_month}`}
        description="Sorted by estimated revenue (descending)."
        actions={<Badge variant="outline">{latestRows.length} tenants</Badge>}
        flush
      >
        <DataTable
          rows={latestRows}
          columns={PER_TENANT_COLUMNS}
          rowKey={(r) => r.tenant_id ?? `unknown-${r.year_month}`}
          maxHeight="32rem"
        />
      </Panel>

      <Panel
        title="Raw table"
        description="Per-tenant × month rows."
        actions={<Badge variant="outline">{rows.length} rows</Badge>}
        flush
      >
        <DataTable
          rows={rows}
          columns={RAW_COLUMNS}
          rowKey={(r) => `${r.year_month}::${r.tenant_id ?? "all"}`}
          maxHeight="32rem"
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
