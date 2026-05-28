import { useMemo, useState } from "react";
import {
  BarChart3,
  Download,
  Table as TableIcon,
} from "lucide-react";
import type { AIChartType } from "@/api/ai";
import { Button } from "@/components/ui/Button";
import { Badge } from "@/components/ui/Badge";
import { cn } from "@/lib/cn";
import { BarChart } from "@/components/charts/BarChart";
import { LineChart } from "@/components/charts/LineChart";
import { PieChart } from "@/components/charts/PieChart";

interface ResultsTableProps {
  rows: ReadonlyArray<Record<string, unknown>>;
  columns: ReadonlyArray<string>;
  /** Server-recommended chart type. We honor it as the default view; the
   * user can flip to table view via the toolbar. */
  chartType: AIChartType;
  className?: string;
}

const VISIBLE_LIMIT = 50;

// Combined results view — chart by default (when the server suggested
// one), table on toggle, and CSV export. Designed to live inside an AI
// chat bubble or a /ai full-page response card.
export function ResultsTable({
  rows,
  columns,
  chartType,
  className,
}: ResultsTableProps) {
  const [view, setView] = useState<"chart" | "table">(
    chartType === "table" || columns.length < 2 ? "table" : "chart",
  );

  const cols = useMemo(
    () => (columns.length > 0 ? columns : Object.keys(rows[0] ?? {})),
    [columns, rows],
  );

  if (rows.length === 0) {
    return (
      <div
        className={cn(
          "rounded-md border border-dashed border-border bg-muted/30 p-6 text-center text-xs text-muted-foreground",
          className,
        )}
      >
        Query returned no rows.
      </div>
    );
  }

  const chartable = chartType !== "table" && cols.length >= 2;

  return (
    <div
      className={cn(
        "overflow-hidden rounded-md border border-border bg-background",
        className,
      )}
    >
      <div className="flex items-center justify-between gap-2 border-b border-border bg-muted/30 px-3 py-2">
        <div className="flex items-center gap-2">
          <Badge variant="outline" className="gap-1">
            {rows.length} {rows.length === 1 ? "row" : "rows"}
          </Badge>
          {cols.length > 0 ? (
            <Badge variant="outline">{cols.length} cols</Badge>
          ) : null}
        </div>
        <div className="flex items-center gap-1">
          {chartable ? (
            <div className="flex items-center rounded-md border border-border bg-card p-0.5">
              <ToggleBtn
                active={view === "chart"}
                onClick={() => setView("chart")}
                icon={<BarChart3 className="size-3" />}
                label="Chart"
              />
              <ToggleBtn
                active={view === "table"}
                onClick={() => setView("table")}
                icon={<TableIcon className="size-3" />}
                label="Table"
              />
            </div>
          ) : null}
          <Button
            variant="ghost"
            size="icon-sm"
            onClick={() => exportCsv(rows, cols)}
            aria-label="Export CSV"
            title="Export CSV"
          >
            <Download className="size-3.5" />
          </Button>
        </div>
      </div>

      <div className="p-3">
        {view === "chart" && chartable ? (
          <ChartView rows={rows} chartType={chartType} columns={cols} />
        ) : (
          <TableView rows={rows} columns={cols} />
        )}
      </div>
    </div>
  );
}

function ToggleBtn({
  active,
  onClick,
  icon,
  label,
}: {
  active: boolean;
  onClick: () => void;
  icon: React.ReactNode;
  label: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "flex h-6 items-center gap-1 rounded px-1.5 text-2xs transition-colors",
        active
          ? "bg-secondary text-secondary-foreground"
          : "text-muted-foreground hover:text-foreground",
      )}
      aria-pressed={active}
    >
      {icon}
      <span>{label}</span>
    </button>
  );
}

function ChartView({
  rows,
  chartType,
  columns,
}: {
  rows: ReadonlyArray<Record<string, unknown>>;
  chartType: AIChartType;
  columns: ReadonlyArray<string>;
}) {
  const [xKey, yKey] = columns;
  if (!xKey || !yKey) return null;

  if (chartType === "line") {
    return (
      <LineChart
        data={rows as Array<Record<string, unknown>>}
        xKey={xKey}
        series={[{ dataKey: yKey, label: yKey }]}
      />
    );
  }
  if (chartType === "pie") {
    return (
      <PieChart
        data={rows.map((r) => ({
          name: String(r[xKey] ?? "—"),
          value: Number(r[yKey] ?? 0),
        }))}
      />
    );
  }
  return (
    <BarChart
      data={rows as Array<Record<string, unknown>>}
      xKey={xKey}
      series={[{ dataKey: yKey, label: yKey }]}
      layout={chooseBarLayout(rows.length, xKey, rows)}
    />
  );
}

function TableView({
  rows,
  columns,
}: {
  rows: ReadonlyArray<Record<string, unknown>>;
  columns: ReadonlyArray<string>;
}) {
  const truncated = rows.length > VISIBLE_LIMIT;
  const shown = truncated ? rows.slice(0, VISIBLE_LIMIT) : rows;

  return (
    <div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[20rem] border-collapse text-xs">
          <thead>
            <tr className="text-left text-muted-foreground">
              {columns.map((c) => (
                <th
                  key={c}
                  className="border-b border-border bg-muted/40 px-2 py-1.5 font-medium uppercase tracking-wider text-2xs"
                >
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {shown.map((r, i) => (
              <tr
                key={i}
                className="hover:bg-secondary/40 even:bg-muted/20 transition-colors"
              >
                {columns.map((c) => (
                  <td
                    key={c}
                    className="border-b border-border/50 px-2 py-1.5 text-foreground num"
                  >
                    {formatCell(r[c])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {truncated ? (
        <p className="mt-2 text-2xs text-muted-foreground">
          Showing first {VISIBLE_LIMIT} of {rows.length} rows.
        </p>
      ) : null}
    </div>
  );
}

function formatCell(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number") {
    if (Number.isInteger(v)) return v.toLocaleString();
    return v.toLocaleString(undefined, { maximumFractionDigits: 4 });
  }
  if (typeof v === "boolean") return v ? "yes" : "no";
  return String(v);
}

function chooseBarLayout(
  n: number,
  xKey: string,
  rows: ReadonlyArray<Record<string, unknown>>,
): "horizontal" | "vertical" {
  if (n > 6) return "vertical";
  const maxLen = Math.max(...rows.map((r) => String(r[xKey] ?? "").length));
  return maxLen > 10 ? "vertical" : "horizontal";
}

function exportCsv(
  rows: ReadonlyArray<Record<string, unknown>>,
  columns: ReadonlyArray<string>,
) {
  const escape = (v: unknown) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [
    columns.join(","),
    ...rows.map((r) => columns.map((c) => escape(r[c])).join(",")),
  ];
  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `accent-results-${new Date().toISOString().slice(0, 10)}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
