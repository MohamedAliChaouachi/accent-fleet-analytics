import {
  Bar,
  BarChart as RBarChart,
  CartesianGrid,
  Cell,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { BRAND, seriesColor } from "@/lib/colors";

export interface BarSeries {
  dataKey: string;
  label?: string;
  color?: string;
  stackId?: string;
}

interface BarChartProps {
  data: ReadonlyArray<Record<string, unknown>>;
  xKey: string;
  series: ReadonlyArray<BarSeries>;
  height?: number;
  layout?: "horizontal" | "vertical";
  yFormatter?: (v: number) => string;
  // Per-row override colors for the first series (used by the risk
  // distribution chart so each category bar uses its own color).
  rowColors?: ReadonlyArray<string>;
  legend?: boolean;
}

export function BarChart({
  data,
  xKey,
  series,
  height = 280,
  layout = "horizontal",
  yFormatter,
  rowColors,
  legend,
}: BarChartProps) {
  const horizontal = layout === "horizontal";
  return (
    <ResponsiveContainer width="100%" height={height}>
      <RBarChart
        data={data as Array<Record<string, unknown>>}
        layout={layout}
        margin={{ top: 8, right: 16, bottom: 8, left: horizontal ? 8 : 80 }}
      >
        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
        {horizontal ? (
          <>
            <XAxis dataKey={xKey} tick={{ fontSize: 11, fill: "#475569" }} />
            <YAxis tick={{ fontSize: 11, fill: "#475569" }} tickFormatter={yFormatter} width={60} />
          </>
        ) : (
          <>
            <XAxis type="number" tick={{ fontSize: 11, fill: "#475569" }} tickFormatter={yFormatter} />
            <YAxis type="category" dataKey={xKey} tick={{ fontSize: 11, fill: "#475569" }} width={120} />
          </>
        )}
        <Tooltip
          formatter={(v: number) => (yFormatter ? yFormatter(v) : v)}
          contentStyle={{ fontSize: 12 }}
        />
        {legend ? <Legend wrapperStyle={{ fontSize: 11 }} /> : null}
        {series.map((s, i) => (
          <Bar
            key={s.dataKey}
            dataKey={s.dataKey}
            name={s.label ?? s.dataKey}
            fill={s.color ?? (i === 0 ? BRAND.accent : seriesColor(i))}
            stackId={s.stackId}
          >
            {rowColors && i === 0
              ? data.map((_, idx) => (
                  <Cell key={idx} fill={rowColors[idx] ?? BRAND.accent} />
                ))
              : null}
          </Bar>
        ))}
      </RBarChart>
    </ResponsiveContainer>
  );
}
