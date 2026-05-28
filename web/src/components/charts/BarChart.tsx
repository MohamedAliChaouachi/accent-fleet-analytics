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
import { seriesColor } from "@/lib/colors";
import { tooltipStyle, useChartTheme } from "./theme";

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
  const t = useChartTheme();
  const horizontal = layout === "horizontal";
  return (
    <ResponsiveContainer width="100%" height={height}>
      <RBarChart
        data={data as Array<Record<string, unknown>>}
        layout={layout}
        margin={{ top: 8, right: 16, bottom: 8, left: horizontal ? 8 : 80 }}
      >
        <CartesianGrid strokeDasharray="3 3" stroke={t.grid} />
        {horizontal ? (
          <>
            <XAxis
              dataKey={xKey}
              tick={{ fontSize: 11, fill: t.axisTick }}
              axisLine={{ stroke: t.axisLine }}
              tickLine={{ stroke: t.axisLine }}
            />
            <YAxis
              tick={{ fontSize: 11, fill: t.axisTick }}
              tickFormatter={yFormatter}
              width={60}
              axisLine={{ stroke: t.axisLine }}
              tickLine={{ stroke: t.axisLine }}
            />
          </>
        ) : (
          <>
            <XAxis
              type="number"
              tick={{ fontSize: 11, fill: t.axisTick }}
              tickFormatter={yFormatter}
              axisLine={{ stroke: t.axisLine }}
              tickLine={{ stroke: t.axisLine }}
            />
            <YAxis
              type="category"
              dataKey={xKey}
              tick={{ fontSize: 11, fill: t.axisTick }}
              width={120}
              axisLine={{ stroke: t.axisLine }}
              tickLine={{ stroke: t.axisLine }}
            />
          </>
        )}
        <Tooltip
          formatter={(v: number) => (yFormatter ? yFormatter(v) : v)}
          contentStyle={tooltipStyle(t)}
          cursor={{ fill: t.grid }}
        />
        {legend ? (
          <Legend
            wrapperStyle={{ fontSize: 11, color: t.axisTick, paddingTop: 8 }}
            iconType="circle"
          />
        ) : null}
        {series.map((s, i) => (
          <Bar
            key={s.dataKey}
            dataKey={s.dataKey}
            name={s.label ?? s.dataKey}
            fill={s.color ?? seriesColor(i)}
            stackId={s.stackId}
            radius={[4, 4, 0, 0]}
          >
            {rowColors && i === 0
              ? data.map((_, idx) => (
                  <Cell key={idx} fill={rowColors[idx] ?? seriesColor(0)} />
                ))
              : null}
          </Bar>
        ))}
      </RBarChart>
    </ResponsiveContainer>
  );
}
