import {
  CartesianGrid,
  Legend,
  Line,
  LineChart as RLineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { seriesColor } from "@/lib/colors";
import { tooltipStyle, useChartTheme } from "./theme";

export interface LineSeries {
  dataKey: string;
  label?: string;
  color?: string;
}

interface LineChartProps {
  data: ReadonlyArray<Record<string, unknown>>;
  xKey: string;
  series: ReadonlyArray<LineSeries>;
  height?: number;
  yFormatter?: (v: number) => string;
  legend?: boolean;
}

export function LineChart({
  data,
  xKey,
  series,
  height = 280,
  yFormatter,
  legend,
}: LineChartProps) {
  const t = useChartTheme();
  return (
    <ResponsiveContainer width="100%" height={height}>
      <RLineChart
        data={data as Array<Record<string, unknown>>}
        margin={{ top: 8, right: 16, bottom: 8, left: 8 }}
      >
        <CartesianGrid strokeDasharray="3 3" stroke={t.grid} />
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
        <Tooltip
          formatter={(v: number) => (yFormatter ? yFormatter(v) : v)}
          contentStyle={tooltipStyle(t)}
          cursor={{ stroke: t.axisLine, strokeWidth: 1 }}
        />
        {legend ? (
          <Legend
            wrapperStyle={{ fontSize: 11, color: t.axisTick, paddingTop: 8 }}
            iconType="circle"
          />
        ) : null}
        {series.map((s, i) => (
          <Line
            key={s.dataKey}
            type="monotone"
            dataKey={s.dataKey}
            name={s.label ?? s.dataKey}
            stroke={s.color ?? seriesColor(i)}
            strokeWidth={2}
            dot={{ r: 3, strokeWidth: 0 }}
            activeDot={{ r: 5, strokeWidth: 0 }}
          />
        ))}
      </RLineChart>
    </ResponsiveContainer>
  );
}
