import {
  CartesianGrid,
  Line,
  LineChart as RLineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { BRAND, seriesColor } from "@/lib/colors";

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
}

export function LineChart({ data, xKey, series, height = 280, yFormatter }: LineChartProps) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <RLineChart data={data as Array<Record<string, unknown>>} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
        <XAxis dataKey={xKey} tick={{ fontSize: 11, fill: "#475569" }} />
        <YAxis
          tick={{ fontSize: 11, fill: "#475569" }}
          tickFormatter={yFormatter}
          width={60}
        />
        <Tooltip
          formatter={(v: number) => (yFormatter ? yFormatter(v) : v)}
          contentStyle={{ fontSize: 12 }}
        />
        {series.map((s, i) => (
          <Line
            key={s.dataKey}
            type="monotone"
            dataKey={s.dataKey}
            name={s.label ?? s.dataKey}
            stroke={s.color ?? (i === 0 ? BRAND.accent : seriesColor(i))}
            strokeWidth={2}
            dot={{ r: 3 }}
            activeDot={{ r: 5 }}
          />
        ))}
      </RLineChart>
    </ResponsiveContainer>
  );
}
