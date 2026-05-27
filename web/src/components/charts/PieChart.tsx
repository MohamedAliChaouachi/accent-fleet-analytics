import {
  Cell,
  Legend,
  Pie,
  PieChart as RPieChart,
  ResponsiveContainer,
  Tooltip,
} from "recharts";
import { seriesColor } from "@/lib/colors";
import { tooltipStyle, useChartTheme } from "./theme";

interface PieDatum {
  name: string;
  value: number;
  color?: string;
}

interface PieChartProps {
  data: ReadonlyArray<PieDatum>;
  height?: number;
  /** Render as a full pie (no inner hole) instead of donut. */
  donut?: boolean;
}

export function PieChart({ data, height = 260, donut = true }: PieChartProps) {
  const t = useChartTheme();
  return (
    <ResponsiveContainer width="100%" height={height}>
      <RPieChart>
        <Tooltip contentStyle={tooltipStyle(t)} />
        <Legend
          wrapperStyle={{ fontSize: 11, color: t.axisTick }}
          iconType="circle"
        />
        <Pie
          data={data as Array<PieDatum>}
          dataKey="value"
          nameKey="name"
          outerRadius={90}
          innerRadius={donut ? 45 : 0}
          stroke={t.tooltipBg}
          strokeWidth={2}
          label={({ name, percent }) =>
            `${name} ${((percent ?? 0) * 100).toFixed(0)}%`
          }
          labelLine={false}
        >
          {data.map((d, i) => (
            <Cell key={d.name} fill={d.color ?? seriesColor(i)} />
          ))}
        </Pie>
      </RPieChart>
    </ResponsiveContainer>
  );
}
