import {
  Cell,
  Legend,
  Pie,
  PieChart as RPieChart,
  ResponsiveContainer,
  Tooltip,
} from "recharts";
import { seriesColor } from "@/lib/colors";

interface PieDatum {
  name: string;
  value: number;
  color?: string;
}

interface PieChartProps {
  data: ReadonlyArray<PieDatum>;
  height?: number;
}

export function PieChart({ data, height = 260 }: PieChartProps) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <RPieChart>
        <Tooltip contentStyle={{ fontSize: 12 }} />
        <Legend wrapperStyle={{ fontSize: 11 }} />
        <Pie
          data={data as Array<PieDatum>}
          dataKey="value"
          nameKey="name"
          outerRadius={90}
          innerRadius={45}
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
