// Theme bridge for Recharts.
//
// Recharts is SVG-driven and expects literal hex/rgb strings for
// strokes and fills, not Tailwind class names or CSS variables. To stay
// in sync with the dark/light theme we expose a hook that returns a
// resolved palette derived from `useTheme()`. Components that need
// theme-aware chart colors (axes, gridlines, tooltips) call this once
// per render — cheap, and avoids hard-coding slate-200 / slate-700 all
// over the chart files.

import { useTheme } from "@/theme/ThemeProvider";

export interface ChartTheme {
  grid: string;
  axisTick: string;
  axisLine: string;
  tooltipBg: string;
  tooltipBorder: string;
  tooltipText: string;
}

const DARK: ChartTheme = {
  grid: "rgba(148, 163, 184, 0.15)", // slate-400 @ 15%
  axisTick: "#94a3b8", // slate-400
  axisLine: "rgba(148, 163, 184, 0.3)",
  tooltipBg: "#0f172a", // slate-900
  tooltipBorder: "#1e293b", // slate-800
  tooltipText: "#e2e8f0", // slate-200
};

const LIGHT: ChartTheme = {
  grid: "rgba(15, 23, 42, 0.08)", // slate-900 @ 8%
  axisTick: "#475569", // slate-600
  axisLine: "rgba(15, 23, 42, 0.15)",
  tooltipBg: "#ffffff",
  tooltipBorder: "#e2e8f0", // slate-200
  tooltipText: "#0f172a", // slate-900
};

export function useChartTheme(): ChartTheme {
  const { resolvedTheme } = useTheme();
  return resolvedTheme === "dark" ? DARK : LIGHT;
}

/** Re-usable Recharts <Tooltip /> contentStyle object. Slight elevation
 * + rounded corners to match our card shadows. */
export function tooltipStyle(t: ChartTheme): React.CSSProperties {
  return {
    fontSize: 12,
    background: t.tooltipBg,
    border: `1px solid ${t.tooltipBorder}`,
    borderRadius: 8,
    color: t.tooltipText,
    boxShadow: "0 4px 12px rgba(0,0,0,0.12)",
    padding: "8px 10px",
  };
}
