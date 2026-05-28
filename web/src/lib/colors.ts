// Mirrors the Tailwind risk palette so Recharts (which needs literal CSS
// colors, not Tailwind class names) and the rest of the UI agree.
//
// Phase 3 update: the original palette stays the same to keep the
// existing AI-chat ResultsTable and KPI badges visually consistent,
// but the chart wrappers now pull theme-aware grid/axis colors via
// `useChartTheme()` instead of hardcoded slate values so they read
// correctly in dark mode.
export const RISK_COLORS: Record<string, string> = {
  low: "#22c55e",
  moderate: "#eab308",
  high: "#f97316",
  critical: "#ef4444",
};

export const BRAND = {
  primary: "#1f3a5f",
  accent: "#06b6d4",
  ai: "#a855f7",
} as const;

// Stable palette for cluster ids / generic series. Avoid red so it
// doesn't read as "critical".
export const SERIES_PALETTE = [
  "#06b6d4", // cyan-500 (accent)
  "#1f3a5f", // navy (primary)
  "#a855f7", // violet (AI)
  "#10b981", // emerald
  "#f59e0b", // amber
  "#475569", // slate
  "#8b5cf6", // purple
  "#0ea5e9", // sky
];

export function seriesColor(i: number): string {
  return SERIES_PALETTE[i % SERIES_PALETTE.length] ?? BRAND.primary;
}
