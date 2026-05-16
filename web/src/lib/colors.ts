// Mirrors the Tailwind risk palette so Recharts (which needs literal CSS
// colors, not Tailwind class names) and the rest of the UI agree.
export const RISK_COLORS: Record<string, string> = {
  low: "#2ecc71",
  moderate: "#f1c40f",
  high: "#e67e22",
  critical: "#e74c3c",
};

export const BRAND = {
  primary: "#1f3a5f",
  accent: "#2a9df4",
} as const;

// Stable palette for cluster ids / generic series. Avoid red so it
// doesn't read as "critical".
export const SERIES_PALETTE = [
  "#2a9df4",
  "#1f3a5f",
  "#9b59b6",
  "#16a085",
  "#f39c12",
  "#34495e",
  "#7f8c8d",
  "#8e44ad",
];

export function seriesColor(i: number): string {
  return SERIES_PALETTE[i % SERIES_PALETTE.length] ?? BRAND.primary;
}
