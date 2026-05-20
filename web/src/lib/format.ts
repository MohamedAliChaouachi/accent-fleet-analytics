// Tiny number/date formatters used across pages. Mirrors the Streamlit
// "{:,.0f}" / "{:,.2f}" defaults so the two dashboards present the same
// values during cutover.

const intFmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const decFmt = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const pctFmt = new Intl.NumberFormat("en-US", {
  style: "percent",
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
});

export function fmtInt(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return intFmt.format(v);
}

export function fmtDec(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return decFmt.format(v);
}

export function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  // API returns "share_pct" columns as raw percentages (0–100), so divide.
  return pctFmt.format(v / 100);
}
