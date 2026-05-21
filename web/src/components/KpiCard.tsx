import type { ReactNode } from "react";

interface KpiCardProps {
  label: string;
  value: string;
  hint?: ReactNode;
  accent?: string; // hex; used as a thin left-border + faint background wash
  icon?: ReactNode;
  trend?: { delta: number; label?: string }; // delta is signed; ">0" = up
}

export function KpiCard({ label, value, hint, accent, icon, trend }: KpiCardProps) {
  const accentStyle = accent
    ? {
        borderLeftColor: accent,
        background: `linear-gradient(180deg, ${accent}0d 0%, #ffffff 60%)`,
      }
    : undefined;
  return (
    <div
      className="group relative overflow-hidden rounded-xl border border-slate-200 border-l-4 bg-white px-4 py-3 shadow-sm transition hover:-translate-y-0.5 hover:shadow-md"
      style={accentStyle}
    >
      <div className="flex items-start justify-between gap-3">
        <p className="text-[11px] font-medium uppercase tracking-wider text-slate-500">
          {label}
        </p>
        {icon ? <span className="text-slate-400">{icon}</span> : null}
      </div>
      <p className="mt-1 text-2xl font-semibold tabular-nums text-slate-900">{value}</p>
      {trend ? (
        <p
          className={`mt-1 text-xs font-medium ${
            trend.delta >= 0 ? "text-emerald-600" : "text-rose-600"
          }`}
        >
          {trend.delta >= 0 ? "\u25B2" : "\u25BC"} {Math.abs(trend.delta).toFixed(1)}%
          {trend.label ? <span className="ml-1 text-slate-400">{trend.label}</span> : null}
        </p>
      ) : null}
      {hint ? <p className="mt-1 text-xs text-slate-400">{hint}</p> : null}
    </div>
  );
}
