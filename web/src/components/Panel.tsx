import type { ReactNode } from "react";

interface PanelProps {
  title: string;
  description?: ReactNode;
  children: ReactNode;
  rightSlot?: ReactNode;
  accent?: string; // optional hex for a thin top accent bar
  padding?: "sm" | "md";
}

export function Panel({
  title,
  description,
  children,
  rightSlot,
  accent,
  padding = "md",
}: PanelProps) {
  return (
    <section className="relative overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm transition hover:shadow-md">
      {accent ? (
        <span
          aria-hidden
          className="absolute inset-x-0 top-0 h-0.5"
          style={{ backgroundColor: accent }}
        />
      ) : null}
      <header
        className={`flex flex-wrap items-start justify-between gap-4 border-b border-slate-100 ${
          padding === "sm" ? "px-4 py-3" : "px-5 py-4"
        }`}
      >
        <div>
          <h2 className="text-sm font-semibold uppercase tracking-wider text-slate-700">
            {title}
          </h2>
          {description ? (
            <p className="mt-1 text-xs text-slate-500">{description}</p>
          ) : null}
        </div>
        {rightSlot ? <div className="flex-shrink-0">{rightSlot}</div> : null}
      </header>
      <div className={padding === "sm" ? "px-4 py-3" : "px-5 py-4"}>{children}</div>
    </section>
  );
}
