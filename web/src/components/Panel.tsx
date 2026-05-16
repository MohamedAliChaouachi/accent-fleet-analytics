import type { ReactNode } from "react";

interface PanelProps {
  title: string;
  description?: ReactNode;
  children: ReactNode;
  rightSlot?: ReactNode;
}

export function Panel({ title, description, children, rightSlot }: PanelProps) {
  return (
    <section className="rounded-lg border border-slate-200 bg-white p-5 shadow-sm">
      <header className="mb-4 flex items-start justify-between gap-4">
        <div>
          <h2 className="text-sm font-semibold uppercase tracking-wider text-slate-700">
            {title}
          </h2>
          {description ? (
            <p className="mt-1 text-xs text-slate-500">{description}</p>
          ) : null}
        </div>
        {rightSlot}
      </header>
      {children}
    </section>
  );
}
