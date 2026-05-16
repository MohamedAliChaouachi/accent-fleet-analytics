import type { ReactNode } from "react";

interface PageHeaderProps {
  title: string;
  caption?: ReactNode;
}

export function PageHeader({ title, caption }: PageHeaderProps) {
  return (
    <header className="mb-6">
      <h1 className="text-2xl font-semibold text-slate-900">{title}</h1>
      {caption ? <p className="mt-2 text-sm text-slate-500">{caption}</p> : null}
    </header>
  );
}
