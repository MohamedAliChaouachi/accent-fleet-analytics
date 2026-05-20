import type { ReactNode } from "react";

type Tone = "info" | "warning" | "error";

const TONE_CLASS: Record<Tone, string> = {
  info: "border-slate-200 bg-slate-50 text-slate-700",
  warning: "border-amber-200 bg-amber-50 text-amber-900",
  error: "border-red-200 bg-red-50 text-red-900",
};

interface StateMessageProps {
  tone?: Tone;
  children: ReactNode;
}

export function StateMessage({ tone = "info", children }: StateMessageProps) {
  return (
    <div className={`rounded-md border px-4 py-3 text-sm ${TONE_CLASS[tone]}`}>
      {children}
    </div>
  );
}
