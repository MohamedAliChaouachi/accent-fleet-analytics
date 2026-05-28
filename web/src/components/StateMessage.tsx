import type { ReactNode } from "react";
import { AlertCircle, AlertTriangle, Info } from "lucide-react";
import { cn } from "@/lib/cn";

type Tone = "info" | "warning" | "error";

const TONE_CLASS: Record<Tone, string> = {
  info: "border-border bg-muted/40 text-foreground",
  warning: "border-warning/30 bg-warning/10 text-foreground",
  error: "border-destructive/30 bg-destructive/10 text-foreground",
};

const TONE_ICON: Record<Tone, ReactNode> = {
  info: <Info className="size-4 text-muted-foreground" />,
  warning: <AlertTriangle className="size-4 text-warning" />,
  error: <AlertCircle className="size-4 text-destructive" />,
};

interface StateMessageProps {
  tone?: Tone;
  children: ReactNode;
  className?: string;
}

export function StateMessage({
  tone = "info",
  children,
  className,
}: StateMessageProps) {
  return (
    <div
      className={cn(
        "flex items-start gap-3 rounded-md border px-4 py-3 text-sm",
        TONE_CLASS[tone],
        className,
      )}
    >
      <span className="mt-0.5 shrink-0">{TONE_ICON[tone]}</span>
      <div className="min-w-0 flex-1">{children}</div>
    </div>
  );
}
