import type { ReactNode } from "react";
import { cn } from "@/lib/cn";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "./Card";

interface PanelProps {
  title: ReactNode;
  description?: ReactNode;
  // Right-aligned slot for controls (filters, tabs, action button).
  actions?: ReactNode;
  // Optional tone — adds a thin top accent strip for visual grouping.
  tone?: "default" | "primary" | "accent" | "ai" | "warning";
  className?: string;
  // Strip the padding when embedding tables that own their own gutters.
  flush?: boolean;
  children: ReactNode;
}

const TONE_STRIPE: Record<NonNullable<PanelProps["tone"]>, string> = {
  default: "",
  primary: "before:bg-primary",
  accent: "before:bg-accent",
  ai: "before:bg-gradient-ai",
  warning: "before:bg-warning",
};

// Standard data panel — chart container, table wrapper, info card.
// Composes Card so any consumer using shadcn-style Card patterns still
// works.
export function Panel({
  title,
  description,
  actions,
  tone = "default",
  className,
  flush,
  children,
}: PanelProps) {
  return (
    <Card
      className={cn(
        "relative overflow-hidden",
        tone !== "default" &&
          `before:absolute before:inset-x-0 before:top-0 before:h-0.5 ${TONE_STRIPE[tone]}`,
        className,
      )}
    >
      <CardHeader className="flex flex-row items-start justify-between gap-4">
        <div>
          <CardTitle className="text-sm font-semibold uppercase tracking-wide text-foreground">
            {title}
          </CardTitle>
          {description ? (
            <CardDescription className="mt-1">{description}</CardDescription>
          ) : null}
        </div>
        {actions ? (
          <div className="flex shrink-0 items-center gap-2">{actions}</div>
        ) : null}
      </CardHeader>
      <CardContent className={cn(flush && "px-0 pb-0")}>{children}</CardContent>
    </Card>
  );
}
