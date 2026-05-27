import type { ReactNode } from "react";
import { ArrowDownRight, ArrowUpRight, Minus } from "lucide-react";
import { Card, CardContent } from "./Card";
import { Skeleton } from "./Skeleton";
import { cn } from "@/lib/cn";

export type KpiTone =
  | "default"
  | "primary"
  | "accent"
  | "ai"
  | "success"
  | "warning"
  | "danger";

interface KpiCardProps {
  label: string;
  value: ReactNode;
  // Optional contextual icon (lucide icon). Rendered in the top-right.
  icon?: ReactNode;
  // Optional small line of supporting text under the value.
  hint?: ReactNode;
  // Optional signed delta. ">0" treated as up, "<0" as down. `inverse`
  // flips the color logic (e.g. for "cost / km" where lower is better).
  trend?: { delta: number; label?: string; inverse?: boolean };
  // Tone drives the accent stripe and the icon background.
  tone?: KpiTone;
  // Optional 1-D series for a sparkline along the bottom of the card.
  sparkline?: ReadonlyArray<number>;
  // Render skeleton placeholder if true.
  loading?: boolean;
  className?: string;
}

const TONE_STYLES: Record<
  KpiTone,
  { stripe: string; iconBg: string; iconFg: string }
> = {
  default: {
    stripe: "bg-border",
    iconBg: "bg-muted",
    iconFg: "text-muted-foreground",
  },
  primary: {
    stripe: "bg-primary",
    iconBg: "bg-primary/10",
    iconFg: "text-primary",
  },
  accent: {
    stripe: "bg-accent",
    iconBg: "bg-accent/10",
    iconFg: "text-accent",
  },
  ai: { stripe: "bg-ai", iconBg: "bg-ai/10", iconFg: "text-ai" },
  success: {
    stripe: "bg-success",
    iconBg: "bg-success/10",
    iconFg: "text-success",
  },
  warning: {
    stripe: "bg-warning",
    iconBg: "bg-warning/10",
    iconFg: "text-warning",
  },
  danger: {
    stripe: "bg-destructive",
    iconBg: "bg-destructive/10",
    iconFg: "text-destructive",
  },
};

export function KpiCard({
  label,
  value,
  icon,
  hint,
  trend,
  tone = "default",
  sparkline,
  loading,
  className,
}: KpiCardProps) {
  const styles = TONE_STYLES[tone];

  if (loading) {
    return (
      <Card className={cn("relative overflow-hidden p-5", className)}>
        <Skeleton className="mb-3 h-3 w-24" />
        <Skeleton className="h-7 w-32" />
        <Skeleton className="mt-3 h-3 w-20" />
      </Card>
    );
  }

  // For trend coloring: "up" is good for most metrics, bad for inverse
  // (cost, risk). Zero or missing delta renders a neutral gray.
  const positive = trend
    ? trend.inverse
      ? trend.delta < 0
      : trend.delta > 0
    : false;
  const negative = trend
    ? trend.inverse
      ? trend.delta > 0
      : trend.delta < 0
    : false;

  return (
    <Card
      className={cn(
        "group relative overflow-hidden p-0 transition-transform duration-200 hover:-translate-y-0.5",
        className,
      )}
    >
      <span
        aria-hidden
        className={cn("absolute inset-y-0 left-0 w-0.5", styles.stripe)}
      />
      <CardContent className="px-5 py-4">
        <div className="flex items-start justify-between gap-3">
          <p className="text-2xs font-semibold uppercase tracking-widest text-muted-foreground">
            {label}
          </p>
          {icon ? (
            <span
              className={cn(
                "flex size-7 items-center justify-center rounded-md [&_svg]:size-3.5",
                styles.iconBg,
                styles.iconFg,
              )}
            >
              {icon}
            </span>
          ) : null}
        </div>

        <p className="mt-2 text-2xl font-semibold tracking-tight text-foreground num">
          {value}
        </p>

        <div className="mt-2 flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 text-xs">
            {trend ? (
              <TrendPill
                delta={trend.delta}
                label={trend.label}
                positive={positive}
                negative={negative}
              />
            ) : null}
            {hint ? (
              <span className="truncate text-muted-foreground">{hint}</span>
            ) : null}
          </div>
        </div>

        {sparkline && sparkline.length > 1 ? (
          <Sparkline
            data={sparkline}
            className={cn(
              "mt-3 h-8 w-full",
              tone === "ai" ? "text-ai" : "text-accent",
            )}
          />
        ) : null}
      </CardContent>
    </Card>
  );
}

function TrendPill({
  delta,
  label,
  positive,
  negative,
}: {
  delta: number;
  label?: string;
  positive: boolean;
  negative: boolean;
}) {
  const Icon = delta === 0 ? Minus : delta > 0 ? ArrowUpRight : ArrowDownRight;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-0.5 rounded-full px-1.5 py-0.5 font-medium",
        positive && "bg-success/10 text-success",
        negative && "bg-destructive/10 text-destructive",
        !positive && !negative && "bg-muted text-muted-foreground",
      )}
    >
      <Icon className="size-3" />
      <span>{Math.abs(delta).toFixed(1)}%</span>
      {label ? (
        <span className="ml-0.5 text-2xs font-normal opacity-70">{label}</span>
      ) : null}
    </span>
  );
}

// Inline SVG sparkline — no chart library overhead. Good enough for KPI
// cards; for richer trend visuals use the Recharts wrappers (Phase 3).
function Sparkline({
  data,
  className,
}: {
  data: ReadonlyArray<number>;
  className?: string;
}) {
  const w = 100;
  const h = 24;
  const min = Math.min(...data);
  const max = Math.max(...data);
  const span = max - min || 1;
  const stepX = w / (data.length - 1);
  const points = data
    .map(
      (v, i) =>
        `${(i * stepX).toFixed(2)},${(h - ((v - min) / span) * h).toFixed(2)}`,
    )
    .join(" ");

  // Closed area path under the line — gives the spark a filled feel.
  const area = `M0,${h} L${points.split(" ").join(" L ")} L${w},${h} Z`;

  return (
    <svg
      viewBox={`0 0 ${w} ${h}`}
      preserveAspectRatio="none"
      className={className}
      aria-hidden
    >
      <path d={area} fill="currentColor" fillOpacity="0.12" />
      <polyline
        points={points}
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
