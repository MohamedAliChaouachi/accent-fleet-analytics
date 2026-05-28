import type { LucideIcon } from "lucide-react";
import {
  Activity,
  AlertTriangle,
  Banknote,
  Cpu,
  Gauge,
  Route,
  ShieldAlert,
  Sparkles,
  Wrench,
} from "lucide-react";
import { cn } from "@/lib/cn";

export interface Suggestion {
  prompt: string;
  /** Short label shown in the chip. Defaults to `prompt`. */
  label?: string;
  icon?: LucideIcon;
}

// Per-route catalog of context-aware quick prompts. When the assistant
// opens from a specific dashboard, we pre-populate with prompts that
// make sense for what the user is currently looking at.
const ROUTE_SUGGESTIONS: Record<string, Suggestion[]> = {
  "/executive": [
    { prompt: "Show monthly trip volume for the past 12 months.", icon: Route },
    { prompt: "Top 5 tenants by operating cost last month.", icon: Banknote },
    { prompt: "How many active devices do we have today?", icon: Cpu },
  ],
  "/operations": [
    { prompt: "Which devices had zero trips in the last 7 days?", icon: Activity },
    { prompt: "Daily trip count for tenant 235 this week.", icon: Route },
  ],
  "/maintenance": [
    {
      prompt: "Vehicles with maintenance due in the next 30 days.",
      icon: Wrench,
    },
    { prompt: "Average maintenance cost per vehicle by tenant.", icon: Banknote },
  ],
  "/risk": [
    {
      prompt: "Top 10 devices with the highest risk scores last month.",
      icon: ShieldAlert,
    },
    {
      prompt: "Distribution of risk categories across the fleet.",
      icon: Gauge,
    },
    {
      prompt: "How many devices are in each behaviour cluster?",
      icon: Sparkles,
    },
  ],
  "/safety": [
    { prompt: "Devices with the most harsh braking events.", icon: AlertTriangle },
    {
      prompt: "Safety score trend over the last 6 months.",
      icon: ShieldAlert,
    },
  ],
  "/alerts": [
    {
      prompt: "Active high/critical alerts grouped by tenant.",
      icon: AlertTriangle,
    },
    {
      prompt: "Alerts triggered in the last 24 hours.",
      icon: AlertTriangle,
    },
  ],
  "/fleet-efficiency": [
    { prompt: "Cost per kilometer trend by tenant.", icon: Banknote },
    {
      prompt: "Top 5 fuel-inefficient devices last month.",
      icon: Gauge,
    },
  ],
};

// Generic fallback for routes we don't have curated prompts for.
const DEFAULT_SUGGESTIONS: Suggestion[] = [
  { prompt: "Show monthly trip volume for the past 12 months.", icon: Route },
  { prompt: "Top 5 vehicles by total cost last month.", icon: Banknote },
  {
    prompt: "Distribution of risk categories across the fleet.",
    icon: Gauge,
  },
  {
    prompt: "How many devices are in each behaviour cluster?",
    icon: Sparkles,
  },
];

interface QuerySuggestionsProps {
  /** Current route — drives which suggestions show. Defaults to
   * window.location.pathname when omitted. */
  pathname?: string;
  /** Custom override — when supplied, ignores the per-route catalog. */
  suggestions?: ReadonlyArray<Suggestion>;
  onPick: (prompt: string) => void;
  className?: string;
}

export function getSuggestionsForRoute(
  pathname: string,
): ReadonlyArray<Suggestion> {
  for (const [route, list] of Object.entries(ROUTE_SUGGESTIONS)) {
    if (pathname.startsWith(route)) return list;
  }
  return DEFAULT_SUGGESTIONS;
}

export function QuerySuggestions({
  pathname,
  suggestions,
  onPick,
  className,
}: QuerySuggestionsProps) {
  const resolved =
    suggestions ??
    getSuggestionsForRoute(
      pathname ?? (typeof window !== "undefined" ? window.location.pathname : "/"),
    );

  return (
    <div className={cn("flex flex-wrap gap-2", className)}>
      {resolved.map((s) => {
        const Icon = s.icon;
        return (
          <button
            key={s.prompt}
            type="button"
            onClick={() => onPick(s.prompt)}
            className={cn(
              "group/sug inline-flex items-center gap-2 rounded-full border border-border bg-card px-3 py-1.5",
              "text-xs text-foreground transition-all duration-150",
              "hover:border-ai/40 hover:bg-ai/5 hover:text-foreground hover:shadow-sm",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background",
            )}
          >
            {Icon ? (
              <Icon className="size-3.5 text-muted-foreground transition-colors group-hover/sug:text-ai" />
            ) : null}
            <span className="text-left">{s.label ?? s.prompt}</span>
          </button>
        );
      })}
    </div>
  );
}
