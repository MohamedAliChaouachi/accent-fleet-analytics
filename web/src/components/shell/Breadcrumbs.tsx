import { ChevronRight, Home } from "lucide-react";
import { Link, useLocation } from "react-router-dom";
import { cn } from "@/lib/cn";

// Map route segments to display labels. Keeping it inline (vs a big map
// on every nav item) avoids drift when route order changes.
const SEGMENT_LABELS: Record<string, string> = {
  executive: "Executive overview",
  operations: "Operations",
  maintenance: "Maintenance",
  risk: "Risk & behavior",
  "fleet-efficiency": "Fleet efficiency",
  safety: "Safety scorecard",
  alerts: "Predictive alerts",
  billing: "Tenant billing",
  ai: "Ask the data",
};

export function Breadcrumbs({ className }: { className?: string }) {
  const { pathname } = useLocation();
  const parts = pathname.split("/").filter(Boolean);

  return (
    <nav
      className={cn("flex items-center text-xs text-muted-foreground", className)}
      aria-label="Breadcrumb"
    >
      <Link
        to="/executive"
        className="flex items-center gap-1 rounded px-1 py-0.5 hover:text-foreground"
      >
        <Home className="size-3" />
        <span className="sr-only">Home</span>
      </Link>
      {parts.map((p, i) => {
        const isLast = i === parts.length - 1;
        const to = "/" + parts.slice(0, i + 1).join("/");
        const label = SEGMENT_LABELS[p] ?? p;
        return (
          <span key={to} className="flex items-center">
            <ChevronRight className="mx-1 size-3 opacity-50" />
            {isLast ? (
              <span className="font-medium text-foreground">{label}</span>
            ) : (
              <Link
                to={to}
                className="rounded px-1 py-0.5 hover:bg-secondary hover:text-foreground"
              >
                {label}
              </Link>
            )}
          </span>
        );
      })}
    </nav>
  );
}
