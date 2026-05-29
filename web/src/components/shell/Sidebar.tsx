import { useMemo } from "react";
import { NavLink } from "react-router-dom";
import {
  Activity,
  AlertTriangle,
  BarChart3,
  Bot,
  Building2,
  ChevronsLeft,
  ChevronsRight,
  Gauge,
  LayoutDashboard,
  ShieldAlert,
  ShieldCheck,
  Wrench,
  type LucideIcon,
} from "lucide-react";
import { useAuth } from "@/auth/AuthContext";
import type { UserRole } from "@/api/types";
import { cn } from "@/lib/cn";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/Tooltip";
import { Badge } from "@/components/ui/Badge";

interface NavItem {
  to: string;
  label: string;
  icon: LucideIcon;
  group: "analytics" | "intelligence" | "admin";
  superadminOnly?: boolean;
  badge?: string;
}

// Icon + grouping for each route. Order matters — this is the visual
// order in the rail. Keep parity with the routes table in App.tsx.
const NAV_ITEMS: ReadonlyArray<NavItem> = [
  { to: "/executive", label: "Executive", icon: LayoutDashboard, group: "analytics" },
  { to: "/operations", label: "Operations", icon: Activity, group: "analytics" },
  { to: "/maintenance", label: "Maintenance", icon: Wrench, group: "analytics" },
  { to: "/fleet-efficiency", label: "Efficiency", icon: Gauge, group: "analytics" },
  { to: "/alerts", label: "Alerts", icon: AlertTriangle, group: "analytics" },
  { to: "/risk", label: "Risk & behavior", icon: ShieldAlert, group: "intelligence" },
  { to: "/safety", label: "Safety scorecard", icon: ShieldCheck, group: "intelligence" },
  { to: "/ai", label: "Ask the data", icon: Bot, group: "intelligence", badge: "AI" },
  { to: "/billing", label: "Tenant billing", icon: Building2, group: "admin", superadminOnly: true },
];

const GROUP_LABEL: Record<NavItem["group"], string> = {
  analytics: "Analytics",
  intelligence: "Intelligence",
  admin: "Admin",
};

function visibleItems(role: UserRole | undefined): ReadonlyArray<NavItem> {
  if (role === "superadmin") return NAV_ITEMS;
  return NAV_ITEMS.filter((i) => !i.superadminOnly);
}

interface SidebarProps {
  collapsed: boolean;
  onToggle: () => void;
}

export function Sidebar({ collapsed, onToggle }: SidebarProps) {
  const { user } = useAuth();
  const items = useMemo(() => visibleItems(user?.role), [user?.role]);

  // Group items in render order, only emitting a group header when the
  // group changes — keeps the rail compact without grouping objects.
  const groupedRender: ReactRenderItem[] = [];
  let lastGroup: NavItem["group"] | null = null;
  for (const item of items) {
    if (item.group !== lastGroup) {
      groupedRender.push({ kind: "group", label: GROUP_LABEL[item.group] });
      lastGroup = item.group;
    }
    groupedRender.push({ kind: "item", item });
  }

  return (
    <aside
      className={cn(
        "relative z-30 flex h-screen shrink-0 flex-col border-r border-border bg-card/60 backdrop-blur-xl",
        "transition-[width] duration-300 ease-out",
        collapsed ? "w-sidebar-collapsed" : "w-sidebar",
      )}
      aria-label="Primary navigation"
    >
      {/* Logo / brand strip */}
      <div
        className={cn(
          "flex h-topbar items-center border-b border-border",
          collapsed ? "justify-center px-2" : "justify-between px-4",
        )}
      >
        <div className="flex items-center gap-2">
          <div className="flex size-8 items-center justify-center rounded-md bg-gradient-brand text-white shadow-sm">
            <BarChart3 className="size-4" />
          </div>
          {!collapsed && (
            <div className="flex flex-col leading-tight">
              <span className="text-2xs font-medium uppercase tracking-widest text-muted-foreground">
                Accent
              </span>
              <span className="text-sm font-semibold text-foreground">
                Fleet Analytics
              </span>
            </div>
          )}
        </div>
        {!collapsed && (
          <button
            type="button"
            onClick={onToggle}
            className="rounded-md p-1 text-muted-foreground hover:bg-secondary hover:text-foreground"
            aria-label="Collapse sidebar"
          >
            <ChevronsLeft className="size-4" />
          </button>
        )}
      </div>

      {/* Nav list */}
      <nav
        className={cn(
          "flex-1 overflow-y-auto py-3",
          collapsed ? "px-2" : "px-3",
        )}
      >
        {groupedRender.map((entry, idx) =>
          entry.kind === "group" ? (
            !collapsed ? (
              <p
                key={`g-${entry.label}-${idx}`}
                className="px-2 pb-1 pt-3 text-2xs font-semibold uppercase tracking-widest text-muted-foreground/70 first:pt-0"
              >
                {entry.label}
              </p>
            ) : idx === 0 ? null : (
              <div
                key={`s-${idx}`}
                className="my-2 h-px bg-border/60"
                aria-hidden
              />
            )
          ) : (
            <NavItemRow
              key={entry.item.to}
              item={entry.item}
              collapsed={collapsed}
            />
          ),
        )}
      </nav>

      {/* Collapse handle when collapsed */}
      {collapsed && (
        <button
          type="button"
          onClick={onToggle}
          className="mx-auto mb-3 rounded-md p-1.5 text-muted-foreground hover:bg-secondary hover:text-foreground"
          aria-label="Expand sidebar"
        >
          <ChevronsRight className="size-4" />
        </button>
      )}
    </aside>
  );
}

type ReactRenderItem =
  | { kind: "group"; label: string }
  | { kind: "item"; item: NavItem };

function NavItemRow({ item, collapsed }: { item: NavItem; collapsed: boolean }) {
  const Icon = item.icon;
  const isAi = item.to === "/ai";

  const link = (
    <NavLink
      to={item.to}
      className={({ isActive }) =>
        cn(
          "group/nav relative flex items-center gap-3 rounded-md text-sm transition-colors",
          "outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background",
          collapsed ? "h-9 w-9 justify-center" : "h-9 px-3",
          isActive
            ? isAi
              ? "bg-ai/10 text-ai"
              : "bg-primary/10 text-primary"
            : "text-muted-foreground hover:bg-secondary hover:text-foreground",
        )
      }
    >
      {({ isActive }) => (
        <>
          {isActive && !collapsed ? (
            <span
              aria-hidden
              className={cn(
                "absolute inset-y-1 left-0 w-0.5 rounded-r-full",
                isAi ? "bg-ai" : "bg-primary",
              )}
            />
          ) : null}
          <Icon
            className={cn(
              "size-4 shrink-0",
              isActive && (isAi ? "text-ai" : "text-primary"),
            )}
          />
          {!collapsed && (
            <>
              <span className="truncate">{item.label}</span>
              {item.badge ? (
                <Badge
                  variant={isAi ? "ai" : "accent"}
                  className="ml-auto px-1.5 py-0 text-[9px]"
                >
                  {item.badge}
                </Badge>
              ) : null}
            </>
          )}
        </>
      )}
    </NavLink>
  );

  if (!collapsed) return link;

  return (
    <Tooltip>
      <TooltipTrigger asChild>{link}</TooltipTrigger>
      <TooltipContent side="right" className="flex items-center gap-2">
        {item.label}
        {item.badge ? <Badge variant="ai">{item.badge}</Badge> : null}
      </TooltipContent>
    </Tooltip>
  );
}
