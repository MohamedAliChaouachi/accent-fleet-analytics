import { useMemo, useState, type ChangeEvent } from "react";
import {
  Bell,
  Calendar,
  ChevronDown,
  Command,
  LogOut,
  Moon,
  Search,
  Sparkles,
  Sun,
  User,
} from "lucide-react";
import { Button } from "@/components/ui/Button";
import { Badge } from "@/components/ui/Badge";
import { Input } from "@/components/ui/Input";
import { Kbd } from "@/components/ui/Kbd";
import { Separator } from "@/components/ui/Separator";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/Tooltip";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/DropdownMenu";
import { useAuth } from "@/auth/AuthContext";
import { useFilters } from "@/filters/FiltersContext";
import { useTheme } from "@/theme/ThemeProvider";
import { cn } from "@/lib/cn";

interface TopBarProps {
  onOpenAssistant: () => void;
}

// Demo tenant catalog — until the API exposes a tenant list endpoint,
// this is the fixed roster from the brief. The active subset comes from
// FiltersContext (URL-backed), so this list is for the menu UI only.
const KNOWN_TENANTS: ReadonlyArray<{ id: number; name: string }> = [
  { id: 235, name: "Tenant 235" },
  { id: 238, name: "Tenant 238" },
  { id: 264, name: "Tenant 264" },
  { id: 1787, name: "Tenant 1787" },
  { id: 7486, name: "Tenant 7486" },
];

export function TopBar({ onOpenAssistant }: TopBarProps) {
  const { user, logout } = useAuth();
  const { filters, setRange, setTenantIds } = useFilters();
  const { resolvedTheme, toggle } = useTheme();

  const isSuperadmin = user?.role === "superadmin";

  const tenantLabel = useMemo(() => {
    if (!isSuperadmin) {
      return user?.tenant_name ?? `Tenant #${user?.tenant_id ?? "—"}`;
    }
    if (filters.tenant_ids.length === 0) return "All tenants";
    if (filters.tenant_ids.length === 1) return `Tenant ${filters.tenant_ids[0]}`;
    return `${filters.tenant_ids.length} tenants`;
  }, [filters.tenant_ids, isSuperadmin, user?.tenant_id, user?.tenant_name]);

  // The "/" focus-search shortcut is registered globally via the shared
  // shortcuts registry (see DashboardShell). The input below carries the
  // `data-topbar-search` hook the registry handler looks up.
  const [searchFocused, setSearchFocused] = useState(false);

  return (
    <header
      className={cn(
        "sticky top-0 z-20 flex h-topbar items-center gap-3 border-b border-border",
        "bg-background/80 backdrop-blur-xl px-4",
      )}
    >
      {/* Global search */}
      <div className="flex max-w-md flex-1 items-center">
        <Input
          data-global-search
          data-topbar-search
          placeholder="Search devices, tenants, queries…"
          leadingIcon={<Search />}
          trailing={
            !searchFocused ? (
              <Kbd className="hidden sm:inline-flex">/</Kbd>
            ) : null
          }
          onFocus={() => setSearchFocused(true)}
          onBlur={() => setSearchFocused(false)}
          className="text-sm"
        />
      </div>

      <div className="flex-1" />

      {/* Date range pill */}
      <DateRange
        start={filters.start}
        end={filters.end}
        onChange={(s, e) => setRange(s, e)}
      />

      {/* Tenant switcher */}
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="sm" className="gap-2">
            <Building2Indicator />
            <span className="text-xs font-medium">{tenantLabel}</span>
            <ChevronDown className="size-3.5 opacity-60" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-56">
          <DropdownMenuLabel>Tenant scope</DropdownMenuLabel>
          {isSuperadmin ? (
            <>
              <DropdownMenuItem onSelect={() => setTenantIds([])}>
                <span className="flex-1">All tenants</span>
                {filters.tenant_ids.length === 0 ? (
                  <Badge variant="accent" className="ml-2">
                    Active
                  </Badge>
                ) : null}
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              {KNOWN_TENANTS.map((t) => {
                const active = filters.tenant_ids.includes(t.id);
                return (
                  <DropdownMenuItem
                    key={t.id}
                    onSelect={() => {
                      const next = active
                        ? filters.tenant_ids.filter((id) => id !== t.id)
                        : [...filters.tenant_ids, t.id];
                      setTenantIds(next);
                    }}
                  >
                    <span className="flex-1">{t.name}</span>
                    {active ? (
                      <Badge variant="accent" className="ml-2">
                        On
                      </Badge>
                    ) : null}
                  </DropdownMenuItem>
                );
              })}
            </>
          ) : (
            <DropdownMenuItem disabled>
              <span className="flex-1">
                {user?.tenant_name ?? `Tenant #${user?.tenant_id ?? "—"}`}
              </span>
              <Badge variant="outline">Locked</Badge>
            </DropdownMenuItem>
          )}
        </DropdownMenuContent>
      </DropdownMenu>

      <Separator orientation="vertical" className="h-6" />

      {/* AI assistant launcher */}
      <Tooltip>
        <TooltipTrigger asChild>
          <Button
            variant="ai"
            size="sm"
            onClick={onOpenAssistant}
            className="gap-2"
          >
            <Sparkles className="size-4" />
            <span className="hidden sm:inline">Ask AI</span>
            <Kbd className="hidden border-white/20 bg-white/10 text-white/90 sm:inline-flex">
              <Command className="size-2.5" />K
            </Kbd>
          </Button>
        </TooltipTrigger>
        <TooltipContent>Open the AI assistant</TooltipContent>
      </Tooltip>

      {/* Notifications */}
      <Tooltip>
        <TooltipTrigger asChild>
          <Button variant="ghost" size="icon" aria-label="Notifications">
            <Bell className="size-4" />
            <span className="absolute right-1.5 top-1.5 size-1.5 rounded-full bg-risk-high" />
          </Button>
        </TooltipTrigger>
        <TooltipContent>Notifications</TooltipContent>
      </Tooltip>

      {/* Theme toggle */}
      <Tooltip>
        <TooltipTrigger asChild>
          <Button
            variant="ghost"
            size="icon"
            onClick={toggle}
            aria-label={`Switch to ${resolvedTheme === "dark" ? "light" : "dark"} mode`}
          >
            {resolvedTheme === "dark" ? (
              <Sun className="size-4" />
            ) : (
              <Moon className="size-4" />
            )}
          </Button>
        </TooltipTrigger>
        <TooltipContent>Toggle theme</TooltipContent>
      </Tooltip>

      {/* User menu */}
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="ghost" size="sm" className="gap-2 pl-1">
            <Avatar email={user?.email} />
            <div className="hidden flex-col items-start leading-tight sm:flex">
              <span className="text-xs font-medium text-foreground">
                {user?.email?.split("@")[0] ?? "Guest"}
              </span>
              <span className="text-2xs text-muted-foreground capitalize">
                {user?.role ?? "anonymous"}
              </span>
            </div>
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-56">
          <DropdownMenuLabel>
            <div className="flex flex-col">
              <span className="truncate text-sm font-medium normal-case tracking-normal text-foreground">
                {user?.email}
              </span>
              <span className="text-2xs text-muted-foreground">
                {user?.role}
                {user?.tenant_name ? ` · ${user.tenant_name}` : ""}
              </span>
            </div>
          </DropdownMenuLabel>
          <DropdownMenuSeparator />
          <DropdownMenuItem>
            <User className="size-4" />
            <span>Profile</span>
          </DropdownMenuItem>
          <DropdownMenuSeparator />
          <DropdownMenuItem
            onSelect={() => void logout()}
            className="text-destructive focus:text-destructive"
          >
            <LogOut className="size-4" />
            <span>Sign out</span>
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </header>
  );
}

function Building2Indicator() {
  return (
    <span className="flex size-5 items-center justify-center rounded bg-primary/10 text-primary">
      <Sparkles className="size-3" />
    </span>
  );
}

function Avatar({ email }: { email?: string | null }) {
  const initials = (email?.split("@")[0] ?? "?")
    .split(/[._-]/)
    .map((s) => s.charAt(0))
    .filter(Boolean)
    .slice(0, 2)
    .join("")
    .toUpperCase();
  return (
    <span className="flex size-7 items-center justify-center rounded-full bg-gradient-brand text-2xs font-semibold text-white">
      {initials || "?"}
    </span>
  );
}

function DateRange({
  start,
  end,
  onChange,
}: {
  start: string;
  end: string;
  onChange: (s: string, e: string) => void;
}) {
  function onS(e: ChangeEvent<HTMLInputElement>) {
    onChange(e.target.value, end);
  }
  function onE(e: ChangeEvent<HTMLInputElement>) {
    onChange(start, e.target.value);
  }
  return (
    <div className="hidden items-center gap-2 rounded-md border border-border bg-card px-2 py-1 md:flex">
      <Calendar className="size-3.5 text-muted-foreground" />
      <input
        type="date"
        value={start}
        onChange={onS}
        className="bg-transparent text-xs text-foreground focus:outline-none"
        aria-label="Start date"
      />
      <span className="text-muted-foreground">–</span>
      <input
        type="date"
        value={end}
        onChange={onE}
        className="bg-transparent text-xs text-foreground focus:outline-none"
        aria-label="End date"
      />
    </div>
  );
}
