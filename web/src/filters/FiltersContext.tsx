import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  type ReactNode,
} from "react";
import { useSearchParams } from "react-router-dom";
import type { DashboardFilters } from "@/api/types";

// Default window: last 90 days.
const DEFAULT_WINDOW_DAYS = 90;

function isoDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function defaultRange(): { start: string; end: string } {
  const today = new Date();
  const start = new Date(today);
  start.setDate(start.getDate() - DEFAULT_WINDOW_DAYS);
  return { start: isoDate(start), end: isoDate(today) };
}

interface FiltersContextValue {
  filters: DashboardFilters;
  setRange: (start: string, end: string) => void;
  setTenantIds: (ids: number[]) => void;
}

const FiltersContext = createContext<FiltersContextValue | null>(null);

// Backing store is the URL. Persisting in the URL means deep links and
// browser back/forward both Just Work, and there's no localStorage
// invalidation to think about. Sidebar filter controls only need to
// update searchParams.
export function FiltersProvider({ children }: { children: ReactNode }) {
  const [searchParams, setSearchParams] = useSearchParams();
  const defaults = useMemo(defaultRange, []);

  const filters: DashboardFilters = useMemo(() => {
    const start = searchParams.get("start") ?? defaults.start;
    const end = searchParams.get("end") ?? defaults.end;
    const rawTenants = searchParams.getAll("tenant_ids");
    const tenant_ids = rawTenants
      .flatMap((s) => s.split(","))
      .map((s) => s.trim())
      .filter((s) => /^\d+$/.test(s))
      .map((s) => parseInt(s, 10));
    return { start, end, tenant_ids };
  }, [searchParams, defaults]);

  const setRange = useCallback(
    (start: string, end: string) => {
      setSearchParams(
        (prev) => {
          prev.set("start", start);
          prev.set("end", end);
          return prev;
        },
        { replace: true },
      );
    },
    [setSearchParams],
  );

  const setTenantIds = useCallback(
    (ids: number[]) => {
      setSearchParams(
        (prev) => {
          prev.delete("tenant_ids");
          for (const id of ids) prev.append("tenant_ids", String(id));
          return prev;
        },
        { replace: true },
      );
    },
    [setSearchParams],
  );

  const value = useMemo(
    () => ({ filters, setRange, setTenantIds }),
    [filters, setRange, setTenantIds],
  );

  return <FiltersContext.Provider value={value}>{children}</FiltersContext.Provider>;
}

export function useFilters(): FiltersContextValue {
  const ctx = useContext(FiltersContext);
  if (!ctx) throw new Error("useFilters must be used inside <FiltersProvider>");
  return ctx;
}
