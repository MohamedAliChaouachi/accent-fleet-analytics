import { useState, type ChangeEvent } from "react";
import { NavLink, Outlet } from "react-router-dom";
import { useAuth } from "@/auth/AuthContext";
import { useFilters } from "@/filters/FiltersContext";

const NAV_ITEMS: ReadonlyArray<{ to: string; label: string }> = [
  { to: "/executive", label: "Executive overview" },
  { to: "/operations", label: "Operations" },
  { to: "/maintenance", label: "Maintenance" },
  { to: "/risk", label: "Risk & behavior" },
  { to: "/what-if", label: "What-if" },
  { to: "/ai", label: "Ask the data" },
];

function navClass({ isActive }: { isActive: boolean }) {
  return [
    "block rounded-md px-3 py-2 text-sm transition-colors",
    isActive
      ? "bg-brand text-white"
      : "text-slate-300 hover:bg-slate-700 hover:text-white",
  ].join(" ");
}

export function Layout() {
  const { user, logout } = useAuth();
  const { filters, setRange, setTenantIds } = useFilters();

  // Local state for the tenants text input so users can type a comma
  // mid-value without us thrashing the URL on every keystroke. Commit
  // on blur or Enter.
  const [tenantsText, setTenantsText] = useState(filters.tenant_ids.join(", "));

  function commitTenants() {
    const ids = tenantsText
      .split(",")
      .map((s) => s.trim())
      .filter((s) => /^\d+$/.test(s))
      .map((s) => parseInt(s, 10));
    setTenantIds(ids);
  }

  function onStartChange(e: ChangeEvent<HTMLInputElement>) {
    setRange(e.target.value, filters.end);
  }
  function onEndChange(e: ChangeEvent<HTMLInputElement>) {
    setRange(filters.start, e.target.value);
  }

  return (
    <div className="flex min-h-full">
      <aside className="w-64 shrink-0 bg-slate-900 px-4 py-6 text-white">
        <div className="mb-6">
          <p className="text-xs uppercase tracking-wider text-slate-400">Accent Fleet</p>
          <p className="text-base font-semibold">Analytics</p>
        </div>

        <nav className="mb-8 space-y-1">
          {NAV_ITEMS.map((item) => (
            <NavLink key={item.to} to={item.to} className={navClass}>
              {item.label}
            </NavLink>
          ))}
        </nav>

        <div className="space-y-4 border-t border-slate-700 pt-6">
          <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400">
            Filters
          </h2>
          <label className="block">
            <span className="mb-1 block text-xs text-slate-400">Start</span>
            <input
              type="date"
              value={filters.start}
              onChange={onStartChange}
              className="block w-full rounded-md border border-slate-700 bg-slate-800 px-2 py-1 text-sm text-white"
            />
          </label>
          <label className="block">
            <span className="mb-1 block text-xs text-slate-400">End</span>
            <input
              type="date"
              value={filters.end}
              onChange={onEndChange}
              className="block w-full rounded-md border border-slate-700 bg-slate-800 px-2 py-1 text-sm text-white"
            />
          </label>
          <label className="block">
            <span className="mb-1 block text-xs text-slate-400">
              Tenant ids (comma-separated, blank = all)
            </span>
            <input
              type="text"
              value={tenantsText}
              onChange={(e) => setTenantsText(e.target.value)}
              onBlur={commitTenants}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault();
                  commitTenants();
                }
              }}
              placeholder="e.g. 1, 2"
              className="block w-full rounded-md border border-slate-700 bg-slate-800 px-2 py-1 text-sm text-white placeholder:text-slate-500"
            />
          </label>
          <p className="text-xs text-slate-500">
            {filters.tenant_ids.length
              ? `Scope: ${filters.tenant_ids.length} tenant(s)`
              : "Scope: all tenants"}
          </p>
        </div>

        <div className="mt-10 border-t border-slate-700 pt-4 text-xs text-slate-400">
          {user ? (
            <>
              <p className="truncate text-slate-200">{user.email}</p>
              <p className="mt-0.5">
                {user.role}
                {user.tenant_name ? ` · ${user.tenant_name}` : ""}
              </p>
            </>
          ) : null}
          <button
            type="button"
            onClick={() => void logout()}
            className="mt-3 text-slate-300 underline-offset-2 hover:text-white hover:underline"
          >
            Sign out
          </button>
        </div>
      </aside>

      <main className="flex-1 overflow-y-auto px-8 py-6">
        <Outlet />
      </main>
    </div>
  );
}
