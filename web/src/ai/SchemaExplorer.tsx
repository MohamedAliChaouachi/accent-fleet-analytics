// Schema Explorer — a searchable view of the curated catalog the SQL
// guard whitelists. Mounted in the slide-out (collapsed by default) and
// shown side-by-side on the /ai page.
//
// The list comes from GET /v1/ai/schema, which serializes the in-process
// CATALOG. The response is stable within a deploy, so we cache it
// indefinitely under one React Query key.
//
// Why a curated catalog instead of `information_schema`: the SQL guard
// only admits tables that appear here, so anything information_schema
// would surface beyond this list is unreachable from /ai/query anyway —
// showing it would be misleading.

import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  ChevronRight,
  Database,
  Loader2,
  Search,
  Table as TableIcon,
} from "lucide-react";
import { fetchAISchema, type AISchemaTable } from "@/api/ai";
import { Badge } from "@/components/ui/Badge";
import { Input } from "@/components/ui/Input";
import { cn } from "@/lib/cn";

interface SchemaExplorerProps {
  /** Compact mode trims spacing for the slide-out panel. */
  compact?: boolean;
  className?: string;
  /** Optional click-through: lets a parent insert a hint into the
   * composer ("ask about marts.v_executive_dashboard"). */
  onPickTable?: (fqname: string) => void;
}

export function SchemaExplorer({
  compact = false,
  className,
  onPickTable,
}: SchemaExplorerProps) {
  const [q, setQ] = useState("");
  const [openFq, setOpenFq] = useState<string | null>(null);

  const schemaQ = useQuery({
    queryKey: ["ai", "schema"],
    queryFn: ({ signal }) => fetchAISchema(signal),
    staleTime: Infinity,
    gcTime: Infinity,
  });

  const filtered = useMemo(() => {
    const tables = schemaQ.data?.tables ?? [];
    const needle = q.trim().toLowerCase();
    if (!needle) return tables;
    return tables.filter((t) => matchesTable(t, needle));
  }, [schemaQ.data, q]);

  return (
    <div
      className={cn(
        "flex h-full min-h-0 flex-col bg-card/40",
        className,
      )}
    >
      <div
        className={cn(
          "shrink-0 border-b border-border",
          compact ? "px-3 py-2" : "px-4 py-3",
        )}
      >
        <div className="flex items-center gap-2">
          <Database className="size-4 text-muted-foreground" />
          <h2 className="text-sm font-semibold leading-none text-foreground">
            Schema
          </h2>
          <Badge variant="outline" className="ml-auto text-2xs">
            {schemaQ.data?.tables.length ?? 0} tables
          </Badge>
        </div>
        <div className="mt-2">
          <Input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Filter tables or columns…"
            leadingIcon={<Search />}
            aria-label="Filter schema"
          />
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        {schemaQ.isLoading ? (
          <div className="flex items-center justify-center gap-2 p-6 text-sm text-muted-foreground">
            <Loader2 className="size-4 animate-spin" />
            Loading catalog…
          </div>
        ) : schemaQ.isError ? (
          <p className="p-6 text-sm text-destructive">
            Failed to load the schema. Try refreshing.
          </p>
        ) : filtered.length === 0 ? (
          <p
            className={cn(
              "text-sm text-muted-foreground",
              compact ? "px-3 py-4" : "px-4 py-6",
            )}
          >
            No tables match &ldquo;{q}&rdquo;.
          </p>
        ) : (
          <ul className="flex flex-col">
            {filtered.map((t) => (
              <TableRow
                key={t.fqname}
                table={t}
                open={openFq === t.fqname}
                onToggle={() =>
                  setOpenFq((cur) => (cur === t.fqname ? null : t.fqname))
                }
                onPick={onPickTable ? () => onPickTable(t.fqname) : undefined}
                compact={compact}
              />
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function matchesTable(t: AISchemaTable, needle: string): boolean {
  if (
    t.fqname.toLowerCase().includes(needle) ||
    t.description.toLowerCase().includes(needle)
  ) {
    return true;
  }
  return t.columns.some(
    (c) =>
      c.name.toLowerCase().includes(needle) ||
      c.description.toLowerCase().includes(needle),
  );
}

function TableRow({
  table,
  open,
  onToggle,
  onPick,
  compact,
}: {
  table: AISchemaTable;
  open: boolean;
  onToggle: () => void;
  onPick?: () => void;
  compact: boolean;
}) {
  return (
    <li className="border-b border-border/60 last:border-b-0">
      <button
        type="button"
        onClick={onToggle}
        aria-expanded={open}
        className={cn(
          "flex w-full items-center gap-2 text-left transition-colors hover:bg-secondary/50",
          compact ? "px-3 py-2" : "px-4 py-2.5",
        )}
      >
        <ChevronRight
          className={cn(
            "size-3.5 shrink-0 text-muted-foreground transition-transform",
            open && "rotate-90",
          )}
        />
        <TableIcon className="size-3.5 shrink-0 text-muted-foreground" />
        <span className="min-w-0 flex-1 truncate font-mono text-xs text-foreground">
          {table.fqname}
        </span>
        {table.tenant_scoped ? (
          <Badge variant="outline" className="shrink-0 text-2xs">
            tenant
          </Badge>
        ) : null}
      </button>

      {open ? (
        <div
          className={cn(
            "border-t border-border/60 bg-background/40",
            compact ? "px-3 py-3" : "px-4 py-3",
          )}
        >
          <p className="text-xs leading-relaxed text-muted-foreground">
            {table.description}
          </p>
          <p className="mt-2 text-2xs uppercase tracking-widest text-muted-foreground/70">
            Grain: <span className="normal-case">{table.grain}</span>
          </p>

          <ul className="mt-3 flex flex-col divide-y divide-border/40 rounded-md border border-border/40">
            {table.columns.map((c) => (
              <li
                key={c.name}
                className="grid grid-cols-[minmax(0,1fr)_auto] gap-x-3 px-2.5 py-1.5"
              >
                <span className="truncate font-mono text-xs text-foreground">
                  {c.name}
                </span>
                <span className="shrink-0 font-mono text-2xs text-muted-foreground">
                  {c.type}
                </span>
                {c.description ? (
                  <span className="col-span-2 truncate text-2xs text-muted-foreground">
                    {c.description}
                  </span>
                ) : null}
              </li>
            ))}
          </ul>

          {onPick ? (
            <button
              type="button"
              onClick={onPick}
              className="mt-3 text-xs font-medium text-primary hover:underline"
            >
              Ask about {table.fqname} →
            </button>
          ) : null}
        </div>
      ) : null}
    </li>
  );
}
