import { useMemo } from "react";
import { Info } from "lucide-react";
import { Badge, type BadgeProps } from "@/components/ui/Badge";
import { cn } from "@/lib/cn";

interface ExplanationPanelProps {
  sql: string;
  className?: string;
}

// Lightweight, deterministic SQL explainer.
//
// This is intentionally NOT an LLM call — the backend doesn't expose an
// explanation endpoint, and the cost (latency + tokens) of asking the
// model for one isn't worth it when the SQL itself is right there. We
// parse the query at a coarse level and surface what the user would
// otherwise have to squint to figure out:
//
//   - tables referenced (FROM / JOIN)
//   - filter columns (WHERE / HAVING)
//   - grouping columns
//   - ordering and LIMIT
//
// Edge cases: nested CTEs, subqueries, and DDL all degrade gracefully
// — the parser just returns what it found and the panel renders fewer
// pills. We don't claim this is bullet-proof; the goal is "good enough
// to give the user a mental model of what's about to run."
export function ExplanationPanel({ sql, className }: ExplanationPanelProps) {
  const parsed = useMemo(() => parseSql(sql), [sql]);

  return (
    <div
      className={cn(
        "rounded-md border border-border bg-muted/30 p-3 text-xs",
        className,
      )}
    >
      <div className="mb-2 flex items-center gap-1.5 text-muted-foreground">
        <Info className="size-3.5" />
        <span className="font-medium uppercase tracking-wider">
          What this query does
        </span>
      </div>
      <dl className="space-y-2">
        <Row label="Reads from">
          {parsed.tables.length > 0 ? (
            <ChipList items={parsed.tables} variant="primary" mono />
          ) : (
            <Muted>No tables detected</Muted>
          )}
        </Row>
        {parsed.filters.length > 0 ? (
          <Row label="Filters on">
            <ChipList items={parsed.filters} variant="accent" mono />
          </Row>
        ) : null}
        {parsed.groupBy.length > 0 ? (
          <Row label="Groups by">
            <ChipList items={parsed.groupBy} variant="ai" mono />
          </Row>
        ) : null}
        {parsed.orderBy.length > 0 ? (
          <Row label="Orders by">
            <ChipList items={parsed.orderBy} variant="outline" mono />
          </Row>
        ) : null}
        {parsed.limit ? (
          <Row label="Limit">
            <span className="font-mono text-foreground">{parsed.limit}</span>
          </Row>
        ) : null}
      </dl>
    </div>
  );
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="grid grid-cols-[7rem_1fr] items-start gap-2">
      <dt className="pt-0.5 text-2xs font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </dt>
      <dd className="flex flex-wrap items-center gap-1.5">{children}</dd>
    </div>
  );
}

function Muted({ children }: { children: React.ReactNode }) {
  return <span className="text-muted-foreground">{children}</span>;
}

function ChipList({
  items,
  variant,
  mono,
}: {
  items: ReadonlyArray<string>;
  variant: BadgeProps["variant"];
  mono?: boolean;
}) {
  return (
    <>
      {items.map((s) => (
        <Badge
          key={s}
          variant={variant}
          className={cn(mono && "font-mono normal-case tracking-normal")}
        >
          {s}
        </Badge>
      ))}
    </>
  );
}

interface ParsedSql {
  tables: string[];
  filters: string[];
  groupBy: string[];
  orderBy: string[];
  limit: string | null;
}

function parseSql(raw: string): ParsedSql {
  // Strip comments and normalize whitespace so the regexes below can
  // assume single-spaced uppercase keywords. We don't change the actual
  // identifiers' casing — only the keyword anchors.
  const cleaned = raw
    .replace(/--[^\n]*/g, " ")
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const upper = cleaned.toUpperCase();

  // Tables: anything after FROM or JOIN until the next keyword.
  const tableRe = /(?:FROM|JOIN)\s+([a-zA-Z0-9_."]+)/g;
  const tables = new Set<string>();
  let m: RegExpExecArray | null;
  while ((m = tableRe.exec(upper))) {
    // Pull the matching slice from the original string so casing is
    // preserved (Postgres identifiers are usually lowercase but tenant
    // schemas like "marts.v_executive" benefit from being shown as-is).
    const start = m.index + m[0].indexOf(m[1]!);
    tables.add(cleaned.slice(start, start + m[1]!.length));
  }

  // Filters: column names appearing in WHERE / AND / OR / HAVING up to
  // an operator. Heuristic only — good enough for the explainer.
  const filters = extractColumns(cleaned, upper, /(?:WHERE|AND|OR|HAVING)\s+([a-zA-Z0-9_.]+)\s*(?:=|<|>|<=|>=|<>|!=|LIKE|IN|BETWEEN|IS)/g);

  const groupBy = extractList(cleaned, upper, /GROUP\s+BY\s+([^;]+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s+HAVING|$)/);
  const orderBy = extractList(cleaned, upper, /ORDER\s+BY\s+([^;]+?)(?:\s+LIMIT|$)/);

  const limitMatch = /LIMIT\s+(\d+)/.exec(upper);
  const limit = limitMatch ? limitMatch[1]! : null;

  return {
    tables: [...tables],
    filters,
    groupBy,
    orderBy,
    limit,
  };
}

function extractColumns(
  cleaned: string,
  upper: string,
  re: RegExp,
): string[] {
  const out = new Set<string>();
  let m: RegExpExecArray | null;
  while ((m = re.exec(upper))) {
    const start = m.index + m[0].indexOf(m[1]!);
    out.add(cleaned.slice(start, start + m[1]!.length));
  }
  return [...out];
}

function extractList(cleaned: string, upper: string, re: RegExp): string[] {
  const m = re.exec(upper);
  if (!m) return [];
  const start = m.index + m[0].indexOf(m[1]!);
  const slice = cleaned.slice(start, start + m[1]!.length);
  return slice
    .split(",")
    .map((s) => s.trim())
    // Strip ASC/DESC and a trailing NULLS FIRST/LAST clause for ORDER BY.
    .map((s) => s.replace(/\s+(ASC|DESC|NULLS\s+(FIRST|LAST))(\s+NULLS\s+(FIRST|LAST))?$/i, ""))
    .filter(Boolean);
}
