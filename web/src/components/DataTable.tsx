import type { ReactNode } from "react";
import { cn } from "@/lib/cn";

export interface ColumnDef<Row> {
  key: string;
  header: string;
  accessor: (row: Row) => ReactNode;
  align?: "left" | "right";
}

interface DataTableProps<Row> {
  rows: ReadonlyArray<Row>;
  columns: ReadonlyArray<ColumnDef<Row>>;
  rowKey: (row: Row, index: number) => string | number;
  emptyMessage?: string;
  /** Adds zebra striping for long tables. Defaults true. */
  striped?: boolean;
  /** Max visual height — internal scroller. Defaults to no cap. */
  maxHeight?: string;
}

export function DataTable<Row>({
  rows,
  columns,
  rowKey,
  emptyMessage = "No rows to display.",
  striped = true,
  maxHeight,
}: DataTableProps<Row>) {
  if (rows.length === 0) {
    return (
      <p className="px-3 py-6 text-center text-xs text-muted-foreground">
        {emptyMessage}
      </p>
    );
  }
  return (
    <div
      className="overflow-auto"
      style={maxHeight ? { maxHeight } : undefined}
    >
      <table className="min-w-full text-sm">
        <thead className="sticky top-0 z-10 bg-muted/60 backdrop-blur">
          <tr className="border-b border-border text-left text-2xs font-semibold uppercase tracking-wider text-muted-foreground">
            {columns.map((c) => (
              <th
                key={c.key}
                className={cn(
                  "px-3 py-2",
                  c.align === "right" ? "text-right" : "text-left",
                )}
              >
                {c.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr
              key={rowKey(row, idx)}
              className={cn(
                "border-b border-border/40 last:border-b-0 text-foreground transition-colors hover:bg-secondary/40",
                striped && "even:bg-muted/20",
              )}
            >
              {columns.map((c) => (
                <td
                  key={c.key}
                  className={cn(
                    "px-3 py-2",
                    c.align === "right" ? "text-right num" : "",
                  )}
                >
                  {c.accessor(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
