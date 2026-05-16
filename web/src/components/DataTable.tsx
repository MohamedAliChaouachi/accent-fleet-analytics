import type { ReactNode } from "react";

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
}

export function DataTable<Row>({
  rows,
  columns,
  rowKey,
  emptyMessage = "No rows to display.",
}: DataTableProps<Row>) {
  if (rows.length === 0) {
    return <p className="text-sm text-slate-500">{emptyMessage}</p>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wider text-slate-500">
            {columns.map((c) => (
              <th
                key={c.key}
                className={`px-2 py-2 ${c.align === "right" ? "text-right" : "text-left"}`}
              >
                {c.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr key={rowKey(row, idx)} className="border-b border-slate-100 last:border-b-0">
              {columns.map((c) => (
                <td
                  key={c.key}
                  className={`px-2 py-1.5 text-slate-700 ${c.align === "right" ? "text-right tabular-nums" : ""}`}
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
