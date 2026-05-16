// AI chat page — natural language → SQL → rows + summary + chart.
//
// Wire model: POST /v1/ai/query is the only call. The endpoint is
// request/response (not streaming) in Phase 1, so this page is a pure
// useMutation: ask one question, receive one answer. Phase 2 will swap
// the mutation for an SSE stream behind the same UI.
//
// Layout decisions:
//   * Summary up top. That's the user's headline — render it big.
//   * One chart underneath, rendered from the server-chosen `chart_type`.
//     The chart-type heuristic is deterministic (see app/ai/services/
//     chart_suggester.py), so the client never overrides it.
//   * SQL + raw rows are tucked into a <details> so power users / auditors
//     can verify what actually ran, without pushing it on casual users.
//
// Error model: server returns `{ stage, detail, sql? }`. We promote that
// to a typed `AIQueryError` in `api/ai.ts`. UX rule (from docs/ai_query.md):
//   sql_guard / tenant_filter → show inline, prompt user to rephrase.
//   llm                       → 502; show a "try again" hint.
//   execution / config        → generic failure; the operator gets paged
//                               via the access log anyway.

import { useMutation } from "@tanstack/react-query";
import { useRef, useState, type FormEvent } from "react";

import { askAI, AIQueryError, type AIQueryResponse } from "@/api/ai";
import { useAuth } from "@/auth/AuthContext";
import { PageHeader } from "@/components/PageHeader";
import { Panel } from "@/components/Panel";
import { StateMessage } from "@/components/StateMessage";
import { DataTable, type ColumnDef } from "@/components/DataTable";
import { BarChart } from "@/components/charts/BarChart";
import { LineChart } from "@/components/charts/LineChart";
import { PieChart } from "@/components/charts/PieChart";

const EXAMPLE_QUESTIONS: ReadonlyArray<string> = [
  "Show monthly trip volume for the past 12 months.",
  "Top 5 vehicles by total cost last month.",
  "What's the distribution of risk categories across the fleet?",
  "Compare overspeed events per 100 km between this month and last month.",
  "How many devices are in each behaviour cluster?",
];

export function AIChat() {
  const { user } = useAuth();
  const [question, setQuestion] = useState("");
  // Superadmins need to specify a tenant; we expose a small input only
  // for them. Tenant users have their tenant fixed by the JWT.
  const [tenantOverride, setTenantOverride] = useState<string>("");
  // Hold on to the last successful answer so the form area can be
  // re-used without losing the previous result on the screen.
  const lastResponse = useRef<AIQueryResponse | null>(null);

  const mutation = useMutation<AIQueryResponse, Error, void>({
    mutationFn: async () => {
      const tenantId =
        user?.role === "superadmin" && tenantOverride.trim()
          ? Number(tenantOverride.trim())
          : undefined;
      const resp = await askAI({ question, tenant_id: tenantId });
      lastResponse.current = resp;
      return resp;
    },
  });

  function onSubmit(e: FormEvent) {
    e.preventDefault();
    if (!question.trim() || mutation.isPending) return;
    mutation.mutate();
  }

  const response = mutation.data ?? lastResponse.current;
  const isSuperadmin = user?.role === "superadmin";

  return (
    <section>
      <PageHeader
        title="Ask the data"
        caption={
          <>
            Natural-language questions answered from the analytics marts. Every
            query runs in a read-only transaction and is scoped to your tenant.
          </>
        }
      />

      <Panel title="Question">
        <form onSubmit={onSubmit} className="space-y-3">
          <textarea
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            rows={3}
            placeholder="e.g. Show monthly trip volume for the past 12 months."
            className="block w-full resize-y rounded-md border border-slate-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-brand focus:outline-none focus:ring-1 focus:ring-brand"
            disabled={mutation.isPending}
          />

          {isSuperadmin ? (
            <label className="block">
              <span className="mb-1 block text-xs uppercase tracking-wider text-slate-500">
                Tenant id (required for superadmin)
              </span>
              <input
                type="number"
                inputMode="numeric"
                value={tenantOverride}
                onChange={(e) => setTenantOverride(e.target.value)}
                className="block w-32 rounded-md border border-slate-300 bg-white px-2 py-1 text-sm"
                disabled={mutation.isPending}
              />
            </label>
          ) : null}

          <div className="flex items-center gap-3">
            <button
              type="submit"
              disabled={
                mutation.isPending ||
                !question.trim() ||
                (isSuperadmin && !tenantOverride.trim())
              }
              className="rounded-md bg-brand px-4 py-2 text-sm font-medium text-white shadow-sm transition-colors hover:bg-brand-dark disabled:cursor-not-allowed disabled:bg-slate-300"
            >
              {mutation.isPending ? "Thinking…" : "Ask"}
            </button>
            {mutation.isPending ? (
              <span className="text-xs text-slate-500">
                Running through the SQL guard and Postgres — typically 1–3s.
              </span>
            ) : null}
          </div>

          <div className="flex flex-wrap gap-2 pt-1">
            {EXAMPLE_QUESTIONS.map((q) => (
              <button
                key={q}
                type="button"
                onClick={() => setQuestion(q)}
                disabled={mutation.isPending}
                className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1 text-xs text-slate-600 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {q}
              </button>
            ))}
          </div>
        </form>
      </Panel>

      <div className="h-4" />

      {mutation.isError ? <ErrorView error={mutation.error} /> : null}
      {response ? <AnswerView response={response} /> : null}
    </section>
  );
}

// ---------------------------------------------------------------------------
// Answer
// ---------------------------------------------------------------------------

function AnswerView({ response }: { response: AIQueryResponse }) {
  return (
    <div className="space-y-6">
      <Panel title="Summary">
        <blockquote className="border-l-4 border-brand pl-4 text-base text-slate-800">
          {response.summary}
        </blockquote>
        <p className="mt-3 text-xs text-slate-500">
          {response.row_count} {response.row_count === 1 ? "row" : "rows"} ·{" "}
          {response.elapsed_ms} ms · {response.provider}/{response.model}
        </p>
      </Panel>

      <Panel title="Chart" description={`Inferred chart type: ${response.chart_type}.`}>
        <ChartFor response={response} />
      </Panel>

      <Panel
        title="Details"
        description="Generated SQL + raw rows. Useful for verifying what actually ran."
      >
        <details className="text-sm" open={false}>
          <summary className="cursor-pointer text-slate-700">
            Show generated SQL
          </summary>
          <pre className="mt-2 overflow-x-auto rounded-md bg-slate-900 p-3 text-xs text-slate-100">
            <code>{response.sql}</code>
          </pre>
        </details>

        <details className="mt-3 text-sm" open={false}>
          <summary className="cursor-pointer text-slate-700">
            Show raw rows ({response.row_count})
          </summary>
          <div className="mt-2">
            <RowsTable response={response} />
          </div>
        </details>
      </Panel>
    </div>
  );
}

function ChartFor({ response }: { response: AIQueryResponse }): JSX.Element {
  const { chart_type, rows, columns } = response;
  if (rows.length === 0) {
    return <StateMessage>No rows to plot.</StateMessage>;
  }

  // Drop the chart and fall back to the table if we don't have the
  // shape the chart type wants. Defensive — the server-side suggester
  // is supposed to send `"table"` in those cases.
  if (chart_type === "table" || columns.length < 2) {
    return <RowsTable response={response} />;
  }

  const [xKey, yKey] = columns;

  if (chart_type === "line") {
    return (
      <LineChart
        data={rows as Array<Record<string, unknown>>}
        xKey={xKey}
        series={[{ dataKey: yKey, label: yKey }]}
      />
    );
  }

  if (chart_type === "pie") {
    const pieData = rows.map((r) => ({
      name: String(r[xKey] ?? "—"),
      value: Number(r[yKey] ?? 0),
    }));
    return <PieChart data={pieData} />;
  }

  // chart_type === "bar"
  return (
    <BarChart
      data={rows as Array<Record<string, unknown>>}
      xKey={xKey}
      series={[{ dataKey: yKey, label: yKey }]}
      layout={chooseBarLayout(rows.length, xKey, rows)}
    />
  );
}

/**
 * Long category labels (matricules, device ids) are easier to read on
 * the vertical axis. Rule of thumb: more than 6 rows or any x-value
 * longer than 10 chars → switch to horizontal bars (i.e. layout="vertical"
 * in Recharts, which puts categories on the y-axis).
 */
function chooseBarLayout(
  n: number,
  xKey: string,
  rows: ReadonlyArray<Record<string, unknown>>,
): "horizontal" | "vertical" {
  if (n > 6) return "vertical";
  const maxLen = Math.max(...rows.map((r) => String(r[xKey] ?? "").length));
  return maxLen > 10 ? "vertical" : "horizontal";
}

// ---------------------------------------------------------------------------
// Raw-rows table
// ---------------------------------------------------------------------------

function RowsTable({ response }: { response: AIQueryResponse }) {
  const columns: ColumnDef<Record<string, unknown>>[] = response.columns.map(
    (col) => ({
      key: col,
      header: col,
      accessor: (row) => formatCell(row[col]),
      align: typeof response.rows[0]?.[col] === "number" ? "right" : "left",
    }),
  );
  return (
    <DataTable
      rows={response.rows}
      columns={columns}
      rowKey={(_r, i) => i}
      emptyMessage="No rows returned for this question."
    />
  );
}

function formatCell(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number") {
    // Tight default: integers as-is, floats with 2 decimals. Recharts
    // formats its own axis values; this is only for the raw table.
    return Number.isInteger(v) ? String(v) : v.toFixed(2);
  }
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

// ---------------------------------------------------------------------------
// Error rendering — stage-aware
// ---------------------------------------------------------------------------

function ErrorView({ error }: { error: Error }) {
  if (error instanceof AIQueryError) {
    return <TypedErrorView err={error} />;
  }
  return (
    <Panel title="Error">
      <StateMessage tone="error">{error.message}</StateMessage>
    </Panel>
  );
}

function TypedErrorView({ err }: { err: AIQueryError }) {
  const hint = stageHint(err.stage);
  return (
    <Panel title="Couldn't answer that">
      <StateMessage tone="error">
        <strong className="block">{prettyStage(err.stage)}</strong>
        <span className="mt-1 block whitespace-pre-wrap text-sm text-slate-700">
          {err.message}
        </span>
        {hint ? <span className="mt-2 block text-xs text-slate-500">{hint}</span> : null}
      </StateMessage>
      {err.sql ? (
        <details className="mt-3 text-sm" open={false}>
          <summary className="cursor-pointer text-slate-700">
            Offending SQL (rejected before execution)
          </summary>
          <pre className="mt-2 overflow-x-auto rounded-md bg-slate-900 p-3 text-xs text-slate-100">
            <code>{err.sql}</code>
          </pre>
        </details>
      ) : null}
    </Panel>
  );
}

function prettyStage(stage: AIQueryError["stage"]): string {
  switch (stage) {
    case "sql_guard":
      return "SQL was unsafe and rejected.";
    case "tenant_filter":
      return "Tenant check failed.";
    case "llm":
      return "Upstream LLM provider failed.";
    case "execution":
      return "Database error while running the query.";
    case "summarization":
      return "Could not summarise the result.";
    case "config":
      return "AI assistant is not configured.";
  }
}

function stageHint(stage: AIQueryError["stage"]): string | null {
  switch (stage) {
    case "sql_guard":
    case "tenant_filter":
      return "Try rephrasing the question — name the metric and the time window explicitly.";
    case "llm":
      return "Transient upstream issue; try again in a moment.";
    case "execution":
    case "summarization":
    case "config":
      return null;
  }
}

