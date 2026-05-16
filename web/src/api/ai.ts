// Typed client for the Text2SQL assistant.
//
// Backed by POST /v1/ai/query — see docs/ai_query.md for the full contract.
// Two design notes:
//
//   1. `chart_type` is one of four literals (line | bar | pie | table) chosen
//      server-side by a deterministic heuristic over the result shape, so the
//      client can switch on it without re-doing the inference.
//
//   2. The error response is structured: `{ stage, detail, sql? }`. The
//      `stage` discriminator tells the UI how to react (rephrase vs.
//      retry vs. page on-call) without parsing the human-readable message.
//      We surface it as `AIQueryError` so callers can `catch (e) { if
//      (e instanceof AIQueryError) ... }`.

import { ApiError, request } from "./client";

export type AIChartType = "line" | "bar" | "pie" | "table";

export type AIStage =
  | "llm"
  | "sql_guard"
  | "tenant_filter"
  | "execution"
  | "summarization"
  | "config";

export interface AIQueryRequest {
  question: string;
  /** Superadmin-only override. Ignored / rejected for tenant users. */
  tenant_id?: number;
}

export interface AIQueryResponse {
  question: string;
  sql: string;
  /** JSON-coerced rows: every cell is one of string/number/boolean/null. */
  rows: ReadonlyArray<Record<string, unknown>>;
  row_count: number;
  columns: ReadonlyArray<string>;
  summary: string;
  chart_type: AIChartType;
  provider: string;
  model: string;
  elapsed_ms: number;
}

/**
 * Structured error from POST /v1/ai/query. The server returns a body
 * shaped `{ detail: { stage, detail, sql? } }`; we re-throw it as this
 * class so React components can render the offending SQL when the
 * failure happened after generation.
 */
export class AIQueryError extends Error {
  stage: AIStage;
  sql: string | null;
  status: number;

  constructor(stage: AIStage, message: string, sql: string | null, status: number) {
    super(message);
    this.name = "AIQueryError";
    this.stage = stage;
    this.sql = sql;
    this.status = status;
  }
}

/** Re-shape the FastAPI error body into AIQueryError when we can. */
function toAIQueryError(err: unknown): never {
  if (err instanceof ApiError) {
    const body = err.body as { detail?: unknown } | null;
    const detail = body?.detail;
    if (
      detail &&
      typeof detail === "object" &&
      "stage" in detail &&
      typeof (detail as { stage: unknown }).stage === "string"
    ) {
      const d = detail as { stage: AIStage; detail?: string; sql?: string | null };
      throw new AIQueryError(d.stage, d.detail ?? err.message, d.sql ?? null, err.status);
    }
  }
  throw err;
}

export async function askAI(
  req: AIQueryRequest,
  signal?: AbortSignal,
): Promise<AIQueryResponse> {
  try {
    return await request<AIQueryResponse>("/ai/query", {
      method: "POST",
      body: req,
      signal,
    });
  } catch (e) {
    toAIQueryError(e);
  }
}
