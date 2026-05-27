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

/**
 * One prior turn of the current chat session. Sent back to the API so
 * the model can resolve follow-ups like "and last week?" against
 * earlier exchanges. Assistant `content` should be the summary text
 * only (not SQL/rows) — the backend doesn't reuse prior SQL.
 */
export interface ChatTurn {
  role: "user" | "assistant";
  content: string;
}

/** Server caps history to the last 6 turns; we trim client-side to match. */
export const MAX_HISTORY_TURNS = 6;

export interface AIQueryRequest {
  question: string;
  /** Superadmin-only override. Ignored / rejected for tenant users. */
  tenant_id?: number;
  /** Prior turns, oldest first. Optional — empty = one-shot Q&A. */
  history?: ReadonlyArray<ChatTurn>;
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
  /**
   * Audit log row id this answer was recorded under. Required to POST
   * feedback on this specific exchange. May be null if the server's
   * audit writer failed (fail-open) — in that case the UI hides the
   * feedback buttons for that message.
   */
  event_id: number | null;
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

// ---------------------------------------------------------------------------
// History (GET /v1/ai/history)
// ---------------------------------------------------------------------------

/**
 * One row of the caller's own past /ai/query audit log, joined with any
 * feedback they've left. `event_id` is stable and FK-safe for POSTing
 * feedback back. `feedback_value` is null when no feedback was left.
 */
export interface AIHistoryItem {
  event_id: number;
  occurred_at: string; // ISO-8601 UTC
  question: string;
  sql: string | null;
  stage: string;
  row_count: number | null;
  elapsed_ms: number | null;
  chart_type: AIChartType | null;
  provider: string | null;
  model: string | null;
  error_detail: string | null;
  feedback_value: -1 | 1 | null;
  feedback_comment: string | null;
}

export interface AIHistoryResponse {
  items: ReadonlyArray<AIHistoryItem>;
}

/** Server caps at 100; the default fits the sidebar without paging. */
export async function fetchAIHistory(
  limit = 50,
  signal?: AbortSignal,
): Promise<AIHistoryResponse> {
  return request<AIHistoryResponse>(`/ai/history?limit=${limit}`, {
    method: "GET",
    signal,
  });
}

// ---------------------------------------------------------------------------
// Feedback (POST /v1/ai/feedback)
// ---------------------------------------------------------------------------

export type AIFeedbackValue = -1 | 1;

export interface AIFeedbackRequest {
  /** event_id from an AIQueryResponse — must belong to the caller. */
  event_id: number;
  value: AIFeedbackValue;
  comment?: string | null;
}

export interface AIFeedbackResponse {
  feedback_id: number;
  event_id: number;
  value: AIFeedbackValue;
  comment: string | null;
  created_at: string;
}

/**
 * Upsert the caller's feedback on a past /ai/query exchange. The server
 * upserts on (user_id, event_id), so calling this twice with different
 * values flips the vote in place rather than appending.
 *
 * Returns 404 if the event_id doesn't belong to the caller (stale local
 * state — the caller should drop its cached thumbs and refetch /history).
 */
export async function postAIFeedback(
  req: AIFeedbackRequest,
  signal?: AbortSignal,
): Promise<AIFeedbackResponse> {
  return request<AIFeedbackResponse>("/ai/feedback", {
    method: "POST",
    body: req,
    signal,
  });
}

// ---------------------------------------------------------------------------
// Schema explorer (GET /v1/ai/schema)
// ---------------------------------------------------------------------------

export interface AISchemaColumn {
  name: string;
  type: string;
  description: string;
}

export interface AISchemaTable {
  fqname: string;
  schema_name: string;
  name: string;
  description: string;
  grain: string;
  tenant_scoped: boolean;
  columns: ReadonlyArray<AISchemaColumn>;
}

export interface AISchemaResponse {
  tables: ReadonlyArray<AISchemaTable>;
}

/**
 * Snapshot of the curated catalog the SQL guard admits. Stable within a
 * deploy; cache aggressively (e.g. React Query `staleTime: Infinity`).
 */
export async function fetchAISchema(
  signal?: AbortSignal,
): Promise<AISchemaResponse> {
  return request<AISchemaResponse>("/ai/schema", {
    method: "GET",
    signal,
  });
}
