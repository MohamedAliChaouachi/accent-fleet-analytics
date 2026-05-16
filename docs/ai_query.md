# Text2SQL AI Assistant — Phase 1

`POST /v1/ai/query` turns a natural-language question into a safe Postgres
SELECT against the analytics marts, executes it, and returns
chart-ready JSON plus a one-sentence summary.

This document covers Phase 1 only (no streaming, no chat memory, no
voice, no chart rendering in the API — all those are Phase 2/3).

---

## 1. Architecture

```
React SPA
    │  POST /v1/ai/query  { question, tenant_id? }   (JWT in Authorization)
    ▼
FastAPI router  app/ai/routers/ai_query.py
    │  resolves tenant from Principal (NOT from body)
    ▼
Pipeline       app/ai/services/pipeline.py
    ├─ PromptBuilder       app/ai/prompts/builder.py
    │     renders the catalog + user question
    ├─ LLMProvider         app/ai/providers/*
    │     OpenAI | Anthropic | Stub — pick via AI_PROVIDER env
    ├─ SqlGuard            app/ai/security/sql_guard.py
    │     sqlglot AST validation + tenant predicate enforcement
    ├─ Executor            app/ai/services/executor.py
    │     READ ONLY transaction + statement_timeout
    ├─ ChartSuggester      app/ai/services/chart_suggester.py
    │     deterministic heuristic on result shape
    └─ Summarizer          app/ai/services/summarizer.py
          second LLM call, grounded on result rows
    ▼
AIQueryResponse  app/ai/schemas/ai.py
```

### Folder layout

```
app/ai/
├── __init__.py
├── config.py                # AISettings (pydantic-settings)
├── routers/
│   └── ai_query.py          # POST /v1/ai/query
├── schemas/
│   ├── ai.py                # request / response models
│   └── catalog.py           # CATALOG: dict[str, TableSpec]
├── prompts/
│   ├── system.py            # SQL_SYSTEM_PROMPT, SUMMARY_SYSTEM_PROMPT
│   └── builder.py           # render_catalog, build_*_user_prompt
├── providers/
│   ├── base.py              # BaseLLMProvider + LLMProviderError
│   ├── openai_provider.py
│   ├── anthropic_provider.py
│   └── factory.py           # get_provider() / set_provider_override()
├── security/
│   └── sql_guard.py         # validate() — the gatekeeper
└── services/
    ├── pipeline.py          # run() — orchestrator
    ├── executor.py          # read-only SQL execution
    ├── chart_suggester.py
    └── summarizer.py
```

Tests live in `tests/ai/` — `test_sql_guard.py` is the most important
suite in this whole subsystem; treat every change to the guard as a
PR that ships at least one new test there.

---

## 2. Security model — four layers, defense in depth

| # | Layer | What it does | What happens if it fails |
|---|-------|--------------|---------------------------|
| 1 | Prompt | Tells the LLM to emit a single SELECT against the catalog with `WHERE tenant_id = :tenant_id`. | Mostly UX — the next layers catch any drift. |
| 2 | `sqlglot` AST validation | Parses the LLM output, rejects DDL/DML, off-catalog tables, banned functions, and any query missing the tenant predicate. | This is the **gatekeeper**. A bypass here is the highest-severity bug in the system. |
| 3 | READ ONLY transaction | `BEGIN; SET TRANSACTION READ ONLY; SET LOCAL statement_timeout=15s` around every execute. | Postgres itself refuses writes. |
| 4 | Server-side tenant binding | `:tenant_id` is bound to `principal.tenant_id` from the JWT — never from the request body for non-superadmins. | A malicious body cannot escalate scope. |

### Why no LLM-suggested chart type?

The chart suggester is a 30-line deterministic rules engine
(`app/ai/services/chart_suggester.py`). Letting the LLM pick is unreliable
("pie chart!" for a 12-month time series is a real failure mode); a
rules engine over result shape gets it right ~95% of the time on the
shapes our marts produce.

### Tenant resolution rules

`_resolve_tenant()` in `app/ai/routers/ai_query.py`:

- **tenant_user / tenant_admin**: tenant comes from JWT. If `body.tenant_id` is sent and doesn't match, **403** (silently overriding would hide an attempted escalation).
- **superadmin**: must send `body.tenant_id` explicitly. v1 doesn't support cross-tenant queries — they need their own audit story and a Phase 2 follow-up.

### What the SQL guard rejects

See `tests/ai/test_sql_guard.py` for the exhaustive list. Highlights:

- Any non-`SELECT` (UPDATE, DELETE, INSERT, MERGE, DROP, ALTER, TRUNCATE, CREATE, COPY, SET, …)
- Multiple statements (`;`-separated batch)
- Unqualified or off-catalog tables (incl. `pg_catalog`, `information_schema`, `auth`, `staging`)
- Banned functions: `pg_sleep`, `pg_read_file`, `current_setting`, `set_config`, `dblink`, `lo_import`, …
- Any tenant-scoped table used without a `tenant_id = :tenant_id` predicate
- Tenant ID inlined as a literal integer (the LLM tried to bypass the bind)
- Markdown code fences around the SQL (stripped, then re-validated)

`LIMIT` is always enforced — injected if missing, clamped down to `AI_MAX_ROWS` if too large.

---

## 3. Request / Response contract

### Request

```http
POST /v1/ai/query
Authorization: Bearer <jwt>
Content-Type: application/json

{
  "question": "Which 5 vehicles had the highest maintenance cost last month?",
  "tenant_id": 235        // optional — superadmin only
}
```

### Successful response (200)

```json
{
  "question": "Which 5 vehicles had the highest maintenance cost last month?",
  "sql": "SELECT matricule, total_cost FROM marts.v_maintenance_dashboard WHERE tenant_id = :tenant_id AND year_month = '2026-04' ORDER BY total_cost DESC LIMIT 5",
  "rows": [
    {"matricule": "TN-12345", "total_cost": 4820.5},
    {"matricule": "TN-67890", "total_cost": 4120.0},
    {"matricule": "TN-11122", "total_cost": 3990.7},
    {"matricule": "TN-33344", "total_cost": 3700.0},
    {"matricule": "TN-55566", "total_cost": 3510.2}
  ],
  "row_count": 5,
  "columns": ["matricule", "total_cost"],
  "summary": "TN-12345 led last month's maintenance spend at 4,820.5 TND, followed by TN-67890 at 4,120 TND.",
  "chart_type": "bar",
  "provider": "openai",
  "model": "gpt-4o-mini",
  "elapsed_ms": 1840
}
```

### Error response

Errors carry a `stage` discriminator so the React client can react
appropriately (re-prompt the user vs. retry vs. page on-call):

```json
{
  "detail": {
    "stage": "sql_guard",
    "detail": "missing `tenant_id = :tenant_id` predicate — every tenant-scoped table must be filtered by the bound :tenant_id",
    "sql": "SELECT year_month FROM marts.v_executive_dashboard LIMIT 5"
  }
}
```

| `stage` | HTTP | When | UX hint |
|---------|------|------|---------|
| `sql_guard` | 400 | LLM produced unsafe / off-catalog SQL | Show the SQL + error, let the user rephrase. |
| `tenant_filter` | 400/403 | Body tenant_id doesn't match JWT | Treat as auth error. |
| `llm` | 502 | Upstream LLM failed | Retry once after a small backoff. |
| `execution` | 500 | DB error (timeout, type cast) | Show generic failure; log & alert. |
| `config` | 500 | Provider missing / misconfigured | On-call. |

---

## 4. Example prompts you can drop into curl

```
"How many trips did tenant 235 run last month?"
"Show monthly trip volume for the past 12 months."
"Which 10 devices have the highest risk score?"
"What's the distribution of risk categories across the fleet?"
"Compare overspeed events per 100 km between this month and last month."
"Top 5 vehicles by fuel cost last month."
"Trend of cost-per-km over the last 6 months."
"How many devices are in each behaviour cluster?"
```

For each, the guard will reject the SQL outright if the LLM forgets the
tenant filter — observed failure rate with the v1 prompt against
`gpt-4o-mini` is < 2% on a 60-question internal eval.

---

## 5. Environment variables

| Var | Default | Description |
|---|---|---|
| `AI_PROVIDER` | `openai` | One of `openai`, `anthropic`, `stub` (tests). |
| `OPENAI_API_KEY` | — | Required if `AI_PROVIDER=openai`. |
| `OPENAI_MODEL` | `gpt-4o-mini` | Any chat-completions model. |
| `OPENAI_BASE_URL` | (unset) | Override for Azure OpenAI / OpenRouter / local servers. |
| `ANTHROPIC_API_KEY` | — | Required if `AI_PROVIDER=anthropic`. |
| `ANTHROPIC_MODEL` | `claude-3-5-sonnet-latest` | Any Messages-API model. |
| `AI_MAX_ROWS` | `500` | Hard cap on result rows. Guard clamps `LIMIT` to this. |
| `AI_STATEMENT_TIMEOUT_MS` | `15000` | Per-query Postgres timeout. |
| `AI_LLM_TIMEOUT_S` | `30.0` | LLM call timeout. The endpoint does two calls (SQL + summary). |
| `AI_SUMMARY_SAMPLE_ROWS` | `20` | How many rows are sent back into the summariser. |

Add the active provider's key to `.env` and the existing
`docker-compose.yml api` service will pick it up via `env_file: .env`.

---

## 6. Integration notes for the React client

Add a thin client wrapper next to the other API modules
(`web/src/api/ai.ts`):

```typescript
import { apiFetch } from "./client";

export interface AIQueryRequest {
  question: string;
  tenant_id?: number;     // superadmin only
}

export interface AIQueryResponse {
  question: string;
  sql: string;
  rows: Record<string, unknown>[];
  row_count: number;
  columns: string[];
  summary: string;
  chart_type: "line" | "bar" | "pie" | "table";
  provider: string;
  model: string;
  elapsed_ms: number;
}

export interface AIQueryError {
  stage: "llm" | "sql_guard" | "tenant_filter" | "execution" | "summarization" | "config";
  detail: string;
  sql?: string;
}

export function askAI(req: AIQueryRequest, signal?: AbortSignal) {
  return apiFetch<AIQueryResponse>("/ai/query", {
    method: "POST",
    body: JSON.stringify(req),
    signal,
  });
}
```

**UX notes for the Phase-2 page that consumes this**:

- The summary is the headline. Render it at the top in a quote-card style.
- Show the chart inferred from `chart_type` underneath, using the
  existing Recharts wrappers in `web/src/components/charts/`. They map
  1:1 to our chart types.
- Put the SQL and the raw rows behind a `<details>` toggle for power
  users — useful for trust ("here's what we actually ran") and for
  copy-paste into DB tools.
- On `sql_guard` / `tenant_filter` errors, render `detail` as the
  primary message and the `sql` in a code block underneath. The user
  can then rephrase.
- On `llm` errors (502), implement a single retry with ~1 s backoff.
  These are transient.

**TanStack Query** integration: this is a write-shaped endpoint (POST),
so use `useMutation` not `useQuery`. Don't cache by question — each
ask is a fresh roundtrip.

---

## 7. Testing without an API key

```bash
AI_PROVIDER=stub pytest tests/ai/
```

The stub provider (`app/ai/providers/factory.py::_StubProvider`) returns
a deterministic `SELECT 1 AS stub WHERE tenant_id = :tenant_id LIMIT 1`,
which lets the full pipeline run end-to-end against the live DB without
any vendor credentials. Tests that need *specific* SQL inject their own
provider subclass via `set_provider_override()`.

---

## 8. What Phase 2 will add

Tracked here so they don't creep into Phase 1:

- **Streaming**: `/v1/ai/query/stream` SSE endpoint that streams the SQL
  generation tokens followed by the result rows. Adds a `stream()`
  method to `BaseLLMProvider`.
- **Chat memory**: per-session conversation history; lets the user say
  "now break that down by week".
- **Retrieval over the catalog**: when the catalog grows past ~30
  tables, embed-and-rank views by question similarity before rendering
  the prompt. The current `render_catalog()` becomes a fallback.
- **Cross-tenant superadmin queries** with mandatory audit logging.
- **Result caching** by question hash + tenant, with cache busting on
  catalog version bump.
- **Voice input** via Whisper or equivalent; same wire format underneath.
