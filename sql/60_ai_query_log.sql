-- =============================================================================
-- 60_ai_query_log.sql
-- =============================================================================
-- Audit log for the Text2SQL assistant (POST /v1/ai/query).
--
-- Why a dedicated table and not auth.audit_log?
--
--   * Volume and lifecycle are different. auth.audit_log captures rare
--     security-relevant events (login, refresh, admin actions); ai.query_log
--     captures one row per /ai/query request and grows with feature usage.
--     Mixing them would make the access patterns conflict on the same
--     indexes and force the same retention policy on both.
--
--   * Different fields. The interesting columns for an AI request — the
--     generated SQL text, the elapsed_ms, the model and provider — have
--     no analogue in auth events; squashing them into a single JSONB
--     `detail` column would lose the ability to GROUP BY model or by
--     guard-rejection stage without runtime jsonb_path queries.
--
-- This file is idempotent: CREATE SCHEMA / CREATE TABLE / CREATE INDEX
-- all use IF NOT EXISTS so it is safe to re-run.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS ai;

-- -----------------------------------------------------------------------------
-- ai.query_log
-- -----------------------------------------------------------------------------
-- One row per /v1/ai/query request.
--
-- `stage` is the discriminator that mirrors PipelineError.stage in
-- app/ai/services/pipeline.py, plus the literal 'success' for the happy
-- path. We keep it as TEXT (not an ENUM) so adding a new stage in the
-- pipeline never requires a schema migration.
--
-- `sql_text` is NULLABLE because failures at the LLM stage never produce
-- any SQL, and we want one row per request regardless of which stage
-- failed (so a SELECT COUNT(*) gives an honest request volume).
--
-- `error_detail` is the human-readable failure message captured at the
-- failing stage. It's TEXT (not JSONB) because the call sites in the
-- pipeline already produce a clean one-line string and structuring it
-- further would be busywork — query the access log if you need the
-- full request envelope.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai.query_log (
    event_id      BIGSERIAL PRIMARY KEY,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Who. user_id may be NULL only if the entry was written outside an
    -- authenticated context, which shouldn't happen via the router (it
    -- requires a Principal) but is allowed defensively so a buggy call
    -- site can't crash on a NOT NULL violation.
    user_id       BIGINT REFERENCES auth.users(user_id),
    tenant_id     INTEGER,

    -- What was asked, what we ran. `question` is bounded to AIQueryRequest's
    -- pydantic max_length=2000; SQL is uncapped to keep room for CTEs.
    question      TEXT NOT NULL,
    sql_text      TEXT,

    -- Outcome. 'success' or one of: llm | sql_guard | tenant_filter |
    -- execution | summarization | config | rate_limited.
    stage         TEXT NOT NULL,
    row_count     INTEGER,
    elapsed_ms    INTEGER,
    chart_type    TEXT,

    -- Provenance — useful for cost reporting and model A/B reviews.
    provider      TEXT,
    model         TEXT,

    -- Failure detail, NULL on success.
    error_detail  TEXT
);

-- "Recent activity across the system" — operator dashboard query.
CREATE INDEX IF NOT EXISTS ix_ai_query_log_occurred_at
    ON ai.query_log (occurred_at DESC);

-- "Show me what tenant X asked this week" — per-tenant audit.
CREATE INDEX IF NOT EXISTS ix_ai_query_log_tenant_occurred
    ON ai.query_log (tenant_id, occurred_at DESC);

-- "Did user Y abuse the endpoint?" — incident response. Same shape as
-- ix_auth_audit_log_user_occurred in sql/50_auth.sql for consistency.
CREATE INDEX IF NOT EXISTS ix_ai_query_log_user_occurred
    ON ai.query_log (user_id, occurred_at DESC);

-- "What's our guard-rejection rate by stage?" — eval / prompt-quality
-- monitoring. Stage is low-cardinality so the index is small.
CREATE INDEX IF NOT EXISTS ix_ai_query_log_stage_occurred
    ON ai.query_log (stage, occurred_at DESC);
