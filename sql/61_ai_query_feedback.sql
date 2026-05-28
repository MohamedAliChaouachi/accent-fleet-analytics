-- =============================================================================
-- 61_ai_query_feedback.sql
-- =============================================================================
-- User feedback on AI assistant answers (thumbs up/down + optional comment).
--
-- One row per (user, query) pair. We upsert because users frequently flip
-- their vote (changed their mind, re-read the answer) and we only care
-- about the latest signal — historical flips are not load-bearing for
-- model evaluation.
--
-- Why a separate table and not a column on ai.query_log?
--
--   * Different write path. query_log is written synchronously by the
--     audit writer in the request lifecycle; feedback arrives minutes
--     to hours later (or never). Coupling them would require an UPDATE
--     against a hot insert-only table.
--
--   * Different grant model. accent_app needs UPDATE on feedback to
--     flip votes, but only INSERT/SELECT on query_log (audit integrity).
--
--   * Different retention. Feedback is small and we want to keep it
--     indefinitely for model training; query_log may be aged out.
--
-- This file is idempotent — safe to re-run.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS ai;

-- -----------------------------------------------------------------------------
-- ai.query_feedback
-- -----------------------------------------------------------------------------
-- `value` is a smallint with a CHECK constraint instead of an ENUM so
-- adding a "neutral" or "flag" later is a one-line migration, not a
-- type-altering one. Today only -1 (down) and 1 (up) are accepted.
--
-- `event_id` is the FK to ai.query_log; we do NOT enforce it via
-- REFERENCES because:
--   1. The audit writer is fail-open — a feedback row can legitimately
--      exist for a query whose audit insert failed.
--   2. accent_etl owns retention on query_log and may delete old rows;
--      a strict FK would either cascade-delete feedback (losing signal)
--      or block retention (operational pain). We accept the dangling
--      reference and treat it the same way ai.query_log treats dangling
--      user_id — documented end-state, not a bug.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai.query_feedback (
    feedback_id   BIGSERIAL PRIMARY KEY,

    -- The query being rated. Not a FK; see header rationale.
    event_id      BIGINT NOT NULL,

    -- Who voted. user_id is required (anonymous feedback would have no
    -- value for follow-up). tenant_id mirrors query_log so per-tenant
    -- rollups stay cheap.
    user_id       BIGINT NOT NULL,
    tenant_id     INTEGER,

    -- -1 = thumbs down, 1 = thumbs up. CHECK keeps junk out.
    value         SMALLINT NOT NULL CHECK (value IN (-1, 1)),

    -- Optional free-text comment. Bounded to a sane size at the
    -- application layer (pydantic max_length=2000) so we don't need
    -- a domain here.
    comment       TEXT,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- One row per (user, query). Re-voting upserts in place via
    -- ON CONFLICT (user_id, event_id) DO UPDATE in the writer.
    CONSTRAINT uq_ai_query_feedback_user_event UNIQUE (user_id, event_id)
);

-- "Recent feedback across the system" — operator dashboard / model eval.
CREATE INDEX IF NOT EXISTS ix_ai_query_feedback_created_at
    ON ai.query_feedback (created_at DESC);

-- "Per-tenant sentiment" — usage reporting.
CREATE INDEX IF NOT EXISTS ix_ai_query_feedback_tenant_created
    ON ai.query_feedback (tenant_id, created_at DESC);

-- "All feedback from user X" — incident response and model fairness audit.
CREATE INDEX IF NOT EXISTS ix_ai_query_feedback_user_created
    ON ai.query_feedback (user_id, created_at DESC);

-- "What did people downvote?" — failure-mode triage. Partial index on the
-- low-cardinality minority (downvotes), which keeps it small.
CREATE INDEX IF NOT EXISTS ix_ai_query_feedback_downvotes
    ON ai.query_feedback (event_id) WHERE value = -1;

-- -----------------------------------------------------------------------------
-- Grants
-- -----------------------------------------------------------------------------
-- accent_app upserts (INSERT + UPDATE on conflict) and reads to surface
-- a user's own past votes. accent_etl gets full access for retention and
-- model-training exports.
-- -----------------------------------------------------------------------------
GRANT INSERT, SELECT, UPDATE ON ai.query_feedback TO accent_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ai.query_feedback TO accent_etl;

GRANT USAGE, SELECT ON SEQUENCE ai.query_feedback_feedback_id_seq
    TO accent_app, accent_etl;
