"""
Best-effort audit-log writer for /v1/ai/query.

One function: :func:`write_ai_query_event`. Inserts one row into
``ai.query_log`` per request — success or failure. Modeled on the
``auth.audit_log`` writer in :mod:`app.auth.audit`:

  - Opens its own short-lived connection so the row commits even if the
    request's outer transaction rolls back.
  - **Never raises.** Any DB error is logged and swallowed; an audit
    failure must not turn a successful query into a 500.
  - Takes scalar columns rather than a dict so each call site is forced
    to think about what it's recording, and so a column rename surfaces
    as a type-checker / test failure instead of a silent NULL.

The migration that creates ``ai.query_log`` is ``sql/60_ai_query_log.sql``.
If the table is missing (the migration hasn't been applied yet), the
writer logs ``ai.audit.write_failed`` once per call and the endpoint keeps
working — same fail-open posture as the auth audit log.
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from accent_fleet.db.engine import get_engine

log = logging.getLogger(__name__)


# The canonical set of stage values the writer will accept. Matches
# Stage in app/ai/services/pipeline.py plus 'success' (happy path) and
# 'rate_limited' (which is enforced above the pipeline). Kept as a tuple
# rather than an Enum so callers can pass a literal string.
STAGES: tuple[str, ...] = (
    "success",
    "llm",
    "sql_guard",
    "tenant_filter",
    "execution",
    "summarization",
    "config",
    "rate_limited",
)


_INSERT_STMT = text(
    """
    INSERT INTO ai.query_log
        (user_id, tenant_id, question, sql_text, stage,
         row_count, elapsed_ms, chart_type, provider, model, error_detail)
    VALUES
        (:user_id, :tenant_id, :question, :sql_text, :stage,
         :row_count, :elapsed_ms, :chart_type, :provider, :model, :error_detail)
    RETURNING event_id
    """
)


def write_ai_query_event(
    *,
    user_id: int,
    tenant_id: int | None,
    question: str,
    stage: str,
    sql_text: str | None = None,
    row_count: int | None = None,
    elapsed_ms: int | None = None,
    chart_type: str | None = None,
    provider: str | None = None,
    model: str | None = None,
    error_detail: str | None = None,
) -> int | None:
    """Insert one row into ``ai.query_log``. Best-effort — never raises.

    On success ``stage='success'`` and ``error_detail`` is None. On
    failure ``stage`` is the pipeline stage that failed and the response
    fields (``row_count``, ``chart_type``, ``model``) may be None.

    Returns the inserted ``event_id``, or ``None`` if the audit write
    failed. The router uses this so the AI response can carry the
    event_id back to the client (for /ai/feedback follow-ups). None is
    fine — the response is still valid; the client just can't leave
    feedback on a query whose audit write was dropped.

    The writer never propagates a DB error: an audit failure must not
    turn a successful /ai/query into a 500. Operators should monitor
    the ``ai.audit.write_failed`` log line to notice gaps.
    """
    # Warn (but still write) on a stage value this writer doesn't know.
    if stage not in STAGES:
        # Don't reject — we want the row even if the caller passes a
        # newly-added stage that this writer hasn't been updated for.
        log.warning("ai.audit.unknown_stage", extra={"stage": stage})

    # Assemble the bind parameters for the single INSERT.
    params = {
        "user_id": user_id,
        "tenant_id": tenant_id,
        # Defense against a runaway prompt: hard-cap what we record at
        # 4× the request's max_length. The router validator already caps
        # incoming questions to 2000 chars, so this is belt-and-braces.
        "question": question[:8000],
        "sql_text": sql_text,
        "stage": stage,
        "row_count": row_count,
        "elapsed_ms": elapsed_ms,
        "chart_type": chart_type,
        "provider": provider,
        "model": model,
        "error_detail": error_detail,
    }

    # Commit on a dedicated connection so the row survives request rollback.
    try:
        with get_engine().begin() as conn:
            row = conn.execute(_INSERT_STMT, params).first()
            return int(row[0]) if row is not None else None
    except Exception as exc:  # noqa: BLE001 — fail-open audit writer
        # Log loud so an operator notices the gap. Don't re-raise: by
        # contract, the audit log can never break the endpoint.
        log.error(
            "ai.audit.write_failed",
            extra={
                "stage": stage,
                "user_id": user_id,
                "tenant_id": tenant_id,
                "error": str(exc),
            },
        )
        return None
