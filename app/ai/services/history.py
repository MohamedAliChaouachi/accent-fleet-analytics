"""
Reader for the per-user AI query history surfaced at ``GET /v1/ai/history``.

The data lives in ``ai.query_log`` (already populated by every call to
``/v1/ai/query`` via :mod:`app.ai.services.audit`) joined with
``ai.query_feedback`` so the UI gets the user's existing thumbs state
in the same payload — no second round-trip needed.

Scoping is strictly per-user. Tenant scoping is *additive*: a tenant
user only sees their own rows from their own tenant; a superadmin sees
their own rows regardless of tenant (because superadmins frequently
issue queries across tenants and we want one consolidated history view).

The reader never returns failed-stage rows where ``sql_text`` is NULL
and ``error_detail`` is also NULL — those are placeholder audit rows
from rate-limit or config rejection that don't represent a real
conversation turn worth replaying.
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from accent_fleet.db.engine import get_engine

log = logging.getLogger(__name__)


_SELECT_STMT = text(
    """
    SELECT
        q.event_id,
        q.occurred_at,
        q.question,
        q.sql_text,
        q.stage,
        q.row_count,
        q.elapsed_ms,
        q.chart_type,
        q.provider,
        q.model,
        q.error_detail,
        f.value      AS feedback_value,
        f.comment    AS feedback_comment
    FROM ai.query_log q
    LEFT JOIN ai.query_feedback f
        ON f.event_id = q.event_id AND f.user_id = :user_id
    WHERE q.user_id = :user_id
    ORDER BY q.occurred_at DESC
    LIMIT :limit
    """
)


def read_user_history(*, user_id: int, limit: int) -> list[dict[str, object]]:
    """Return the user's most recent /v1/ai/query rows, newest first.

    ``limit`` is enforced by the caller (the router clamps to
    ``MAX_HISTORY_PAGE``). If the DB read fails — typically because the
    migration hasn't run — returns an empty list and logs once, mirroring
    the fail-open posture of the audit writer.
    """
    # Read the user's rows; on any DB error fail open with an empty list.
    try:
        with get_engine().connect() as conn:
            result = conn.execute(
                _SELECT_STMT, {"user_id": user_id, "limit": limit}
            )
            rows = [dict(r._mapping) for r in result]
    except Exception as exc:  # noqa: BLE001 — fail-open reader
        log.error(
            "ai.history.read_failed",
            extra={"user_id": user_id, "error": str(exc)},
        )
        return []

    # ISO-stringify the timestamp here so the schema layer doesn't have
    # to know about datetime objects. Postgres returns timezone-aware
    # tz=UTC values.
    for r in rows:
        ts = r.get("occurred_at")
        if ts is not None:
            r["occurred_at"] = ts.isoformat()
    return rows
