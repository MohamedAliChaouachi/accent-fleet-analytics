"""
Writer for ``POST /v1/ai/feedback`` — user thumbs-up/down on a past query.

Why upsert and not append?

  The UI exposes one thumbs button per assistant message. Users
  frequently flip their vote (re-read the answer, change their mind).
  Treating each flip as a new row would (a) clutter model-training
  exports with stale signal and (b) require de-duplication on read.
  An upsert keyed on ``(user_id, event_id)`` keeps "latest vote wins"
  semantics with no read-side bookkeeping.

The writer enforces user ownership of the feedback target: a user can
only leave feedback on their own query rows. This matters because the
``event_id`` comes from the request body — without the check, a user
could spoof feedback against another user's queries. We verify the
ownership in the same transaction as the upsert so there's no TOCTOU
window.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

from accent_fleet.db.engine import get_engine

log = logging.getLogger(__name__)


class FeedbackError(Exception):
    """Domain error from the feedback writer.

    ``kind`` is one of:
      - 'not_found'   — event_id does not exist or doesn't belong to user
      - 'db'          — unexpected database failure
    """

    def __init__(self, kind: str, detail: str) -> None:
        super().__init__(detail)
        self.kind = kind
        self.detail = detail


# Verify the user owns the query they're rating. We pull tenant_id back
# at the same time so we can stamp the feedback row without a second
# round-trip — keeps the feedback row's tenant_id consistent with the
# original query even if the user has since switched tenants.
_OWNERSHIP_STMT = text(
    """
    SELECT tenant_id
    FROM ai.query_log
    WHERE event_id = :event_id AND user_id = :user_id
    """
)


# Upsert on the unique (user_id, event_id) constraint. ON CONFLICT does
# UPDATE so a re-vote flips the value in place; updated_at is bumped
# so model-eval queries can ORDER BY "most recent signal first".
_UPSERT_STMT = text(
    """
    INSERT INTO ai.query_feedback
        (event_id, user_id, tenant_id, value, comment)
    VALUES
        (:event_id, :user_id, :tenant_id, :value, :comment)
    ON CONFLICT (user_id, event_id) DO UPDATE
        SET value      = EXCLUDED.value,
            comment    = EXCLUDED.comment,
            updated_at = NOW()
    RETURNING feedback_id, event_id, value, comment, created_at
    """
)


def upsert_feedback(
    *,
    user_id: int,
    event_id: int,
    value: int,
    comment: str | None,
) -> dict[str, Any]:
    """Insert or update one feedback row. Returns the upserted record.

    Raises :class:`FeedbackError` with ``kind='not_found'`` if the user
    does not own the referenced ``event_id``. This is surfaced as a 404
    by the router so client-side stale state (a stale event_id from a
    cleared audit table) is distinguishable from a bug.
    """
    try:
        with get_engine().begin() as conn:
            owner = conn.execute(
                _OWNERSHIP_STMT,
                {"event_id": event_id, "user_id": user_id},
            ).first()
            if owner is None:
                raise FeedbackError(
                    "not_found",
                    f"no query_log row with event_id={event_id} for user_id={user_id}",
                )
            tenant_id = owner[0]

            row = conn.execute(
                _UPSERT_STMT,
                {
                    "event_id": event_id,
                    "user_id": user_id,
                    "tenant_id": tenant_id,
                    "value": value,
                    "comment": comment,
                },
            ).first()
    except FeedbackError:
        raise
    except Exception as exc:  # noqa: BLE001
        log.error(
            "ai.feedback.write_failed",
            extra={"event_id": event_id, "user_id": user_id, "error": str(exc)},
        )
        raise FeedbackError("db", str(exc)) from exc

    assert row is not None  # guaranteed by RETURNING on a successful upsert
    return {
        "feedback_id": int(row[0]),
        "event_id": int(row[1]),
        "value": int(row[2]),
        "comment": row[3],
        "created_at": row[4].isoformat(),
    }
