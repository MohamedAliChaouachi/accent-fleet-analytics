"""
Append-only audit log writer.

One function: `write_audit_event()`. Inserts a row into `auth.audit_log`
on every state-changing auth event (login attempt, refresh, logout,
admin action). Reads are intentionally NOT audited at the row level —
the access log (structlog request line) already records what was
queried, and per-read auditing would multiply the audit volume by
orders of magnitude.

The writer:
  - Accepts an existing SQLAlchemy `Connection` so the caller can
    decide whether the audit row commits with their business
    transaction or independently. Login failure paths typically want
    independent commits so the audit row survives a rollback.
  - Coerces `detail` to JSON via psycopg's adapter — pass a dict, not
    a json.dumps string.
  - Never raises on a writer error. An audit-log INSERT failure should
    log a warning and let the original request continue, not turn a
    successful login into a 500.
"""

from __future__ import annotations

from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.engine import Connection

from accent_fleet.db.engine import get_engine

logger = structlog.get_logger("accent_fleet.api.auth.audit")

# Canonical action names, per docs/auth_design.md §5. Kept as a tuple
# rather than an Enum so callers can pass a literal string without the
# import overhead — and the DB column is TEXT, so no consistency value
# is lost by avoiding Python's enum machinery.
ACTIONS = (
    "login_success",
    "login_failure",
    "refresh",
    "logout",
    "token_revoked",
    "admin_create_user",
    "admin_create_tenant",
    "admin_disable_user",
    "password_reset",
)


def write_audit_event(
    *,
    action: str,
    user_id: int | None = None,
    tenant_id: int | None = None,
    source_ip: str | None = None,
    user_agent: str | None = None,
    detail: dict[str, Any] | None = None,
    conn: Connection | None = None,
) -> None:
    """
    Insert one audit row. Best-effort: any DB error is logged and
    swallowed so audit writes can never break a request.

    When `conn` is None, opens its own short-lived autocommit-equivalent
    connection via `engine.begin()`. This is the right default for
    "audit a login failure regardless of what else happens in the
    request" — the row commits even if the caller's outer logic later
    fails or returns 401.
    """
    if action not in ACTIONS:
        logger.warning("auth.audit.unknown_action", action=action)

    params = {
        "user_id": user_id,
        "tenant_id": tenant_id,
        "action": action,
        "source_ip": source_ip,
        "user_agent": user_agent,
        # psycopg adapts a Python dict to JSONB automatically; passing
        # None writes a NULL rather than the string "null".
        "detail": detail,
    }
    stmt = text(
        "INSERT INTO auth.audit_log "
        "  (user_id, tenant_id, action, source_ip, user_agent, detail) "
        "VALUES (:user_id, :tenant_id, :action, :source_ip, :user_agent, "
        "        CAST(:detail AS JSONB))"
    )
    # Stringify dict at the bind layer — SQLAlchemy/psycopg won't
    # implicitly cast a Python dict into JSONB via :detail without
    # the explicit CAST above; the param itself must be a JSON string.
    if isinstance(detail, dict):
        import json
        params["detail"] = json.dumps(detail)

    try:
        if conn is not None:
            conn.execute(stmt, params)
        else:
            with get_engine().begin() as own_conn:
                own_conn.execute(stmt, params)
    except Exception as exc:  # pylint: disable=broad-except
        # Never let an audit-log failure break the request. Log loudly
        # so the operator notices the gap during incident review.
        logger.error(
            "auth.audit.write_failed",
            action=action,
            user_id=user_id,
            tenant_id=tenant_id,
            error=str(exc),
        )
