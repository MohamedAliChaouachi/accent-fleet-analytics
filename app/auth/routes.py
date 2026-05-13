"""
/v1/auth/* endpoints.

  POST /auth/login          — email + password → access + refresh tokens.
  POST /auth/refresh        — refresh token   → fresh pair (old refresh revoked).
  POST /auth/logout         — refresh token   → 204; row marked revoked.
  GET  /auth/me             — bearer access   → current user info.

The router prefix is `/auth`. `include_versioned_router` mounts it
twice: at /v1/auth/* (canonical) and /auth/* (legacy hidden). The
middleware exempts both `/auth/login` and `/v1/auth/login` from
enforcement so login itself never requires a bearer token.

The seed script (M2) created the bootstrap users. Self-service
signup is out of scope for v0.9.0 — new users come in through
/admin/users.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog
from fastapi import APIRouter, HTTPException, Request, status
from sqlalchemy import text
from sqlalchemy.engine import Connection

from accent_fleet.config import settings
from app.auth.audit import write_audit_event
from app.auth.deps import CurrentPrincipalDep
from app.auth.passwords import (
    InvalidPasswordError,
    hash_password,
    needs_rehash,
    verify_password,
)
from app.auth.principal import Principal
from app.auth.rate_limit import RateLimitExceededError, get_login_rate_limiter
from app.auth.tokens import (
    issue_access_token,
    issue_refresh_token_id,
)
from app.deps import DbDep
from app.schemas.auth import (
    LoginRequest,
    LogoutRequest,
    MeResponse,
    RefreshRequest,
    TokenPair,
)

logger = structlog.get_logger("accent_fleet.api.auth.routes")

router = APIRouter(prefix="/auth", tags=["auth"])


def _client_ip(request: Request) -> str:
    """
    Return the originating client IP.

    Prefers X-Forwarded-For (set by nginx in `--profile auth` and by any
    cloud LB), falls back to the socket peer. We trust the header
    because the only ingress in front of us today is our own nginx;
    when the deployment topology changes this needs to learn about
    trusted-proxy whitelisting.
    """
    xff = request.headers.get("x-forwarded-for")
    if xff:
        # First entry is the client; subsequent ones are intermediate proxies.
        return xff.split(",", 1)[0].strip()
    return request.client.host if request.client else "unknown"


# ---------------------------------------------------------------------------
# POST /auth/login
# ---------------------------------------------------------------------------
@router.post(
    "/login",
    response_model=TokenPair,
    status_code=status.HTTP_200_OK,
    summary="Exchange email + password for a token pair.",
)
async def login(
    body: LoginRequest,
    request: Request,
    conn: Connection = DbDep,
) -> TokenPair:
    ip = _client_ip(request)
    user_agent = request.headers.get("user-agent")

    # Rate-limit BEFORE hashing/verifying so a brute-force attacker
    # can't keep the CPU pinned by submitting bogus passwords. The
    # limiter raises before we ever touch the DB on the slow path.
    try:
        await get_login_rate_limiter().check(ip)
    except RateLimitExceededError as exc:
        write_audit_event(
            action="login_failure",
            source_ip=ip,
            user_agent=user_agent,
            detail={"reason": "rate_limited", "email": body.email},
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="too many login attempts; try again later",
            headers={"Retry-After": str(exc.retry_after_seconds)},
        ) from exc

    row = conn.execute(
        text(
            "SELECT user_id, tenant_id, email, password_hash, role, "
            "       is_active "
            "FROM auth.users "
            "WHERE email = :email"
        ),
        {"email": body.email},
    ).first()

    if row is None or not row.is_active:
        # Constant-shape failure response so an attacker can't
        # distinguish "no such user" from "wrong password".
        write_audit_event(
            action="login_failure",
            source_ip=ip,
            user_agent=user_agent,
            detail={
                "reason": "no_such_user_or_disabled",
                "email": body.email,
            },
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid credentials",
        )

    try:
        verify_password(body.password, row.password_hash)
    except InvalidPasswordError:
        write_audit_event(
            action="login_failure",
            user_id=row.user_id,
            tenant_id=row.tenant_id,
            source_ip=ip,
            user_agent=user_agent,
            detail={"reason": "bad_password", "email": body.email},
        )
        # `from None` deliberately swallows the InvalidPasswordError —
        # the client gets one shape (401 invalid credentials) whether
        # the email was missing, the user was disabled, or the hash
        # didn't verify. Internal cause lives in the audit log.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid credentials",
        ) from None

    # Silently rehash if the stored parameters drifted. Doesn't block
    # the response; failures here are swallowed-warned.
    if needs_rehash(row.password_hash):
        try:
            new_hash = hash_password(body.password)
            conn.execute(
                text(
                    "UPDATE auth.users SET password_hash = :h "
                    "WHERE user_id = :uid"
                ),
                {"h": new_hash, "uid": row.user_id},
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                "auth.rehash_failed",
                user_id=row.user_id,
                error=str(exc),
            )

    principal = Principal(
        user_id=row.user_id,
        tenant_id=row.tenant_id,
        role=row.role,
        email=row.email,
    )
    access_token, expires_at = issue_access_token(principal)

    # Refresh token row. UUID returned to the client; server-side row
    # is what `logout` and `refresh` look up.
    refresh_uuid = issue_refresh_token_id()
    refresh_expires_at = datetime.now(UTC) + timedelta(
        seconds=settings().jwt_refresh_ttl_seconds
    )
    conn.execute(
        text(
            "INSERT INTO auth.refresh_tokens "
            "  (token_id, user_id, expires_at, user_agent, source_ip) "
            "VALUES (CAST(:tid AS uuid), :uid, :exp, :ua, "
            "        CAST(:ip AS inet))"
        ),
        {
            "tid": refresh_uuid,
            "uid": row.user_id,
            "exp": refresh_expires_at,
            "ua": user_agent,
            "ip": ip,
        },
    )
    conn.execute(
        text(
            "UPDATE auth.users SET last_login_at = NOW() "
            "WHERE user_id = :uid"
        ),
        {"uid": row.user_id},
    )

    write_audit_event(
        action="login_success",
        user_id=row.user_id,
        tenant_id=row.tenant_id,
        source_ip=ip,
        user_agent=user_agent,
        detail={"email": row.email},
    )

    return TokenPair(
        access_token=access_token,
        refresh_token=refresh_uuid,
        expires_at=expires_at,
    )


# ---------------------------------------------------------------------------
# POST /auth/refresh
# ---------------------------------------------------------------------------
@router.post(
    "/refresh",
    response_model=TokenPair,
    summary="Rotate refresh token, mint a new access token.",
)
def refresh(
    body: RefreshRequest,
    request: Request,
    conn: Connection = DbDep,
) -> TokenPair:
    ip = _client_ip(request)
    user_agent = request.headers.get("user-agent")

    # Look up the refresh row. We do NOT trust the client's token until
    # we've checked all three of: exists, not revoked, not expired.
    # Casting to uuid via SQL keeps a malformed token from blowing up
    # the parameter binding — bad input becomes a clean "not found".
    row = conn.execute(
        text(
            "SELECT rt.token_id, rt.user_id, rt.expires_at, rt.revoked_at, "
            "       u.email, u.tenant_id, u.role, u.is_active "
            "FROM auth.refresh_tokens rt "
            "JOIN auth.users u ON u.user_id = rt.user_id "
            "WHERE rt.token_id = "
            "      CASE WHEN :tid ~ '^[0-9a-fA-F-]{36}$' "
            "           THEN CAST(:tid AS uuid) "
            "           ELSE NULL END"
        ),
        {"tid": body.refresh_token},
    ).first()

    if row is None or row.revoked_at is not None or not row.is_active:
        write_audit_event(
            action="login_failure",
            source_ip=ip,
            user_agent=user_agent,
            detail={"reason": "refresh_invalid"},
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid refresh token",
        )

    if row.expires_at <= datetime.now(UTC):
        write_audit_event(
            action="login_failure",
            user_id=row.user_id,
            tenant_id=row.tenant_id,
            source_ip=ip,
            user_agent=user_agent,
            detail={"reason": "refresh_expired"},
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="refresh token expired",
        )

    # Rotate: revoke old, mint new.
    principal = Principal(
        user_id=row.user_id,
        tenant_id=row.tenant_id,
        role=row.role,
        email=row.email,
    )
    access_token, expires_at = issue_access_token(principal)
    new_refresh = issue_refresh_token_id()
    new_refresh_expires = datetime.now(UTC) + timedelta(
        seconds=settings().jwt_refresh_ttl_seconds
    )

    conn.execute(
        text(
            "UPDATE auth.refresh_tokens SET revoked_at = NOW() "
            "WHERE token_id = :tid"
        ),
        {"tid": row.token_id},
    )
    conn.execute(
        text(
            "INSERT INTO auth.refresh_tokens "
            "  (token_id, user_id, expires_at, user_agent, source_ip) "
            "VALUES (CAST(:tid AS uuid), :uid, :exp, :ua, "
            "        CAST(:ip AS inet))"
        ),
        {
            "tid": new_refresh,
            "uid": row.user_id,
            "exp": new_refresh_expires,
            "ua": user_agent,
            "ip": ip,
        },
    )

    write_audit_event(
        action="refresh",
        user_id=row.user_id,
        tenant_id=row.tenant_id,
        source_ip=ip,
        user_agent=user_agent,
    )

    return TokenPair(
        access_token=access_token,
        refresh_token=new_refresh,
        expires_at=expires_at,
    )


# ---------------------------------------------------------------------------
# POST /auth/logout
# ---------------------------------------------------------------------------
@router.post(
    "/logout",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revoke a refresh token.",
)
def logout(
    body: LogoutRequest,
    request: Request,
    conn: Connection = DbDep,
) -> None:
    ip = _client_ip(request)
    user_agent = request.headers.get("user-agent")

    # Mark the row revoked. We don't return 404 on "no such token" —
    # logout is idempotent from the client's perspective. Returning 204
    # for both states avoids leaking whether a token ever existed.
    result = conn.execute(
        text(
            "UPDATE auth.refresh_tokens "
            "   SET revoked_at = NOW() "
            " WHERE token_id = "
            "       CASE WHEN :tid ~ '^[0-9a-fA-F-]{36}$' "
            "            THEN CAST(:tid AS uuid) "
            "            ELSE NULL END "
            "   AND revoked_at IS NULL "
            "RETURNING user_id"
        ),
        {"tid": body.refresh_token},
    ).first()

    if result is not None:
        write_audit_event(
            action="logout",
            user_id=result.user_id,
            source_ip=ip,
            user_agent=user_agent,
        )


# ---------------------------------------------------------------------------
# GET /auth/me
# ---------------------------------------------------------------------------
@router.get(
    "/me",
    response_model=MeResponse,
    summary="Identity of the current bearer-token holder.",
)
def me(
    principal: Principal = CurrentPrincipalDep,
    conn: Connection = DbDep,
) -> MeResponse:
    # Fetch tenant_name + last_login_at in one query rather than
    # cramming everything into the JWT. The JWT stays small (claims
    # are sent on every request) and the dashboard's "current user"
    # widget gets a richer response.
    tenant_name: str | None = None
    if principal.tenant_id is not None:
        tenant_name = conn.execute(
            text(
                "SELECT display_name FROM auth.tenants WHERE tenant_id = :tid"
            ),
            {"tid": principal.tenant_id},
        ).scalar()

    last_login = conn.execute(
        text(
            "SELECT last_login_at FROM auth.users WHERE user_id = :uid"
        ),
        {"uid": principal.user_id},
    ).scalar()

    return MeResponse(
        user_id=principal.user_id,
        email=principal.email,
        tenant_id=principal.tenant_id,
        tenant_name=tenant_name,
        role=principal.role,
        last_login_at=last_login,
    )
