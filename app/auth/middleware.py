"""
JWT verification middleware.

Wired AFTER MetricsMiddleware in app/main.py, so the order is:
    MetricsMiddleware (outer)
        AuthMiddleware
            FastAPI route

Behaviour is governed by `Settings.auth_enforcement`:

  off       — middleware short-circuits, principal stays None. The
              system runs as if the middleware were absent. Use for
              local dev only; the audit log gets no auth events.

  advisory  — DEFAULT for M3. Every request is inspected:
                * No token → log "auth.missing_token", let request pass.
                * Token present and valid → set principal as usual.
                * Token present and invalid → log "auth.invalid_token",
                  let request pass (status remains whatever the route
                  returns).
              This is the soft-launch state. We watch the audit log
              for one week to find any client we forgot about, then
              flip to `enforce`.

  enforce   — Production state (M4):
                * /v1/auth/login is exempt (you can't log in if you
                  need to be logged in already).
                * /v1/auth/refresh is exempt (uses refresh token, not
                  bearer access token).
                * /health, /metrics, /v1/auth/* exempt as listed below.
                * Everything else: missing or invalid token → 401.

The exempt-paths set is hardcoded rather than configurable because the
list is short and getting it wrong has security consequences. A
typo in an env-var-driven exempt list could silently open a hole.
"""

from __future__ import annotations

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from accent_fleet.config import settings
from app.auth.principal import clear_principal, set_principal
from app.auth.tokens import (
    SigningKeyMissingError,
    TokenError,
    TokenExpiredError,
    TokenInvalidError,
    principal_from_payload,
    verify_access_token,
)

logger = structlog.get_logger("accent_fleet.api.auth.middleware")

# Paths that never require a bearer token, even in `enforce` mode.
# Operational: /health (probes), /metrics (scrapers), / (service banner).
# Auth bootstrap: /v1/auth/login (initial token), /v1/auth/refresh (uses
# refresh token in body, not bearer).
_EXEMPT_PATHS: frozenset[str] = frozenset(
    {
        "/",
        "/health",
        "/ready",
        "/metrics",
        "/docs",
        "/openapi.json",
        "/redoc",
        # Auth bootstrap — both v1 and legacy bare paths.
        "/v1/auth/login",
        "/auth/login",
        "/v1/auth/refresh",
        "/auth/refresh",
    }
)


def _is_exempt(path: str) -> bool:
    """
    True if `path` is in the never-require-auth set.

    Match is exact, not prefix-based, on purpose: a "starts-with"
    check on `/v1/auth/` would also exempt future privileged endpoints
    accidentally placed under that prefix. The auth router knows which
    of its routes are exempt and which need a token; everything else
    must pass through.
    """
    return path in _EXEMPT_PATHS


def _extract_bearer(request: Request) -> str | None:
    """Return the bearer token from the Authorization header, or None."""
    header = request.headers.get("authorization", "")
    if not header.lower().startswith("bearer "):
        return None
    return header[len("bearer ") :].strip() or None


class AuthMiddleware(BaseHTTPMiddleware):
    """
    Verify the bearer token, populate the request-scoped ContextVar,
    and (in `enforce` mode) reject requests that lack a valid one.
    """

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        # Read the enforcement mode and target path for this request.
        mode = settings().auth_enforcement.lower()
        path = request.url.path

        # Fast path: nothing to do at all.
        if mode == "off" or _is_exempt(path):
            return await call_next(request)

        # Pull the bearer token from the Authorization header (may be None).
        token = _extract_bearer(request)

        # No token: reject under enforce, otherwise log and pass through.
        if token is None:
            if mode == "enforce":
                return _unauth_response("missing bearer token")
            logger.info("auth.missing_token", path=path, mode=mode)
            return await call_next(request)

        # Verify the token and derive a Principal; branch by failure mode.
        try:
            payload = verify_access_token(token)
            principal = principal_from_payload(payload)
        except TokenExpiredError as exc:
            if mode == "enforce":
                return _unauth_response("token expired")
            logger.info(
                "auth.invalid_token",
                path=path,
                mode=mode,
                reason="expired",
                detail=str(exc),
            )
            return await call_next(request)
        except (TokenInvalidError, TokenError) as exc:
            if mode == "enforce":
                return _unauth_response("invalid token")
            logger.info(
                "auth.invalid_token",
                path=path,
                mode=mode,
                reason="invalid",
                detail=str(exc),
            )
            return await call_next(request)
        except SigningKeyMissingError:
            # In enforce mode, refuse the request outright. In advisory
            # mode the request still passes — but we shout into the log
            # so the operator notices the misconfiguration on the way
            # to flipping enforce.
            if mode == "enforce":
                logger.error("auth.signing_key_missing", path=path)
                return _unauth_response("server auth misconfigured")
            logger.error("auth.signing_key_missing", path=path, mode=mode)
            return await call_next(request)

        # Happy path: stash the principal for the SQLAlchemy event
        # listener and route deps to read.
        set_principal(principal)
        try:
            response: Response = await call_next(request)
        finally:
            # Always clear, even on exception, so the next reuse of
            # this task's ContextVar starts from a clean state.
            clear_principal()
        return response


def _unauth_response(reason: str) -> JSONResponse:
    """
    Build a 401 with a stable JSON shape. WWW-Authenticate header tells
    well-behaved clients which scheme to retry with.
    """
    return JSONResponse(
        status_code=401,
        content={"detail": reason},
        headers={"WWW-Authenticate": "Bearer"},
    )
