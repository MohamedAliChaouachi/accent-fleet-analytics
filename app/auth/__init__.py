"""
Authentication + multi-tenancy package.

Implements §3 of docs/auth_design.md:

  - principal.py   — `Principal` dataclass and the request-scoped ContextVar
                     that downstream code (RLS event listener, audit log)
                     reads to learn "who is this request for?"
  - passwords.py   — Argon2id hash/verify wrappers around argon2-cffi.
  - tokens.py      — HS256 JWT issuance + verification with dual-key
                     rotation support.
  - rate_limit.py  — In-process token-bucket rate limiter for /v1/auth/login.
  - audit.py       — Append-only audit log writer.
  - middleware.py  — `AuthMiddleware`. Verifies bearer tokens, populates the
                     ContextVar, enforces `AUTH_ENFORCEMENT={off,advisory,
                     enforce}`.
  - deps.py        — FastAPI dependencies (`CurrentPrincipal`,
                     `RequireSuperadmin`, etc.) for route signatures.
  - routes.py      — `/v1/auth/{login,refresh,logout,me}` + admin endpoints.
"""

from app.auth.principal import (
    Principal,
    clear_principal,
    current_principal,
    set_principal,
)

__all__ = [
    "Principal",
    "clear_principal",
    "current_principal",
    "set_principal",
]
