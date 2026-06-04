"""
FastAPI dependencies that resolve to a `Principal`.

Routes declare:

    @router.get("/me")
    def me(principal: Principal = CurrentPrincipalDep):
        ...

The dependency reads `current_principal()` (set by AuthMiddleware) and
turns "no principal" into a 401. It deliberately does NOT consult the
middleware's enforcement mode — by the time we're inside a route that
requires a principal, "advisory" no longer applies. Any route that
wants to opt out of auth simply doesn't take this dependency.
"""

from __future__ import annotations

from fastapi import Depends, HTTPException, status

from app.auth.principal import Principal, current_principal


# Resolve the request principal set by AuthMiddleware, or 401 if absent.
def _require_principal() -> Principal:
    principal = current_principal()
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return principal


# Require an authenticated principal that is specifically a superadmin.
def _require_superadmin() -> Principal:
    principal = _require_principal()
    if not principal.is_superadmin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="superadmin role required",
        )
    return principal


# Require a principal whose role is tenant_admin or superadmin.
def _require_tenant_admin() -> Principal:
    """
    Pass through for `tenant_admin` and `superadmin`. Routes that
    accept either role take this dep; routes that require strict
    superadmin take `RequireSuperadminDep`.
    """
    principal = _require_principal()
    if principal.role not in ("tenant_admin", "superadmin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="tenant_admin or superadmin role required",
        )
    return principal


# Pre-built Depends() markers for use in route signatures.
CurrentPrincipalDep = Depends(_require_principal)
RequireSuperadminDep = Depends(_require_superadmin)
RequireTenantAdminDep = Depends(_require_tenant_admin)
