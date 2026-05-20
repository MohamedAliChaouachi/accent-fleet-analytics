"""
/v1/admin/{tenants,users}* — tenant + user management.

Lives in app/auth/ (alongside the rest of the auth surface) rather than
in app/routes/admin.py (which keeps the older /admin/reload-model and
its X-API-Key scheme). Mixing the two routers in the same prefix would
make the auth model harder to read; this file gets its own routes
under the same `/admin` prefix and `include_versioned_router` merges
them in the URL space.

Authorization:
  - `superadmin` can do everything.
  - `tenant_admin` can create/disable/reset users WITHIN their own
    tenant. The handler verifies `tenant_id` match before any DB write.
  - Nobody else (`tenant_user`, unauthenticated in advisory mode) can
    reach these endpoints.
"""

from __future__ import annotations

import secrets
import string

import structlog
from fastapi import APIRouter, HTTPException, Request, status
from sqlalchemy import text
from sqlalchemy.engine import Connection

from app.auth.audit import write_audit_event
from app.auth.deps import RequireSuperadminDep, RequireTenantAdminDep
from app.auth.passwords import hash_password
from app.auth.principal import Principal
from app.deps import DbDep
from app.schemas.auth import (
    CreatedUserResponse,
    CreateTenantRequest,
    CreateUserRequest,
    ResetPasswordResponse,
    TenantResponse,
)

logger = structlog.get_logger("accent_fleet.api.auth.admin")

router = APIRouter(prefix="/admin", tags=["admin-auth"])


def _client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",", 1)[0].strip()
    return request.client.host if request.client else "unknown"


def _generate_temp_password(length: int = 20) -> str:
    """Same generator used by scripts/seed_auth.py for consistency."""
    alphabet = string.ascii_letters + string.digits + "-_.!@#$%^&*+="
    return "".join(secrets.choice(alphabet) for _ in range(length))


# ---------------------------------------------------------------------------
# Tenants
# ---------------------------------------------------------------------------
@router.get(
    "/tenants",
    response_model=list[TenantResponse],
    summary="List all tenants known to the auth layer (superadmin only).",
)
def list_tenants(
    principal: Principal = RequireSuperadminDep,  # noqa: ARG001 — gate only
    conn: Connection = DbDep,
) -> list[TenantResponse]:
    # Source of truth is auth.tenants (already a strict subset of
    # warehouse.dim_tenant via FK). We sort by display_name so the
    # React dropdown is alphabetical without client-side sorting.
    rows = conn.execute(
        text(
            "SELECT tenant_id, display_name, is_active, created_at "
            "FROM auth.tenants "
            "ORDER BY display_name ASC"
        )
    ).all()
    return [
        TenantResponse(
            tenant_id=r.tenant_id,
            display_name=r.display_name,
            is_active=r.is_active,
            created_at=r.created_at,
        )
        for r in rows
    ]


@router.post(
    "/tenants",
    response_model=TenantResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create an auth.tenants row for a known warehouse tenant.",
)
def create_tenant(
    body: CreateTenantRequest,
    request: Request,
    principal: Principal = RequireSuperadminDep,
    conn: Connection = DbDep,
) -> TenantResponse:
    # warehouse.dim_tenant is the source of truth for tenant_id; the
    # auth-side row simply names it. The FK in sql/50_auth.sql will
    # reject any tenant_id not in dim_tenant — we surface that as a
    # clean 422 here rather than letting psycopg raise an IntegrityError.
    exists = conn.execute(
        text(
            "SELECT 1 FROM warehouse.dim_tenant WHERE tenant_id = :tid"
        ),
        {"tid": body.tenant_id},
    ).scalar()
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"tenant_id={body.tenant_id} does not exist in "
                "warehouse.dim_tenant"
            ),
        )

    row = conn.execute(
        text(
            "INSERT INTO auth.tenants (tenant_id, display_name) "
            "VALUES (:tid, :name) "
            "ON CONFLICT (tenant_id) DO UPDATE "
            "  SET display_name = EXCLUDED.display_name "
            "RETURNING tenant_id, display_name, is_active, created_at"
        ),
        {"tid": body.tenant_id, "name": body.display_name},
    ).first()

    write_audit_event(
        action="admin_create_tenant",
        user_id=principal.user_id,
        tenant_id=body.tenant_id,
        source_ip=_client_ip(request),
        user_agent=request.headers.get("user-agent"),
        detail={"display_name": body.display_name},
    )

    assert row is not None
    return TenantResponse(
        tenant_id=row.tenant_id,
        display_name=row.display_name,
        is_active=row.is_active,
        created_at=row.created_at,
    )


# ---------------------------------------------------------------------------
# Users — create
# ---------------------------------------------------------------------------
@router.post(
    "/users",
    response_model=CreatedUserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a user. superadmin may target any tenant; "
            "tenant_admin only their own.",
)
def create_user(
    body: CreateUserRequest,
    request: Request,
    principal: Principal = RequireTenantAdminDep,
    conn: Connection = DbDep,
) -> CreatedUserResponse:
    # --- authorization checks ---
    if body.role == "superadmin":
        if not principal.is_superadmin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="only superadmin may create superadmins",
            )
        if body.tenant_id is not None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="superadmin users must have tenant_id=null",
            )
    else:
        if body.tenant_id is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"role={body.role} requires tenant_id"
                ),
            )
        if not principal.is_superadmin and body.tenant_id != principal.tenant_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="tenant_admin cannot create users in another tenant",
            )

    password_hash = hash_password(body.initial_password)

    try:
        row = conn.execute(
            text(
                "INSERT INTO auth.users "
                "  (tenant_id, email, password_hash, role) "
                "VALUES (:tid, :email, :hash, :role) "
                "RETURNING user_id, email, tenant_id, role, is_active"
            ),
            {
                "tid": body.tenant_id,
                "email": body.email,
                "hash": password_hash,
                "role": body.role,
            },
        ).first()
    except Exception as exc:
        # The unique-email constraint is the realistic failure mode;
        # surface it as 409 rather than letting it bubble up as 500.
        message = str(exc).lower()
        if "users_email_key" in message or "duplicate key" in message:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="email already in use",
            ) from exc
        raise

    write_audit_event(
        action="admin_create_user",
        user_id=principal.user_id,
        tenant_id=body.tenant_id,
        source_ip=_client_ip(request),
        user_agent=request.headers.get("user-agent"),
        detail={"target_user_id": row.user_id, "target_email": body.email,
                "target_role": body.role},
    )

    assert row is not None
    return CreatedUserResponse(
        user_id=row.user_id,
        email=row.email,
        tenant_id=row.tenant_id,
        role=row.role,
        is_active=row.is_active,
    )


# ---------------------------------------------------------------------------
# Users — disable
# ---------------------------------------------------------------------------
@router.post(
    "/users/{user_id}/disable",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Deactivate a user and revoke all of their refresh tokens.",
)
def disable_user(
    user_id: int,
    request: Request,
    principal: Principal = RequireTenantAdminDep,
    conn: Connection = DbDep,
) -> None:
    target = conn.execute(
        text(
            "SELECT user_id, tenant_id, role FROM auth.users "
            "WHERE user_id = :uid"
        ),
        {"uid": user_id},
    ).first()
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="no such user",
        )

    if not principal.is_superadmin:
        if target.tenant_id != principal.tenant_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="tenant_admin cannot disable users in another tenant",
            )
        if target.role == "superadmin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="tenant_admin cannot disable a superadmin",
            )

    # Safety rail: never disable yourself. The check applies equally to
    # superadmin — accidentally locking the only superadmin out is the
    # exact incident this prevents.
    if target.user_id == principal.user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="cannot disable your own account",
        )

    conn.execute(
        text(
            "UPDATE auth.users SET is_active = FALSE "
            "WHERE user_id = :uid"
        ),
        {"uid": user_id},
    )
    conn.execute(
        text(
            "UPDATE auth.refresh_tokens "
            "   SET revoked_at = NOW() "
            " WHERE user_id = :uid AND revoked_at IS NULL"
        ),
        {"uid": user_id},
    )

    write_audit_event(
        action="admin_disable_user",
        user_id=principal.user_id,
        tenant_id=target.tenant_id,
        source_ip=_client_ip(request),
        user_agent=request.headers.get("user-agent"),
        detail={"target_user_id": user_id},
    )


# ---------------------------------------------------------------------------
# Users — reset password
# ---------------------------------------------------------------------------
@router.post(
    "/users/{user_id}/reset-password",
    response_model=ResetPasswordResponse,
    summary="Generate a new temp password and revoke all refresh tokens.",
)
def reset_password(
    user_id: int,
    request: Request,
    principal: Principal = RequireTenantAdminDep,
    conn: Connection = DbDep,
) -> ResetPasswordResponse:
    target = conn.execute(
        text(
            "SELECT user_id, tenant_id, role, email FROM auth.users "
            "WHERE user_id = :uid"
        ),
        {"uid": user_id},
    ).first()
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="no such user",
        )

    if not principal.is_superadmin:
        if target.tenant_id != principal.tenant_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="tenant_admin cannot reset users in another tenant",
            )
        if target.role == "superadmin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="tenant_admin cannot reset a superadmin's password",
            )

    new_password = _generate_temp_password()
    new_hash = hash_password(new_password)

    conn.execute(
        text(
            "UPDATE auth.users SET password_hash = :h "
            "WHERE user_id = :uid"
        ),
        {"h": new_hash, "uid": user_id},
    )
    conn.execute(
        text(
            "UPDATE auth.refresh_tokens "
            "   SET revoked_at = NOW() "
            " WHERE user_id = :uid AND revoked_at IS NULL"
        ),
        {"uid": user_id},
    )

    write_audit_event(
        action="password_reset",
        user_id=principal.user_id,
        tenant_id=target.tenant_id,
        source_ip=_client_ip(request),
        user_agent=request.headers.get("user-agent"),
        detail={"target_user_id": user_id},
    )

    return ResetPasswordResponse(
        user_id=user_id,
        email=target.email,
        new_password=new_password,
    )
