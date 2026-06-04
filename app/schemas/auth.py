"""Request and response schemas for /v1/auth/* and /v1/admin/*."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

# Why not EmailStr?
# ----------------
# `email-validator` rejects RFC-6761 special-use TLDs like `.local`,
# `.test`, `.example`. Our seeded system identities use `.local` on
# purpose (they're not deliverable mailboxes — they're keys into
# `auth.users.email` with a CITEXT uniqueness constraint). A loose
# `str` with `@` presence + length bounds matches the DB column.
EmailLike = Field(..., min_length=3, max_length=320, pattern=r"^[^@\s]+@[^@\s]+$")


# --- /v1/auth/login ---------------------------------------------------

# Credentials posted to /login.
class LoginRequest(BaseModel):
    email: str = EmailLike
    password: str = Field(..., min_length=1, max_length=256)


class TokenPair(BaseModel):
    """Issued by /login and /refresh. Same shape so the client treats them
    interchangeably after authentication."""

    access_token: str
    refresh_token: str
    expires_at: int = Field(
        ..., description="Unix-epoch seconds at which access_token expires."
    )
    token_type: Literal["bearer"] = "bearer"


# --- /v1/auth/refresh -------------------------------------------------

# Refresh token presented to /refresh to mint a new access token.
class RefreshRequest(BaseModel):
    refresh_token: str = Field(..., min_length=8, max_length=128)


# --- /v1/auth/logout --------------------------------------------------

# Refresh token to revoke on /logout.
class LogoutRequest(BaseModel):
    refresh_token: str = Field(..., min_length=8, max_length=128)


# --- /v1/auth/me ------------------------------------------------------

# Current authenticated user's profile, returned by /me.
class MeResponse(BaseModel):
    user_id: int
    email: str
    tenant_id: int | None
    tenant_name: str | None
    role: Literal["tenant_user", "tenant_admin", "superadmin"]
    last_login_at: datetime | None = None


# --- /v1/admin/tenants ------------------------------------------------

# Payload to provision a new tenant via /admin/tenants.
class CreateTenantRequest(BaseModel):
    tenant_id: int = Field(..., ge=1)
    display_name: str = Field(..., min_length=1, max_length=128)


# Tenant record returned after creation / lookup.
class TenantResponse(BaseModel):
    tenant_id: int
    display_name: str
    is_active: bool
    created_at: datetime


# --- /v1/admin/users --------------------------------------------------

# Payload to provision a new user via /admin/users.
class CreateUserRequest(BaseModel):
    email: str = EmailLike
    role: Literal["tenant_user", "tenant_admin", "superadmin"]
    tenant_id: int | None = None
    initial_password: str = Field(..., min_length=12, max_length=256)


# Newly created user record returned to the admin.
class CreatedUserResponse(BaseModel):
    user_id: int
    email: str
    tenant_id: int | None
    role: str
    is_active: bool


# Result of an admin password reset, carrying the one-shot temp password.
class ResetPasswordResponse(BaseModel):
    user_id: int
    email: str
    new_password: str = Field(
        ..., description="One-shot temporary password. Capture immediately."
    )
