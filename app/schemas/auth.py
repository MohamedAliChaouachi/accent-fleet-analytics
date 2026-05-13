"""Request and response schemas for /v1/auth/* and /v1/admin/*."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, EmailStr, Field

# --- /v1/auth/login ---------------------------------------------------

class LoginRequest(BaseModel):
    email: EmailStr
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

class RefreshRequest(BaseModel):
    refresh_token: str = Field(..., min_length=8, max_length=128)


# --- /v1/auth/logout --------------------------------------------------

class LogoutRequest(BaseModel):
    refresh_token: str = Field(..., min_length=8, max_length=128)


# --- /v1/auth/me ------------------------------------------------------

class MeResponse(BaseModel):
    user_id: int
    email: str
    tenant_id: int | None
    tenant_name: str | None
    role: Literal["tenant_user", "tenant_admin", "superadmin"]
    last_login_at: datetime | None = None


# --- /v1/admin/tenants ------------------------------------------------

class CreateTenantRequest(BaseModel):
    tenant_id: int = Field(..., ge=1)
    display_name: str = Field(..., min_length=1, max_length=128)


class TenantResponse(BaseModel):
    tenant_id: int
    display_name: str
    is_active: bool
    created_at: datetime


# --- /v1/admin/users --------------------------------------------------

class CreateUserRequest(BaseModel):
    email: EmailStr
    role: Literal["tenant_user", "tenant_admin", "superadmin"]
    tenant_id: int | None = None
    initial_password: str = Field(..., min_length=12, max_length=256)


class CreatedUserResponse(BaseModel):
    user_id: int
    email: str
    tenant_id: int | None
    role: str
    is_active: bool


class ResetPasswordResponse(BaseModel):
    user_id: int
    email: str
    new_password: str = Field(
        ..., description="One-shot temporary password. Capture immediately."
    )
