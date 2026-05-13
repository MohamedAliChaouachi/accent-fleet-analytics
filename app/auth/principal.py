"""
Request-scoped principal: who is making this request.

A single `contextvars.ContextVar[Principal | None]` holds the value for
the lifetime of one request. FastAPI runs each request in its own task,
so the ContextVar isolation is automatic — concurrent requests do not
see each other's principals.

Why ContextVar and not Request.state?
  - The SQLAlchemy `before_cursor_execute` event listener (M5) needs to
    read the principal at SQL-execute time, where it has no FastAPI
    request handle. ContextVar gives it a side-channel that doesn't
    require threading the request through every helper.
  - Route handlers still take `Principal` as a typed dependency rather
    than reading the ContextVar directly. See app/auth/deps.py.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Literal

Role = Literal["tenant_user", "tenant_admin", "superadmin"]


@dataclass(frozen=True, slots=True)
class Principal:
    """
    Immutable identity attached to one request.

    `tenant_id` is None iff `role == 'superadmin'` — enforced by the DB
    CHECK constraint in sql/50_auth.sql, mirrored here in `__post_init__`.
    """

    user_id: int
    tenant_id: int | None
    role: Role
    email: str

    def __post_init__(self) -> None:
        # Mirror the DB invariant. If we ever build a Principal that
        # violates it, fail loudly here rather than letting RLS surface
        # a confusing "no rows" instead.
        if (self.role == "superadmin") != (self.tenant_id is None):
            raise ValueError(
                "Principal invariant violated: role=='superadmin' must "
                "be the only role with tenant_id is None "
                f"(got role={self.role!r}, tenant_id={self.tenant_id!r})"
            )

    @property
    def is_superadmin(self) -> bool:
        return self.role == "superadmin"


# The actual storage. Default None means "no authenticated principal yet";
# downstream code MUST handle that case (e.g. middleware in advisory mode
# may leave it None on purpose).
_current_principal: ContextVar[Principal | None] = ContextVar(
    "accent_fleet.current_principal", default=None
)


def set_principal(principal: Principal) -> None:
    _current_principal.set(principal)


def current_principal() -> Principal | None:
    return _current_principal.get()


def clear_principal() -> None:
    """
    Reset the ContextVar to None at request teardown.

    ContextVar values are scoped to the current async task, so a leaked
    principal cannot reach a different request in practice — but
    clearing on the way out keeps the invariant explicit and helps
    debugging when a test re-uses the same event loop task.
    """
    _current_principal.set(None)
