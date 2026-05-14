"""
Unit tests for the RLS GUC setter in src/accent_fleet/db/engine.py.

Doesn't open a real Postgres connection — we exercise `_set_tenant_guc`
directly with a recording fake `Connection`. The point is to verify the
*policy* of the listener (when to SET LOCAL, when to skip), not the
SQLAlchemy plumbing.

The matching integration test that proves RLS actually fires lives
alongside the M6 cutover work, where we connect as a non-BYPASSRLS role.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from accent_fleet.db.engine import _set_tenant_guc
from app.auth.principal import (
    Principal,
    clear_principal,
    set_principal,
)


# ---------------------------------------------------------------------------
# Fake Connection — captures every exec_driver_sql call.
# ---------------------------------------------------------------------------
@dataclass
class _RecordingConn:
    calls: list[str]

    def exec_driver_sql(self, sql: str, *args: Any, **kwargs: Any) -> None:
        self.calls.append(sql)


@pytest.fixture
def conn() -> _RecordingConn:
    return _RecordingConn(calls=[])


@pytest.fixture(autouse=True)
def _clean_principal():
    """Each test starts with no principal; teardown clears too."""
    clear_principal()
    yield
    clear_principal()


# ---------------------------------------------------------------------------
class TestNoPrincipal:
    def test_no_principal_emits_nothing(self, conn: _RecordingConn) -> None:
        """ETL / seed scripts have no Principal — listener must no-op."""
        _set_tenant_guc(conn)  # type: ignore[arg-type]
        assert conn.calls == []


class TestSuperadmin:
    def test_superadmin_emits_set_local_role(self, conn: _RecordingConn) -> None:
        """
        Post-M6 the API connects as `accent_app` (NOBYPASSRLS), so superadmin
        principals must elevate within the transaction. The listener issues
        `SET LOCAL ROLE accent_superadmin` (which has BYPASSRLS); the role
        swap reverts at COMMIT/ROLLBACK. See engine.py docstring and
        sql/54_grant_superadmin_membership.sql.

        Earlier versions of this test asserted `conn.calls == []` on the
        assumption that superadmin connected as a BYPASSRLS role directly —
        that became wrong the moment M6 cut the API over to accent_app.
        """
        set_principal(
            Principal(user_id=1, tenant_id=None,
                      role="superadmin", email="sa@x")
        )
        _set_tenant_guc(conn)  # type: ignore[arg-type]
        assert conn.calls == ["SET LOCAL ROLE accent_superadmin"]


class TestTenantUser:
    def test_tenant_user_emits_set_local(self, conn: _RecordingConn) -> None:
        set_principal(
            Principal(user_id=42, tenant_id=7,
                      role="tenant_user", email="u@x")
        )
        _set_tenant_guc(conn)  # type: ignore[arg-type]
        assert conn.calls == ["SET LOCAL app.current_tenant = '7'"]

    def test_tenant_admin_emits_set_local(self, conn: _RecordingConn) -> None:
        set_principal(
            Principal(user_id=42, tenant_id=12,
                      role="tenant_admin", email="a@x")
        )
        _set_tenant_guc(conn)  # type: ignore[arg-type]
        assert conn.calls == ["SET LOCAL app.current_tenant = '12'"]


class TestInjectionSafety:
    def test_tenant_id_is_coerced_to_int(self, conn: _RecordingConn) -> None:
        """
        Even though tenant_id is typed `int` on Principal, we still
        coerce explicitly inside the listener so a future bug that
        smuggles a string can't inject SQL. Belt + suspenders.
        """
        # Bypass Principal.__post_init__ by setting tenant_id directly
        # via object.__setattr__ on a frozen dataclass — this simulates
        # the kind of "shouldn't happen" we're guarding against.
        p = Principal(user_id=1, tenant_id=5, role="tenant_user", email="u@x")
        object.__setattr__(p, "tenant_id", "5; DROP TABLE users--")
        set_principal(p)
        with pytest.raises(ValueError):
            # int("5; DROP TABLE users--") raises before any SQL emitted.
            _set_tenant_guc(conn)  # type: ignore[arg-type]
        assert conn.calls == []
