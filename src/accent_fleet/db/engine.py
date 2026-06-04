"""
SQLAlchemy engine factory.

We use a single process-wide engine with a small pool. The pipeline is
either single-process (Prefect task) or, in streaming mode, one consumer
per topic partition — neither needs a large pool.

# RLS coupling (v0.9.0 §2.2 M5)

This module also attaches a `begin` event listener that issues
``SET LOCAL app.current_tenant = '<id>'`` for every transaction that
runs inside a request whose ContextVar holds a non-superadmin
Principal. The matching policies live in sql/51_rls_policies.sql.

The listener pulls the principal from `app.auth.principal` — a soft
dependency: if app.auth isn't importable (e.g. running a pipeline
script that doesn't ship the web stack), we silently skip the SET
LOCAL so ETL keeps working. ETL/pipeline transactions therefore see
all tenants; that's safe today because they connect as a BYPASSRLS
role anyway, and M6 will explicitly split the role so the API
connects as non-BYPASSRLS while pipelines keep their BYPASSRLS.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.engine import Connection

from accent_fleet.config import settings

logger = logging.getLogger(__name__)


# Lazily fetch the request's auth Principal; None outside the web stack.
def _try_get_principal():  # noqa: ANN202 — Principal type lives in soft dep
    """
    Return the request-scoped Principal, or None if app.auth isn't loaded.

    Imported lazily so non-web entry points (Prefect flows, the seed
    script, ad-hoc psql-style scripts) don't have to drag in the
    FastAPI dependency tree just to get an Engine.
    """
    try:
        from app.auth.principal import current_principal
    except ImportError:
        return None
    return current_principal()


# Per-transaction RLS hook: set the tenant GUC (or elevate superadmin role).
def _set_tenant_guc(conn: Connection) -> None:
    """
    Fired by SQLAlchemy's `begin` event. Issues `SET LOCAL` so RLS
    policies in sql/51_rls_policies.sql can compare against the
    request's tenant.

    Decision matrix (post-M6, when the API connects as `accent_app`,
    NOBYPASSRLS):
      - No Principal (ETL / Prefect / ad-hoc scripts)        → no-op.
        These do NOT run on the API connection; they connect as a
        BYPASSRLS role (medamine_dev today, accent_etl after B7) so
        skipping the GUC is harmless — they see all rows by virtue of
        the role attribute, not the policy.
      - role == 'superadmin'                                 → swap to
        accent_superadmin via `SET LOCAL ROLE`. accent_app is a member
        of accent_superadmin (sql/54_grant_superadmin_membership.sql)
        and accent_superadmin has BYPASSRLS — so the transaction sees
        every tenant's rows. Reverts at COMMIT/ROLLBACK.
      - role in {'tenant_user', 'tenant_admin'}              → set GUC
        to principal.tenant_id. Policies match; cross-tenant rows are
        invisible.
    """
    # No Principal (ETL/Prefect/scripts run as BYPASSRLS roles): nothing to set.
    p = _try_get_principal()
    if p is None:
        # ETL / Prefect / ad-hoc scripts: no Principal in scope. Today these
        # connect as a BYPASSRLS role (medamine_dev, accent_etl) so the lack
        # of a SET LOCAL is harmless. If you ever route an API-style request
        # through an ETL connection, this is the place to fix it.
        return
    # Superadmin: switch to the BYPASSRLS role for the duration of this txn.
    if p.role == "superadmin":
        # Post-M6 the API connects as accent_app (NOBYPASSRLS), so a superadmin
        # principal *also* runs through that connection. Without intervention,
        # the RLS policies on warehouse/marts would filter every row away
        # (empty GUC compares to no tenant). We elevate the transaction to
        # accent_superadmin (BYPASSRLS) via SET LOCAL ROLE — this reverts at
        # COMMIT/ROLLBACK, so the pooled connection returns to accent_app for
        # the next request. Requires `GRANT accent_superadmin TO accent_app`
        # (sql/54_grant_superadmin_membership.sql).
        conn.exec_driver_sql("SET LOCAL ROLE accent_superadmin")
        return
    # Defensive guard: a tenant principal with no tenant_id must never set an empty GUC.
    if p.tenant_id is None:
        # Should be unreachable — Principal.__post_init__ rejects this
        # combo — but a future bug here would be silent + dangerous,
        # so log loudly and refuse rather than setting GUC to ''.
        logger.error("rls.principal_missing_tenant_id user_id=%s", p.user_id)
        return
    # `exec_driver_sql` avoids SQLAlchemy's parameterization layer; that
    # matters because SET LOCAL doesn't accept bound parameters in Postgres.
    # We coerce to int before string-formatting to make injection impossible.
    tenant_id_int = int(p.tenant_id)
    conn.exec_driver_sql(f"SET LOCAL app.current_tenant = '{tenant_id_int}'")


# Build the process-wide engine once and wire up the RLS begin-listener.
@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Create (once) and return the process-wide engine."""
    s = settings()
    engine = create_engine(
        s.sqlalchemy_url,
        pool_size=5,
        max_overflow=5,
        pool_pre_ping=True,
        future=True,
        echo=False,
    )
    # Attach the RLS GUC setter. `begin` fires once per transaction on
    # the Connection — exactly when SET LOCAL is meaningful.
    event.listen(engine, "begin", _set_tenant_guc)
    return engine


# Context manager yielding a connection wrapped in one atomic transaction.
@contextmanager
def transaction() -> Iterator[Connection]:
    """
    Yield a Connection inside an explicit transaction.

    Why explicit? Because a single pipeline task may run multiple SQL
    statements (insert into fact + advance watermark) and they must
    commit together. SQLAlchemy's `engine.begin()` is the right pattern.
    """
    engine = get_engine()
    with engine.begin() as conn:
        yield conn
