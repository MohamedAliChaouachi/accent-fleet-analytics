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


def _set_tenant_guc(conn: Connection) -> None:
    """
    Fired by SQLAlchemy's `begin` event. Issues `SET LOCAL` so RLS
    policies in sql/51_rls_policies.sql can compare against the
    request's tenant.

    Decision matrix:
      - No Principal (ETL / Prefect / superadmin op tools)   → no-op.
        These connect as BYPASSRLS today and see all rows.
      - role == 'superadmin'                                 → no-op,
        same reasoning.
      - role in {'tenant_user', 'tenant_admin'}              → set GUC
        to principal.tenant_id. Policies match; cross-tenant rows are
        invisible.
    """
    p = _try_get_principal()
    if p is None or p.role == "superadmin":
        return
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
