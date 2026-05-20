"""
Safe SQL executor.

Runs validated SQL in a dedicated READ ONLY transaction with a hard
statement timeout. Reasons:

  - READ ONLY is a Postgres-level backstop. Even if sql_guard.py ever
    let an INSERT through, the transaction would refuse it.
  - statement_timeout caps the blast radius of an expensive plan the
    LLM might generate (e.g. accidentally cross-joining marts).
  - idle_in_transaction_session_timeout protects against a client
    hanging mid-fetch and holding the connection.

We don't reuse the request's main DbDep connection — that one is in a
RW transaction owned by the FastAPI lifespan. Opening a fresh
short-lived connection keeps the read-only mode strictly local.
"""

from __future__ import annotations

import datetime as dt
import decimal
import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import text

from accent_fleet.db.engine import get_engine
from app.ai.config import AISettings

log = logging.getLogger(__name__)


class ExecutorError(RuntimeError):
    """Raised when the DB rejects, times out, or otherwise fails the query."""


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int


def execute(sql: str, *, binds: dict[str, object], settings: AISettings) -> ExecutionResult:
    """Run ``sql`` in a fresh READ ONLY transaction and return its rows."""
    engine = get_engine()
    try:
        # `engine.connect()` does not autobegin a transaction; calling
        # `conn.begin()` does, and we need it so SET LOCAL has scope.
        # Combined into a single `with` so ruff SIM117 is happy.
        with engine.connect() as conn, conn.begin():
            # READ ONLY: belt-and-suspenders against the SQL guard.
            # statement_timeout: per-query wall clock cap.
            # idle_in_transaction_session_timeout: prevents a stuck
            # client from holding the row locks beyond the timeout.
            conn.execute(text("SET TRANSACTION READ ONLY"))
            conn.execute(
                text(f"SET LOCAL statement_timeout = {settings.statement_timeout_ms}")
            )
            conn.execute(
                text(
                    "SET LOCAL idle_in_transaction_session_timeout = "
                    f"{settings.statement_timeout_ms}"
                )
            )
            result = conn.execute(text(sql), binds)
            columns = list(result.keys())
            rows = [_jsonable(dict(r)) for r in result.mappings().all()]
    except Exception as e:  # noqa: BLE001 — uniform conversion to ExecutorError
        log.warning("ai.executor.failure", extra={"error": str(e)})
        raise ExecutorError(str(e)) from e
    return ExecutionResult(columns=columns, rows=rows, row_count=len(rows))


# ---------------------------------------------------------------------------
# JSON coercion
# ---------------------------------------------------------------------------
# Pydantic will happily refuse to serialise Decimal / date / UUID values
# embedded in `dict[str, Any]` (the response model uses Any specifically
# because we don't know the column types ahead of time). Convert to
# JSON-native primitives here so the response shape is stable across
# every catalog table.


def _jsonable(row: dict[str, Any]) -> dict[str, Any]:
    return {k: _coerce(v) for k, v in row.items()}


def _coerce(v: Any) -> Any:  # noqa: ANN401 — pass-through coercer
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (dt.datetime, dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, UUID):
        return str(v)
    if isinstance(v, memoryview):
        return v.tobytes().hex()
    if isinstance(v, (list, tuple)):
        return [_coerce(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _coerce(x) for k, x in v.items()}
    # Last resort: stringify rather than blow up the whole response.
    return str(v)
