"""
SQL file loader.

SQL files live under /sql and use :named parameters. We resolve the path,
read the file, and hand it to SQLAlchemy's `text()` which understands
:name bindparams. This keeps the SQL grep-able and copy-pasteable into
psql during debugging (psql also supports :variable via \\set).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from accent_fleet.config import SQL_DIR


def load_sql(filename: str) -> str:
    """Read a SQL file by its relative name under /sql."""
    path = SQL_DIR / filename
    if not path.is_file():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def run_sql_file(
    conn: Connection,
    filename: str,
    params: dict[str, Any] | None = None,
) -> Any:
    """
    Execute a SQL file, optionally with :named parameters.

    Returns the SQLAlchemy Result so callers can consume rowcount or rows.
    Works for single-statement or multi-statement files; multi-statement
    files must be split by the caller or contain only top-level DDL.
    """
    sql = load_sql(filename)
    return run_sql_statement(conn, sql, params)


def run_sql_statement(
    conn: Connection,
    sql: str,
    params: dict[str, Any] | None = None,
) -> Any:
    """Execute a single SQL statement with named parameters."""
    stmt = text(sql)
    return conn.execute(stmt, params or {})


def split_sql_statements(sql: str) -> list[str]:
    """
    Split a multi-statement SQL blob on top-level semicolons, preserving
    semicolons inside string literals and dollar-quoted blocks.

    Used for bootstrap/DDL files like 00_schemas_and_state.sql that
    contain multiple CREATE statements and a final INSERT.
    """
    out: list[str] = []
    buf: list[str] = []
    in_single = False
    in_dollar = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""
        if ch == "'" and not in_dollar:
            in_single = not in_single
            buf.append(ch)
        elif ch == "$" and nxt == "$" and not in_single:
            in_dollar = not in_dollar
            buf.append("$$")
            i += 1
        elif ch == ";" and not in_single and not in_dollar:
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out
