"""
SQL file loader.

SQL files live under /sql and use :named parameters. We resolve the path,
read the file, and hand it to SQLAlchemy's `text()` which understands
:name bindparams. This keeps the SQL grep-able and copy-pasteable into
psql during debugging (psql also supports :variable via \\set).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from accent_fleet.config import SQL_DIR


# Resolve and read a SQL file from /sql, erroring if it's missing.
def load_sql(filename: str) -> str:
    """Read a SQL file by its relative name under /sql."""
    path = SQL_DIR / filename
    if not path.is_file():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


# Load a SQL file and run each top-level statement, returning the last result.
def run_sql_file(
    conn: Connection,
    filename: str,
    params: dict[str, Any] | None = None,
) -> Any:
    """
    Execute a SQL file, optionally with :named parameters.

    Returns the final SQLAlchemy Result so callers can consume rowcount or rows.
    Files may contain multiple top-level statements. This matters for psycopg,
    which refuses prepared statements containing multiple commands when bind
    parameters are present.
    """
    sql = load_sql(filename)
    result = None
    # Run statements one at a time (psycopg rejects multi-command prepared stmts).
    for statement in split_sql_statements(sql):
        result = run_sql_statement(conn, statement, params)
    return result


# Strip comments, bind named params, and execute a single statement.
def run_sql_statement(
    conn: Connection,
    sql: str,
    params: dict[str, Any] | None = None,
) -> Any:
    """Execute a single SQL statement with named parameters."""
    stmt = text(strip_sql_comments(sql))
    return conn.execute(stmt, params or {})


# Character-state scanner that drops comments while respecting string/$$ quoting.
def strip_sql_comments(sql: str) -> str:
    """
    Remove SQL comments before SQLAlchemy parses :named bind parameters.

    SQLAlchemy's text() treats :tokens inside comments as bind params. That is
    harmless until a statement has only comment mentions, at which point drivers
    such as psycopg receive phantom parameters with no SQL type context.
    """
    out: list[str] = []
    in_single = False
    in_dollar = False
    in_line_comment = False
    in_block_comment = False
    i = 0
    # Walk the text char by char, tracking which quoted/comment context we're in.
    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        # Inside a -- line comment: skip until newline (newline is preserved).
        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                out.append(ch)
        # Inside a /* */ block comment: skip until close (keep newlines for line numbers).
        elif in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 1
            elif ch == "\n":
                out.append(ch)
        # Toggle single-quote string state (quotes suppress comment detection).
        elif ch == "'" and not in_dollar:
            in_single = not in_single
            out.append(ch)
        # Toggle dollar-quoted block state.
        elif ch == "$" and nxt == "$" and not in_single:
            in_dollar = not in_dollar
            out.append("$$")
            i += 1
        # Enter a line comment only when not inside a string or dollar-quote.
        elif ch == "-" and nxt == "-" and not in_single and not in_dollar:
            in_line_comment = True
            i += 1
        # Enter a block comment likewise.
        elif ch == "/" and nxt == "*" and not in_single and not in_dollar:
            in_block_comment = True
            i += 1
        else:
            out.append(ch)
        i += 1
    return "".join(out)


# Quote/comment-aware splitter that breaks SQL only on top-level semicolons.
def split_sql_statements(sql: str) -> list[str]:
    """
    Split a multi-statement SQL blob on top-level semicolons, preserving
    semicolons inside string literals, dollar-quoted blocks, and comments.

    Used for bootstrap/DDL files like 00_schemas_and_state.sql that
    contain multiple CREATE statements and a final INSERT.
    """
    out: list[str] = []
    buf: list[str] = []
    in_single = False
    in_dollar = False
    in_line_comment = False
    in_block_comment = False
    i = 0
    # Scan char by char, buffering the current statement (comments kept verbatim).
    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        # In a -- line comment: copy through until newline ends it.
        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
        # In a /* */ block comment: copy through until the close marker.
        elif in_block_comment:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                in_block_comment = False
                i += 1
        # Toggle single-quote string state.
        elif ch == "'" and not in_dollar:
            in_single = not in_single
            buf.append(ch)
        # Toggle dollar-quoted block state.
        elif ch == "$" and nxt == "$" and not in_single:
            in_dollar = not in_dollar
            buf.append("$$")
            i += 1
        # Enter a line comment outside any quote.
        elif ch == "-" and nxt == "-" and not in_single and not in_dollar:
            in_line_comment = True
            buf.append(ch)
            buf.append(nxt)
            i += 1
        # Enter a block comment outside any quote.
        elif ch == "/" and nxt == "*" and not in_single and not in_dollar:
            in_block_comment = True
            buf.append(ch)
            buf.append(nxt)
            i += 1
        # Top-level semicolon: flush the buffered statement.
        elif ch == ";" and not in_single and not in_dollar:
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
        else:
            buf.append(ch)
        i += 1
    # Emit any trailing statement that wasn't semicolon-terminated.
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out
