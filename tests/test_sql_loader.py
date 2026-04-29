from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from accent_fleet.config import SQL_DIR
from accent_fleet.db import sql_loader
from accent_fleet.transforms.facts import FACT_SQL


@dataclass
class FakeResult:
    rowcount: int


class FakeConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, statement: Any, params: dict[str, Any]) -> FakeResult:
        self.calls.append((str(statement), params))
        return FakeResult(rowcount=len(self.calls))


def test_run_sql_file_splits_multi_statement_file(monkeypatch) -> None:
    monkeypatch.setattr(
        sql_loader,
        "load_sql",
        lambda _: """
            CREATE TABLE IF NOT EXISTS demo (id int);
            INSERT INTO demo (id) SELECT :value;
        """,
    )
    conn = FakeConnection()

    result = sql_loader.run_sql_file(conn, "demo.sql", {"value": 42})

    assert result.rowcount == 2
    assert len(conn.calls) == 2
    assert conn.calls[0][0].lstrip().startswith("CREATE TABLE")
    assert conn.calls[1][0].lstrip().startswith("INSERT INTO")
    assert conn.calls[0][1] == {"value": 42}
    assert conn.calls[1][1] == {"value": 42}


def test_split_sql_statements_preserves_semicolon_in_string() -> None:
    statements = sql_loader.split_sql_statements(
        "INSERT INTO demo VALUES ('a;b'); SELECT 1;"
    )

    assert statements == ["INSERT INTO demo VALUES ('a;b')", "SELECT 1"]


def test_split_sql_statements_preserves_semicolon_in_line_comment() -> None:
    statements = sql_loader.split_sql_statements(
        """
        INSERT INTO demo (value)
        SELECT 1
        -- Derived metric: subtotal; total is computed below
        , 2;
        SELECT 3;
        """
    )

    assert len(statements) == 2
    assert statements[0].lstrip().startswith("INSERT INTO demo")
    assert "total is computed below" in statements[0]
    assert statements[0].rstrip().endswith(", 2")
    assert statements[1] == "SELECT 3"


def test_run_sql_statement_ignores_bind_like_tokens_in_comments() -> None:
    conn = FakeConnection()

    sql_loader.run_sql_statement(
        conn,
        """
        -- :comment_only should not become a bind parameter
        CREATE TABLE demo (label text DEFAULT ':literal_kept')
        """,
        {"comment_only": "ignored"},
    )

    statement, params = conn.calls[0]
    assert ":comment_only" not in statement
    assert ":literal_kept" in statement
    assert params == {"comment_only": "ignored"}


def test_bootstrap_seeds_watermark_for_every_incremental_fact() -> None:
    bootstrap_sql = (SQL_DIR / "00_schemas_and_state.sql").read_text(encoding="utf-8")

    missing = [fact_name for fact_name in FACT_SQL if f"'{fact_name}'" not in bootstrap_sql]

    assert missing == []
