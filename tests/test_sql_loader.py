from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from accent_fleet.db import sql_loader


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
