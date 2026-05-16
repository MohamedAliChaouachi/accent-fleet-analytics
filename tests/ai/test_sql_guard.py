"""
SQL guard tests.

These are the most important tests in the whole AI subsystem — every
new LLM behaviour-change should add a case here. The guard is the
boundary between "the model said something" and "Postgres ran something",
so a single bypass is catastrophic.

The cases are grouped by the rule they exercise (see the docstring of
``app.ai.security.sql_guard`` for the numbered rules).
"""

from __future__ import annotations

import pytest

from app.ai.security.sql_guard import SqlGuardError, validate

TENANT = 42
MAX = 500


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_simple_select_passes():
    sql = (
        "SELECT year_month, total_distance_km "
        "FROM marts.v_executive_dashboard "
        "WHERE tenant_id = :tenant_id "
        "ORDER BY year_month DESC LIMIT 12"
    )
    out = validate(sql, tenant_id=TENANT, max_rows=MAX)
    assert out.binds == {"tenant_id": TENANT}
    assert "v_executive_dashboard" in out.sql


def test_cte_then_select_passes():
    sql = (
        "WITH base AS ("
        "  SELECT year_month, total_trips FROM marts.v_executive_dashboard "
        "  WHERE tenant_id = :tenant_id"
        ") SELECT * FROM base ORDER BY year_month LIMIT 24"
    )
    out = validate(sql, tenant_id=TENANT, max_rows=MAX)
    assert out.binds == {"tenant_id": TENANT}


def test_limit_injected_when_missing():
    sql = (
        "SELECT year_month, total_trips FROM marts.v_executive_dashboard "
        "WHERE tenant_id = :tenant_id"
    )
    out = validate(sql, tenant_id=TENANT, max_rows=100)
    assert "LIMIT 100" in out.sql.upper()


def test_limit_clamped_when_too_high():
    sql = (
        "SELECT year_month FROM marts.v_executive_dashboard "
        "WHERE tenant_id = :tenant_id LIMIT 99999"
    )
    out = validate(sql, tenant_id=TENANT, max_rows=500)
    assert "LIMIT 500" in out.sql.upper()


def test_limit_preserved_when_under_cap():
    sql = (
        "SELECT year_month FROM marts.v_executive_dashboard "
        "WHERE tenant_id = :tenant_id LIMIT 10"
    )
    out = validate(sql, tenant_id=TENANT, max_rows=500)
    assert "LIMIT 10" in out.sql.upper()


# ---------------------------------------------------------------------------
# Rule 1: parse + single statement
# ---------------------------------------------------------------------------


def test_garbage_input_rejected():
    with pytest.raises(SqlGuardError):
        validate("not sql at all !!!", tenant_id=TENANT, max_rows=MAX)


def test_multiple_statements_rejected():
    sql = (
        "SELECT 1 FROM marts.v_executive_dashboard WHERE tenant_id = :tenant_id; "
        "SELECT 1 FROM marts.v_executive_dashboard WHERE tenant_id = :tenant_id"
    )
    with pytest.raises(SqlGuardError, match="multiple statements"):
        validate(sql, tenant_id=TENANT, max_rows=MAX)


# ---------------------------------------------------------------------------
# Rule 2 + 3: SELECT only, no DDL/DML
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM marts.v_executive_dashboard WHERE tenant_id = :tenant_id",
        "UPDATE marts.v_executive_dashboard SET total_trips=0 WHERE tenant_id = :tenant_id",
        "INSERT INTO marts.v_executive_dashboard (tenant_id) VALUES (1)",
        "DROP TABLE marts.v_executive_dashboard",
        "ALTER TABLE marts.v_executive_dashboard DROP COLUMN total_trips",
        "TRUNCATE marts.v_executive_dashboard",
        "CREATE TABLE foo AS SELECT * FROM marts.v_executive_dashboard",
    ],
)
def test_ddl_dml_rejected(sql: str):
    with pytest.raises(SqlGuardError):
        validate(sql, tenant_id=TENANT, max_rows=MAX)


def test_set_statement_rejected():
    # SET parses as exp.Command in sqlglot, which is in the forbidden list.
    with pytest.raises(SqlGuardError):
        validate("SET ROLE postgres", tenant_id=TENANT, max_rows=MAX)


# ---------------------------------------------------------------------------
# Rule 4: table whitelist
# ---------------------------------------------------------------------------


def test_unqualified_table_rejected():
    with pytest.raises(SqlGuardError, match="unqualified"):
        validate(
            "SELECT * FROM v_executive_dashboard WHERE tenant_id = :tenant_id",
            tenant_id=TENANT,
            max_rows=MAX,
        )


def test_pg_catalog_rejected():
    with pytest.raises(SqlGuardError, match="off-limits"):
        validate(
            "SELECT * FROM pg_catalog.pg_tables WHERE tenant_id = :tenant_id",
            tenant_id=TENANT,
            max_rows=MAX,
        )


def test_information_schema_rejected():
    with pytest.raises(SqlGuardError, match="off-limits"):
        validate(
            "SELECT table_name FROM information_schema.tables WHERE tenant_id = :tenant_id",
            tenant_id=TENANT,
            max_rows=MAX,
        )


def test_unknown_table_rejected():
    with pytest.raises(SqlGuardError, match="not in the analytics catalog"):
        validate(
            "SELECT * FROM marts.no_such_table WHERE tenant_id = :tenant_id",
            tenant_id=TENANT,
            max_rows=MAX,
        )


def test_pure_scalar_select_rejected():
    """SELECT 1 is technically a SELECT but references no table — reject so
    we never let the LLM hide a probe behind a meaningless query."""
    with pytest.raises(SqlGuardError, match="no catalog tables"):
        validate("SELECT 1", tenant_id=TENANT, max_rows=MAX)


# ---------------------------------------------------------------------------
# Rule 5: function denylist
# ---------------------------------------------------------------------------


def test_pg_sleep_rejected():
    sql = (
        "SELECT pg_sleep(5), tenant_id FROM marts.v_executive_dashboard "
        "WHERE tenant_id = :tenant_id"
    )
    with pytest.raises(SqlGuardError, match="pg_sleep"):
        validate(sql, tenant_id=TENANT, max_rows=MAX)


def test_current_setting_rejected():
    sql = (
        "SELECT current_setting('search_path'), tenant_id "
        "FROM marts.v_executive_dashboard WHERE tenant_id = :tenant_id"
    )
    with pytest.raises(SqlGuardError, match="current_setting"):
        validate(sql, tenant_id=TENANT, max_rows=MAX)


# ---------------------------------------------------------------------------
# Rule 6: tenant predicate
# ---------------------------------------------------------------------------


def test_missing_tenant_predicate_rejected():
    sql = "SELECT year_month FROM marts.v_executive_dashboard LIMIT 5"
    with pytest.raises(SqlGuardError, match="tenant_id"):
        validate(sql, tenant_id=TENANT, max_rows=MAX)


def test_tenant_predicate_with_alias_passes():
    sql = (
        "SELECT e.year_month FROM marts.v_executive_dashboard e "
        "WHERE e.tenant_id = :tenant_id LIMIT 5"
    )
    out = validate(sql, tenant_id=TENANT, max_rows=MAX)
    assert out.binds == {"tenant_id": TENANT}


def test_inlined_tenant_value_rejected():
    """LLM tried to inline the tenant_id integer instead of using the
    bind parameter. The guard MUST reject — otherwise the SQL is no
    longer tied to the JWT-derived tenant."""
    sql = (
        "SELECT year_month FROM marts.v_executive_dashboard "
        f"WHERE tenant_id = {TENANT} LIMIT 5"
    )
    with pytest.raises(SqlGuardError):
        validate(sql, tenant_id=TENANT, max_rows=MAX)


def test_tenant_predicate_inside_cte_passes():
    sql = (
        "WITH t AS ("
        "  SELECT year_month, total_trips "
        "  FROM marts.v_executive_dashboard "
        "  WHERE tenant_id = :tenant_id"
        ") SELECT * FROM t LIMIT 5"
    )
    out = validate(sql, tenant_id=TENANT, max_rows=MAX)
    assert out.binds == {"tenant_id": TENANT}


# ---------------------------------------------------------------------------
# Markdown fence tolerance
# ---------------------------------------------------------------------------


def test_markdown_fence_stripped():
    sql = (
        "```sql\n"
        "SELECT year_month FROM marts.v_executive_dashboard "
        "WHERE tenant_id = :tenant_id LIMIT 5\n"
        "```"
    )
    out = validate(sql, tenant_id=TENANT, max_rows=MAX)
    assert "v_executive_dashboard" in out.sql
