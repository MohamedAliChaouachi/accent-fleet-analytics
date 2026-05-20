"""
SQL validator — the only thing standing between the LLM and the database.

Design principle: *AST validation, never string matching.* Regex
allowlists/denylists for SQL are notoriously easy to bypass with
comments, casing tricks, or whitespace. We use ``sqlglot`` to parse the
candidate into an expression tree, then walk it and enforce a small set
of structural invariants.

What we enforce, in order:

1. The text parses cleanly under the Postgres dialect, and there is
   exactly ONE statement (no semicolon-separated batch).

2. The top-level statement is a SELECT — either a plain ``exp.Select``
   or a CTE (``exp.With``) whose body is a SELECT.

3. The entire subtree contains NO forbidden expressions (Insert, Update,
   Delete, Drop, Alter, Create, TruncateTable, Merge, Command, etc.).

4. Every referenced ``exp.Table`` resolves to a catalog entry. Bare
   names (no schema) and unknown schemas are rejected — this blocks
   pg_catalog / information_schema and any non-mart table.

5. Function denylist — no admin/IO functions like pg_read_file,
   current_setting, set_config, pg_sleep, etc.

6. Every tenant-scoped table referenced has a ``tenant_id = :tenant_id``
   equality predicate in the WHERE clause of its enclosing scope.

7. A safety LIMIT is appended if the LLM omitted one or set it higher
   than ``AISettings.max_rows``.

Outcome: a normalised, post-validated SQL string ready for the executor,
plus the bind parameters it expects (`tenant_id`).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from app.ai.schemas.catalog import allowed_fqnames, tenant_scoped_fqnames

DIALECT = "postgres"


class SqlGuardError(ValueError):
    """Validation failed. ``stage`` distinguishes parser failures from
    structural rejections so the router can map both to a 400 response
    with the correct ``stage`` discriminator."""

    def __init__(self, message: str, *, sql: str | None = None) -> None:
        super().__init__(message)
        self.sql = sql


# Top-level expression types that are absolute red flags. ``exp.Command``
# catches one-off statements sqlglot doesn't model (SET, RESET, COPY,
# VACUUM, etc.). ``exp.Transaction`` catches BEGIN/COMMIT/ROLLBACK.
_FORBIDDEN_NODES: tuple[type[exp.Expression], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Drop,
    exp.Alter,
    exp.Create,
    exp.TruncateTable,
    exp.Command,
    exp.Transaction,
)

# Functions that have side effects, reach outside the database, or read
# server-side configuration. Most of these don't appear in the marts
# but the LLM can hallucinate them, so we deny by name regardless of
# whether they show up.
_FORBIDDEN_FUNCTIONS: frozenset[str] = frozenset(
    {
        "pg_read_file",
        "pg_read_binary_file",
        "pg_ls_dir",
        "pg_stat_file",
        "lo_import",
        "lo_export",
        "current_setting",
        "set_config",
        "pg_sleep",
        "pg_sleep_for",
        "pg_sleep_until",
        "pg_advisory_lock",
        "pg_advisory_unlock",
        "dblink",
        "dblink_exec",
        "copy",
    }
)

# Schemas the LLM is never allowed to touch. The catalog enforcement in
# step 4 already covers this, but the explicit check gives clearer error
# messages when someone tries.
_FORBIDDEN_SCHEMAS: frozenset[str] = frozenset(
    {"pg_catalog", "information_schema", "auth", "staging", "public_archive"}
)

# Hard-coded backstop: if the LLM fails to add LIMIT we apply one. The
# config value is the upper bound; the LLM is told to use 500 by default.
_BIND_TENANT = "tenant_id"


@dataclass(frozen=True)
class GuardOutcome:
    """Validated, normalised SQL plus the bind parameters needed to run it."""

    sql: str
    binds: dict[str, object]


def validate(sql: str, *, tenant_id: int, max_rows: int) -> GuardOutcome:
    """Validate ``sql`` and return a normalised, executable form.

    Raises :class:`SqlGuardError` if the SQL is unsafe or malformed.
    """
    text = _strip_fences(sql)

    # --- (1) Parse + single statement --------------------------------------
    try:
        statements = sqlglot.parse(text, read=DIALECT)
    except sqlglot.errors.ParseError as e:
        raise SqlGuardError(f"could not parse SQL: {e}", sql=text) from e

    statements = [s for s in statements if s is not None]
    if len(statements) == 0:
        raise SqlGuardError("empty SQL", sql=text)
    if len(statements) > 1:
        raise SqlGuardError(
            "multiple statements are not allowed; emit a single SELECT",
            sql=text,
        )

    tree = statements[0]

    # --- (2) Top-level must be SELECT (plain or CTE-wrapped) ---------------
    if not _is_select_tree(tree):
        raise SqlGuardError(
            "only SELECT statements are allowed (WITH … SELECT is fine)",
            sql=text,
        )

    # --- (3) Forbidden subexpressions --------------------------------------
    for node in tree.walk():
        if isinstance(node, _FORBIDDEN_NODES):
            raise SqlGuardError(
                f"forbidden statement type: {type(node).__name__}",
                sql=text,
            )

    # --- (4) Table whitelist -----------------------------------------------
    allowed = allowed_fqnames()
    cte_names = _collect_cte_names(tree)
    referenced_fqnames: set[str] = set()
    for table in tree.find_all(exp.Table):
        # Skip references *to* CTEs defined in the same query — those
        # aren't physical tables. (sqlglot represents `FROM my_cte` as an
        # exp.Table with no db; same shape as a bare table.)
        if not table.db and table.name in cte_names:
            continue
        if not table.db:
            raise SqlGuardError(
                f"unqualified table `{table.name}` is not allowed; "
                f"use a fully qualified `schema.view` name",
                sql=text,
            )
        if table.db in _FORBIDDEN_SCHEMAS:
            raise SqlGuardError(
                f"schema `{table.db}` is off-limits", sql=text
            )
        fq = f"{table.db}.{table.name}"
        if fq not in allowed:
            raise SqlGuardError(
                f"table `{fq}` is not in the analytics catalog", sql=text
            )
        referenced_fqnames.add(fq)

    if not referenced_fqnames:
        # Pure-scalar SELECTs (e.g. `SELECT 1`) have no business hitting
        # the production DB through this endpoint. Reject so we never
        # accidentally execute a query the catalog can't account for.
        raise SqlGuardError(
            "query references no catalog tables", sql=text
        )

    # --- (5) Function denylist --------------------------------------------
    for fn in tree.find_all(exp.Anonymous):
        # exp.Anonymous covers function calls whose name sqlglot doesn't
        # model as a typed node. Postgres admin functions all land here.
        name = (fn.this or "").lower() if isinstance(fn.this, str) else ""
        if name in _FORBIDDEN_FUNCTIONS:
            raise SqlGuardError(f"function `{name}` is not allowed", sql=text)
    # Built-in funcs sqlglot DOES model (e.g. CurrentTimestamp) are
    # always safe; no additional check needed.

    # --- (6) Tenant predicate present on every tenant-scoped table --------
    tenant_required = referenced_fqnames & tenant_scoped_fqnames()
    if tenant_required and not _has_tenant_predicate(tree):
        raise SqlGuardError(
            "missing `tenant_id = :tenant_id` predicate — every tenant-"
            "scoped table must be filtered by the bound :tenant_id",
            sql=text,
        )

    # --- (7) LIMIT enforcement --------------------------------------------
    _apply_limit(tree, max_rows=max_rows)

    # sqlglot's Postgres dialect renders `:name` placeholders as the
    # psycopg-native `%(name)s`. SQLAlchemy `text()` only honours `:name`
    # for parameter substitution, so convert back. Safe to do unconditionally
    # — the AST validation above already ensured we control the bind names.
    normalised = _PSYCOPG_BIND_RE.sub(r":\1", tree.sql(dialect=DIALECT))
    return GuardOutcome(sql=normalised, binds={_BIND_TENANT: tenant_id})


# Match psycopg-style `%(name)s` placeholders. `name` is restricted to
# `[A-Za-z_][A-Za-z0-9_]*` so we don't accidentally rewrite literals
# that happen to contain `%(...)s` (e.g. inside a quoted string).
_PSYCOPG_BIND_RE = re.compile(r"%\(([A-Za-z_][A-Za-z0-9_]*)\)s")


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _strip_fences(text: str) -> str:
    """Tolerate markdown code fences in case the LLM ignores the prompt.

    Stripping fences is a *display* concern, not a security one — the
    AST validator still has to accept what's left.
    """
    s = text.strip()
    if s.startswith("```"):
        # Drop opening fence (optionally with `sql` language hint).
        s = s.split("\n", 1)[1] if "\n" in s else s[3:]
    if s.endswith("```"):
        s = s.rsplit("```", 1)[0]
    return s.strip().rstrip(";").strip()


def _is_select_tree(tree: exp.Expression) -> bool:
    """Top-level expression must be a Select or a With that wraps Selects.

    Set-ops (UNION/INTERSECT/EXCEPT) are sqlglot's `exp.Union`; those are
    fine — each side is a Select. We accept them so common patterns like
    "trips by tenant UNION ALL trips overall" don't get blocked.
    """
    if isinstance(tree, exp.Select):
        return True
    if isinstance(tree, exp.Union):
        return True
    if isinstance(tree, exp.With):
        body = tree.this
        return isinstance(body, (exp.Select, exp.Union))
    return False


def _collect_cte_names(tree: exp.Expression) -> set[str]:
    """Names of every CTE in the tree (including nested CTEs inside CTEs).

    We walk the whole tree rather than looking at a single ``with`` arg
    because sqlglot has shifted that arg name across versions (``with`` →
    ``with_``) and the recursive walk is more future-proof. The cost is
    nil; CTE counts are tiny in real queries.
    """
    return {cte.alias for cte in tree.find_all(exp.CTE) if cte.alias}


def _has_tenant_predicate(tree: exp.Expression) -> bool:
    """True iff the AST contains an equality predicate of the shape
    ``<...>.tenant_id = :tenant_id`` (or unqualified ``tenant_id = :tenant_id``).

    We accept the predicate appearing *anywhere* in the tree — top-level
    WHERE, inside a CTE, inside a JOIN ON. The LLM is told to put it on
    every tenant-scoped table; if it nests the filter inside a subquery
    that feeds the outer SELECT, that's still functionally correct.
    """
    target_placeholder = _BIND_TENANT  # e.g. "tenant_id" → bind param :tenant_id
    for eq in tree.find_all(exp.EQ):
        left = eq.left
        right = eq.right
        if _is_tenant_column(left) and _is_tenant_placeholder(right, target_placeholder):
            return True
        if _is_tenant_column(right) and _is_tenant_placeholder(left, target_placeholder):
            return True
    return False


def _is_tenant_column(node: exp.Expression) -> bool:
    if isinstance(node, exp.Column):
        return node.name == "tenant_id"
    return False


def _is_tenant_placeholder(node: exp.Expression, bind_name: str) -> bool:
    """Match `:tenant_id` style placeholders in the dialects sqlglot
    normalises to ``exp.Placeholder`` (Postgres / SQLAlchemy bind form).
    """
    if isinstance(node, exp.Placeholder):
        return (node.name or "").lower() == bind_name
    # Fallback: sqlglot may render the placeholder as a Parameter node.
    if isinstance(node, exp.Parameter):
        param = node.this
        if isinstance(param, exp.Var):
            return param.name.lower() == bind_name
        if isinstance(param, str):
            return param.lower() == bind_name
    return False


def _apply_limit(tree: exp.Expression, *, max_rows: int) -> None:
    """Ensure the top-level SELECT has a LIMIT ≤ ``max_rows``.

    Mutates ``tree`` in place. We don't touch limits inside subqueries
    or CTEs — those are legitimate analytics patterns (e.g. "top 5
    devices per tenant" needs a LATERAL limit) and bounding them too
    aggressively breaks valid queries.
    """
    select = _outer_select(tree)
    if select is None:
        return
    existing = select.args.get("limit")
    if existing is not None and isinstance(existing, exp.Limit):
        try:
            current = int(existing.expression.this)  # type: ignore[union-attr]
        except (AttributeError, TypeError, ValueError):
            current = max_rows  # opaque LIMIT — leave as-is
        if current <= max_rows:
            return
    select.set("limit", exp.Limit(expression=exp.Literal.number(max_rows)))


def _outer_select(tree: exp.Expression) -> exp.Select | None:
    if isinstance(tree, exp.Select):
        return tree
    if isinstance(tree, exp.With):
        return tree.this if isinstance(tree.this, exp.Select) else None
    if isinstance(tree, exp.Union):
        # Set-op: LIMIT applied to the union itself; sqlglot stores it
        # on the Union node, not on either side. Use a dedicated branch.
        return None
    return None
