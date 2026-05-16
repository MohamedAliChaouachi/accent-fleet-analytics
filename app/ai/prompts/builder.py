"""
Prompt builders.

The user message we send to the LLM is a structured rendering of the
schema catalog + the user's question + the (server-side) tenant_id.

We render the *full* catalog every call for Phase 1. Phase 2 will add a
retrieval step (embedding the question, picking the top-K views) once
the catalog grows past ~30 tables. At seven views we're still well
under any reasonable prompt budget and the LLM consistently picks the
right table when given the descriptions.
"""

from __future__ import annotations

import json
from typing import Any

from app.ai.schemas.catalog import CATALOG, TableSpec


def render_catalog() -> str:
    """Render the whole catalog as plain text for the user message."""
    blocks: list[str] = ["Available views (use the fully qualified name):", ""]
    for spec in CATALOG.values():
        blocks.append(_render_table(spec))
        blocks.append("")
    return "\n".join(blocks).rstrip() + "\n"


def _render_table(spec: TableSpec) -> str:
    tenant_note = (
        "tenant-scoped — MUST filter with `tenant_id = :tenant_id`"
        if spec.tenant_scoped
        else "global (no tenant filter required)"
    )
    lines = [
        f"### {spec.fqname}",
        f"  - {spec.description}",
        f"  - Grain: {spec.grain}",
        f"  - {tenant_note}",
        "  - Columns:",
    ]
    for c in spec.columns:
        suffix = f" — {c.description}" if c.description else ""
        lines.append(f"      {c.name} ({c.type}){suffix}")
    return "\n".join(lines)


def build_sql_user_prompt(question: str, tenant_id: int) -> str:
    """Assemble the user message for the SQL-generation call.

    The tenant_id is included so the model has the value visible (useful
    for reasoning about scope), but the SQL it emits must still use the
    bind parameter `:tenant_id` — the server binds the *real* value at
    execute time. We restate that here as a backstop against the LLM
    inlining the integer.
    """
    return "\n".join(
        [
            render_catalog(),
            "---",
            f"Caller tenant_id: {tenant_id}  (do NOT inline; use `:tenant_id`)",
            "",
            "User question:",
            question.strip(),
        ]
    )


def build_summary_user_prompt(
    question: str,
    sql: str,
    rows: list[dict[str, Any]],
    sample_rows: int,
) -> str:
    """Assemble the user message for the result-summarisation call.

    Rows are truncated to ``sample_rows``. We send JSON rather than a
    pretty table because the LLM parses it more reliably and it keeps
    column names attached to values (vital for accurate one-liner
    answers about specific fields).
    """
    sample = rows[:sample_rows]
    body = {
        "question": question,
        "row_count": len(rows),
        "sample": sample,
    }
    # default=str catches Decimal / date / datetime / UUID without us
    # having to special-case each one. The executor already stringifies
    # most of those, but defensive default keeps this function safe to
    # call from anywhere.
    return (
        "Question:\n"
        f"{question.strip()}\n\n"
        "SQL executed:\n"
        f"{sql}\n\n"
        "Result (JSON):\n"
        f"{json.dumps(body, default=str)}"
    )
