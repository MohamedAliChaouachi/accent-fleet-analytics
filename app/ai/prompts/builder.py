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
from collections.abc import Sequence
from typing import Any

from app.ai.schemas.ai import ChatTurn
from app.ai.schemas.catalog import CATALOG, TableSpec


# Flatten every catalog entry into one text block for the prompt.
def render_catalog() -> str:
    """Render the whole catalog as plain text for the user message."""
    blocks: list[str] = ["Available views (use the fully qualified name):", ""]
    # One rendered block per table, blank-line separated.
    for spec in CATALOG.values():
        blocks.append(_render_table(spec))
        blocks.append("")
    return "\n".join(blocks).rstrip() + "\n"


# Render one table's header, grain, tenant-scope note, and column list.
def _render_table(spec: TableSpec) -> str:
    # Spell out the tenant-filter requirement so the LLM can't miss it.
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
    # Append each column as "name (type) — description".
    for c in spec.columns:
        suffix = f" — {c.description}" if c.description else ""
        lines.append(f"      {c.name} ({c.type}){suffix}")
    return "\n".join(lines)


def _render_history(history: Sequence[ChatTurn]) -> str:
    """Render prior chat turns as a plain text block for the user message.

    We deliberately keep this as text in the user message rather than
    expanding it into multi-turn ``messages`` against the chat API:
    a single user message keeps the providers interchangeable (the
    Anthropic provider has slightly different multi-turn semantics)
    and the system prompt stays the single source of truth for
    SQL-generation rules.
    """
    lines = ["Previous conversation (oldest first):", ""]
    # Label each turn by speaker so the model can resolve back-references.
    for turn in history:
        speaker = "User" if turn.role == "user" else "Assistant"
        lines.append(f"{speaker}: {turn.content.strip()}")
    return "\n".join(lines)


def build_sql_user_prompt(
    question: str,
    tenant_id: int,
    history: Sequence[ChatTurn] = (),
) -> str:
    """Assemble the user message for the SQL-generation call.

    The tenant_id is included so the model has the value visible (useful
    for reasoning about scope), but the SQL it emits must still use the
    bind parameter `:tenant_id` — the server binds the *real* value at
    execute time. We restate that here as a backstop against the LLM
    inlining the integer.

    When ``history`` is non-empty the conversation is rendered between
    the catalog and the current question so the model can resolve
    references like "and last week?" against earlier turns. Each call
    still regenerates SQL from scratch — the guardrails apply
    uniformly regardless of history.
    """
    # Start with the catalog, then optionally splice in prior turns.
    sections: list[str] = [render_catalog(), "---"]
    if history:
        sections.append(_render_history(history))
        sections.append("---")
    # Restate the tenant binding rule and append the current question last.
    sections.extend(
        [
            f"Caller tenant_id: {tenant_id}  (do NOT inline; use `:tenant_id`)",
            "",
            "Current user question:" if history else "User question:",
            question.strip(),
        ]
    )
    return "\n".join(sections)


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
    # Truncate to the sample cap and wrap with the row count for context.
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
