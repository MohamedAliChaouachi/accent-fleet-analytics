"""
Wire format for /v1/ai/query.

Kept intentionally small: the v1 contract is one POST in, one JSON out.
Phase 2 will extend this with streaming and chat history; the response
shape here is the stable subset both phases will share, so the React
client can be built against it today.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

ChartType = Literal["line", "bar", "pie", "table"]


class AIQueryRequest(BaseModel):
    """User's natural-language question.

    ``tenant_id`` is OPTIONAL and only honored for superadmin principals.
    For all other roles the server uses the tenant from the JWT and
    rejects mismatched body values — see app/ai/routers/ai_query.py.
    """

    question: str = Field(..., min_length=3, max_length=2_000)
    tenant_id: int | None = Field(
        default=None,
        description=(
            "Superadmin-only override. Tenant users have their tenant "
            "derived from the JWT; this field is rejected if it doesn't "
            "match the caller's tenant_id."
        ),
    )


class AIQueryError(BaseModel):
    """Structured error body. Returned with non-2xx responses where the
    failure happened *inside* the pipeline (e.g. SQL guard rejection)
    rather than during request parsing."""

    stage: Literal[
        "llm",
        "sql_guard",
        "tenant_filter",
        "execution",
        "summarization",
        "config",
    ]
    detail: str
    # Surface the offending SQL when the failure is the LLM's fault. This
    # is invaluable in production triage — without it, an "SQL rejected"
    # message is opaque. Safe to expose: tenant_id is server-bound so the
    # SQL never contains the literal value.
    sql: str | None = None


class AIQueryResponse(BaseModel):
    """Successful answer.

    ``rows`` is JSON-serialisable: every cell is one of int/float/str/bool/
    None/list/dict. Datetimes are stringified ISO-8601 by the executor.
    """

    question: str
    sql: str
    rows: list[dict[str, Any]]
    row_count: int
    columns: list[str]
    summary: str
    chart_type: ChartType
    # Lightweight debug payload — handy for the React "show the SQL"
    # toggle, opt-in surfacing in dev. Pure metadata; safe to return.
    provider: str
    model: str
    elapsed_ms: int
