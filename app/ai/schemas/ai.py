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

# Hard cap on conversational context sent back to us. Picked at 6 to
# keep prompt cost bounded (≈ 3 Q&A pairs) while giving the model
# enough recent context to resolve follow-ups like "and last week?".
# The router trims anything longer before it reaches the pipeline.
MAX_HISTORY_TURNS = 6


class ChatTurn(BaseModel):
    """One previous exchange in the conversation.

    ``content`` is the natural-language text only — for assistant turns
    we send the one-line summary, not the SQL or rows, to keep prompts
    small. The pipeline never re-uses prior SQL; it always regenerates
    from the catalog so guardrails apply uniformly to every call.
    """

    role: Literal["user", "assistant"]
    content: str = Field(..., min_length=1, max_length=4_000)


class AIQueryRequest(BaseModel):
    """User's natural-language question.

    ``tenant_id`` is OPTIONAL and only honored for superadmin principals.
    For all other roles the server uses the tenant from the JWT and
    rejects mismatched body values — see app/ai/routers/ai_query.py.

    ``history`` is the prior turns of the current chat session, oldest
    first. Optional — when empty the call behaves exactly like the v1
    one-shot endpoint. Server trims to the last ``MAX_HISTORY_TURNS``
    turns so a runaway client can't blow up the prompt budget.
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
    history: list[ChatTurn] = Field(
        default_factory=list,
        description=(
            "Prior turns of this chat session, oldest first. Capped to "
            f"the last {MAX_HISTORY_TURNS} turns server-side."
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
