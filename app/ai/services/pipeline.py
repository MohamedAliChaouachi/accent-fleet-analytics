"""
Orchestrator: question → SQL → rows → response.

This is the only file the router needs to import to run a query. Each
stage is broken out into a small, individually-testable helper so the
flow reads top-to-bottom like a state machine:

    1. Build prompts (system + user) from the question and catalog.
    2. Call the LLM to produce candidate SQL.
    3. Validate + normalise the SQL with sql_guard.
    4. Execute against Postgres in a read-only transaction.
    5. Suggest a chart type from the result shape.
    6. Ask the LLM for a one-sentence summary of the rows.
    7. Assemble the response.

Errors at any stage are wrapped into :class:`PipelineError` carrying a
``stage`` discriminator so the router can map them to the right HTTP
status and surface a structured error body.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Literal

from app.ai.config import AISettings
from app.ai.prompts.builder import build_sql_user_prompt
from app.ai.prompts.system import SQL_SYSTEM_PROMPT
from app.ai.providers.base import BaseLLMProvider, LLMProviderError
from app.ai.schemas.ai import AIQueryResponse, ChartType, ChatTurn
from app.ai.security.sql_guard import SqlGuardError, validate
from app.ai.services.chart_suggester import suggest
from app.ai.services.executor import ExecutorError, execute
from app.ai.services.summarizer import summarize

Stage = Literal["llm", "sql_guard", "tenant_filter", "execution", "summarization", "config"]


# Single error type carrying the failing stage for HTTP mapping.
class PipelineError(RuntimeError):
    """Wraps any per-stage failure with the stage that produced it."""

    def __init__(self, stage: Stage, detail: str, *, sql: str | None = None) -> None:
        super().__init__(f"{stage}: {detail}")
        self.stage: Stage = stage
        self.detail = detail
        self.sql = sql


# Immutable input bundle for one pipeline run.
@dataclass(frozen=True, slots=True)
class PipelineInput:
    question: str
    tenant_id: int
    # Prior turns of the current chat session, oldest first. The router
    # has already trimmed this to the per-request cap (MAX_HISTORY_TURNS
    # in app/ai/schemas/ai.py), so the pipeline can pass it through
    # without further bounds-checking.
    history: tuple[ChatTurn, ...] = ()


def run(
    *,
    inp: PipelineInput,
    provider: BaseLLMProvider,
    settings: AISettings,
) -> AIQueryResponse:
    """Execute the full pipeline. Raises :class:`PipelineError` on failure."""
    t0 = time.perf_counter()

    # 1+2: prompt → LLM → SQL
    user_prompt = build_sql_user_prompt(
        question=inp.question,
        tenant_id=inp.tenant_id,
        history=inp.history,
    )
    try:
        llm_resp = provider.generate_sql(SQL_SYSTEM_PROMPT, user_prompt)
    except LLMProviderError as e:
        raise PipelineError("llm", str(e)) from e

    candidate_sql = llm_resp.text

    # 3: validate
    try:
        outcome = validate(
            candidate_sql,
            tenant_id=inp.tenant_id,
            max_rows=settings.max_rows,
        )
    except SqlGuardError as e:
        raise PipelineError("sql_guard", str(e), sql=e.sql or candidate_sql) from e

    # 4: execute
    try:
        result = execute(outcome.sql, binds=outcome.binds, settings=settings)
    except ExecutorError as e:
        raise PipelineError("execution", str(e), sql=outcome.sql) from e

    # 5: chart heuristic
    chart: ChartType = suggest(result.columns, result.rows)

    # 6: summary (never raises — best-effort)
    summary = summarize(
        provider=provider,
        question=inp.question,
        sql=outcome.sql,
        rows=result.rows,
        settings=settings,
    )

    # 7: assemble the response with timing and provider/model metadata
    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    return AIQueryResponse(
        question=inp.question,
        sql=outcome.sql,
        rows=result.rows,
        row_count=result.row_count,
        columns=result.columns,
        summary=summary,
        chart_type=chart,
        provider=provider.name,
        model=llm_resp.model,
        elapsed_ms=elapsed_ms,
    )
