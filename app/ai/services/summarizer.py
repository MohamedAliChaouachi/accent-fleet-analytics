"""
Result summariser.

Best-effort second LLM call that produces the one-sentence answer. If it
fails (timeout, rate limit, transport error) we fall back to a
deterministic templated answer rather than failing the whole request —
the user already has the SQL and rows; a missing summary is degraded UX,
not a broken response.
"""

from __future__ import annotations

import logging
from typing import Any

from app.ai.config import AISettings
from app.ai.prompts.builder import build_summary_user_prompt
from app.ai.prompts.system import SUMMARY_SYSTEM_PROMPT
from app.ai.providers.base import BaseLLMProvider, LLMProviderError

log = logging.getLogger(__name__)


def summarize(
    *,
    provider: BaseLLMProvider,
    question: str,
    sql: str,
    rows: list[dict[str, Any]],
    settings: AISettings,
) -> str:
    """Return a one-sentence answer. Never raises."""
    # No rows: skip the LLM and answer directly.
    if not rows:
        return "No results found for your question."

    # Build the summary prompt from the question, SQL, and sampled rows.
    user_prompt = build_summary_user_prompt(
        question=question,
        sql=sql,
        rows=rows,
        sample_rows=settings.summary_sample_rows,
    )
    # Call the LLM; on failure degrade to the deterministic fallback.
    try:
        resp = provider.summarize_result(SUMMARY_SYSTEM_PROMPT, user_prompt)
    except LLMProviderError as e:
        log.info("ai.summarizer.fallback", extra={"error": str(e)})
        return _fallback_summary(rows)
    # Squash any newlines a model adds despite the prompt asking for one
    # sentence — the React UI shows this in a single line.
    return " ".join(resp.text.strip().split())


# Row-count-only summary used when the LLM call fails.
def _fallback_summary(rows: list[dict[str, Any]]) -> str:
    """Deterministic answer derived from the result shape only."""
    n = len(rows)
    if n == 1:
        return "Returned a single row."
    return f"Returned {n} rows."
