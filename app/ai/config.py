"""
AI-assistant settings.

Kept separate from the global `accent_fleet.config.settings` so the AI
feature can be deployed independently (e.g. behind a feature flag) and so
secrets that only the AI path needs — provider keys, model names — don't
widen the global settings surface.

Reads from environment variables; an empty/missing API key for the chosen
provider is treated as "feature disabled" by :mod:`app.ai.providers.factory`.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

Provider = Literal["openai", "anthropic", "stub"]


class AISettings(BaseSettings):
    """Runtime configuration for the Text2SQL assistant.

    ``provider`` selects the LLM backend. ``stub`` is used in tests; it
    short-circuits the network call with a deterministic SQL response so
    the pipeline can be exercised end-to-end without API keys.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    provider: Provider = Field("openai", alias="AI_PROVIDER")

    # --- OpenAI ---
    openai_api_key: str = Field("", alias="OPENAI_API_KEY")
    openai_model: str = Field("gpt-4o-mini", alias="OPENAI_MODEL")
    openai_base_url: str | None = Field(None, alias="OPENAI_BASE_URL")

    # --- Anthropic ---
    anthropic_api_key: str = Field("", alias="ANTHROPIC_API_KEY")
    anthropic_model: str = Field("claude-3-5-sonnet-latest", alias="ANTHROPIC_MODEL")

    # --- Execution guardrails ---
    # Hard cap on rows the executor will return. Two reasons: (1) cap the
    # response payload size so a misclicked "select *" doesn't OOM the
    # client, (2) reduce blast radius if the SQL guard ever misses
    # something. The LLM is also told this in the prompt.
    max_rows: int = Field(500, alias="AI_MAX_ROWS", ge=1, le=10_000)

    # Statement timeout applied as `SET LOCAL statement_timeout` inside
    # the read-only transaction. Defense against runaway SQL the LLM
    # might produce against marts that grow over time.
    statement_timeout_ms: int = Field(15_000, alias="AI_STATEMENT_TIMEOUT_MS", ge=500)

    # LLM call timeout (seconds). Two LLM calls per /ai/query — SQL and
    # summary — so the user-perceived ceiling is roughly 2× this.
    llm_timeout_seconds: float = Field(30.0, alias="AI_LLM_TIMEOUT_S", ge=1.0)

    # Cap how many rows we feed back into the summariser. Sending 500
    # rows of detail to the LLM bloats the second prompt and rarely
    # changes the quality of a one-sentence answer.
    summary_sample_rows: int = Field(20, alias="AI_SUMMARY_SAMPLE_ROWS", ge=1, le=200)

    # --- Rate limiting ---
    # Two independent ceilings, both enforced before the LLM call.
    # See app/ai/services/rate_limit.py for the rationale of two scopes.
    rate_limit_user_max: int = Field(
        20, alias="AI_RATE_LIMIT_USER_MAX", ge=1, le=10_000
    )
    rate_limit_tenant_max: int = Field(
        60, alias="AI_RATE_LIMIT_TENANT_MAX", ge=1, le=100_000
    )
    rate_limit_window_seconds: int = Field(
        60, alias="AI_RATE_LIMIT_WINDOW_S", ge=1, le=3600
    )


@lru_cache(maxsize=1)
def ai_settings() -> AISettings:
    return AISettings()  # type: ignore[call-arg]
