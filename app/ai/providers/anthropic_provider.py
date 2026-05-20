"""
Anthropic Claude provider.

Mirrors OpenAIProvider in shape so the factory can swap providers via
the AI_PROVIDER env var with no other code changes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from app.ai.config import AISettings
from app.ai.providers.base import BaseLLMProvider, LLMProviderError, LLMResponse

if TYPE_CHECKING:  # pragma: no cover
    from anthropic import Anthropic

# Bounded by Anthropic's max_tokens parameter. SQL responses and one-line
# summaries are both well under this; bumping it later if needed is a
# config tweak, not a code change.
_MAX_TOKENS = 1024


class AnthropicProvider(BaseLLMProvider):
    name = "anthropic"

    def __init__(self, settings: AISettings) -> None:
        try:
            from anthropic import Anthropic
        except ImportError as e:  # pragma: no cover
            raise LLMProviderError(
                "AI_PROVIDER=anthropic but the `anthropic` package is not "
                "installed. Add `anthropic>=0.34.0` to requirements.txt."
            ) from e
        if not settings.anthropic_api_key:
            raise LLMProviderError(
                "ANTHROPIC_API_KEY is empty — refusing to construct AnthropicProvider."
            )
        self._client: Anthropic = Anthropic(
            api_key=settings.anthropic_api_key,
            timeout=settings.llm_timeout_seconds,
        )
        self.model = settings.anthropic_model

    def generate_sql(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        return self._chat(system_prompt, user_prompt, temperature=0.0)

    def summarize_result(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        return self._chat(system_prompt, user_prompt, temperature=0.2)

    def _chat(self, system_prompt: str, user_prompt: str, *, temperature: float) -> LLMResponse:
        try:
            resp = self._client.messages.create(
                model=self.model,
                max_tokens=_MAX_TOKENS,
                temperature=temperature,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
        except Exception as e:  # noqa: BLE001
            raise LLMProviderError(f"anthropic messages call failed: {e}") from e
        # Anthropic returns a list of content blocks; for a plain text
        # response there's exactly one of type "text". Be defensive in
        # case the API ever returns tool_use blocks alongside text.
        chunks: list[str] = []
        for block in resp.content:
            text = getattr(block, "text", None)
            if isinstance(text, str):
                chunks.append(text)
        joined = "".join(chunks).strip()
        if not joined:
            raise LLMProviderError("anthropic returned no text content")
        return LLMResponse(text=joined, model=resp.model or self.model)
