"""
OpenAI-compatible provider.

Talks to the standard chat completions endpoint. Works with any
provider that exposes the same wire format (Azure OpenAI, OpenRouter,
local llama.cpp servers behind the OpenAI shim, ...) by overriding
``OPENAI_BASE_URL``.

The ``openai`` SDK is imported lazily so a missing dependency only
breaks calls that actually need OpenAI — the rest of the app still
boots. If you set ``AI_PROVIDER=openai`` without installing the SDK,
the factory raises a clear configuration error.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from app.ai.config import AISettings
from app.ai.providers.base import BaseLLMProvider, LLMProviderError, LLMResponse

if TYPE_CHECKING:  # pragma: no cover
    from openai import OpenAI


class OpenAIProvider(BaseLLMProvider):
    name = "openai"

    def __init__(self, settings: AISettings) -> None:
        try:
            from openai import OpenAI
        except ImportError as e:  # pragma: no cover
            raise LLMProviderError(
                "AI_PROVIDER=openai but the `openai` package is not "
                "installed. Add `openai>=1.40.0` to requirements.txt."
            ) from e
        if not settings.openai_api_key:
            raise LLMProviderError(
                "OPENAI_API_KEY is empty — refusing to construct OpenAIProvider."
            )
        kwargs: dict[str, object] = {
            "api_key": settings.openai_api_key,
            "timeout": settings.llm_timeout_seconds,
        }
        if settings.openai_base_url:
            kwargs["base_url"] = settings.openai_base_url
        self._client: OpenAI = OpenAI(**kwargs)  # type: ignore[arg-type]
        self.model = settings.openai_model

    def generate_sql(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        return self._chat(system_prompt, user_prompt, temperature=0.0)

    def summarize_result(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        # Slightly higher temperature so the summary doesn't feel
        # robotic, but capped — we still want grounded answers.
        return self._chat(system_prompt, user_prompt, temperature=0.2)

    def _chat(self, system_prompt: str, user_prompt: str, *, temperature: float) -> LLMResponse:
        try:
            resp = self._client.chat.completions.create(
                model=self.model,
                temperature=temperature,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
        except Exception as e:  # noqa: BLE001 — surface SDK errors uniformly
            raise LLMProviderError(f"openai chat call failed: {e}") from e
        if not resp.choices:
            raise LLMProviderError("openai returned no choices")
        text = (resp.choices[0].message.content or "").strip()
        if not text:
            raise LLMProviderError("openai returned empty content")
        return LLMResponse(text=text, model=resp.model or self.model)
