"""Provider factory.

Single entry point — :func:`get_provider` — returns a singleton
configured from environment variables. Tests inject a stub via
:func:`set_provider_override` so they don't need API keys.
"""

from __future__ import annotations

from threading import Lock

from app.ai.config import AISettings, ai_settings
from app.ai.providers.base import BaseLLMProvider, LLMProviderError, LLMResponse

_lock = Lock()
_singleton: BaseLLMProvider | None = None
_override: BaseLLMProvider | None = None


class _StubProvider(BaseLLMProvider):
    """Deterministic non-network provider for tests.

    Returns a canned `SELECT 1` so the pipeline can be exercised without
    real API keys. Test code that needs more interesting behaviour should
    inject its own subclass via :func:`set_provider_override`.
    """

    name = "stub"
    model = "stub-1"

    def generate_sql(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        return LLMResponse(
            text="SELECT 1 AS stub WHERE tenant_id = :tenant_id LIMIT 1",
            model=self.model,
        )

    def summarize_result(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        return LLMResponse(text="Stub provider response.", model=self.model)


def _build(settings: AISettings) -> BaseLLMProvider:
    if settings.provider == "stub":
        return _StubProvider()
    if settings.provider == "openai":
        from app.ai.providers.openai_provider import OpenAIProvider

        return OpenAIProvider(settings)
    if settings.provider == "anthropic":
        from app.ai.providers.anthropic_provider import AnthropicProvider

        return AnthropicProvider(settings)
    if settings.provider == "bedrock":
        from app.ai.providers.bedrock_provider import BedrockProvider

        return BedrockProvider(settings)
    raise LLMProviderError(f"unknown AI_PROVIDER={settings.provider!r}")


def get_provider() -> BaseLLMProvider:
    """Return the configured singleton, or the test-injected override."""
    if _override is not None:
        return _override
    global _singleton
    with _lock:
        if _singleton is None:
            _singleton = _build(ai_settings())
    return _singleton


def set_provider_override(provider: BaseLLMProvider | None) -> None:
    """Tests only — install a fake provider that bypasses env config."""
    global _override
    _override = provider
