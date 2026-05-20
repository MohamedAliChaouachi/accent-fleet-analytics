"""
BaseLLMProvider — the seam the pipeline talks to.

The interface is deliberately narrow: two methods, both string-in/
string-out. We don't expose vendor-specific concepts (response_format,
tool calling, streaming) because every method on the base class is one
more thing every provider has to implement. Phase 2's streaming work
will add a `stream(...)` method here.

All providers are sync. FastAPI runs sync route handlers in a thread
pool, so a 1–3 s LLM call doesn't block the event loop. Making the
providers async would require us to maintain an async HTTP client per
vendor and to thread async through every layer — not worth the
complexity at the volume of /ai/query traffic we expect at v1.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


class LLMProviderError(RuntimeError):
    """Raised when the upstream LLM call fails (network, auth, rate limit,
    etc.). The pipeline converts this into a structured 502 response."""


@dataclass(frozen=True, slots=True)
class LLMResponse:
    """Plain transcript — provider-agnostic. ``text`` is the assistant's
    message stripped of any markdown fences the model may have added."""

    text: str
    model: str  # actual model id used, for the response payload


class BaseLLMProvider(ABC):
    """Contract every provider implements.

    Two methods, intentionally:
      - generate_sql(): low-temperature, format-constrained "produce one
        SELECT" call. The pipeline post-validates with sqlglot regardless
        of how strict the system prompt is.
      - summarize_result(): higher-latitude "explain these rows in one
        sentence" call, grounded on the actual result rows.
    """

    name: str  # "openai" | "anthropic" | "stub"
    model: str

    @abstractmethod
    def generate_sql(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        """Return a single SELECT statement (plus optional CTEs).

        Implementations MUST:
          - Use a low temperature (≤ 0.2) so the same question maps to
            the same SQL across retries.
          - Set provider-side timeouts honouring ``AISettings.llm_timeout_seconds``.
          - Raise ``LLMProviderError`` on transport / auth failures so
            the pipeline can format a clean 502.
        """

    @abstractmethod
    def summarize_result(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        """One-sentence natural language answer derived from result rows.

        Pipeline must remain functional if this raises — the executor
        falls back to a deterministic templated summary. Implementations
        SHOULD still raise on hard failures so we can log them.
        """
