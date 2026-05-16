"""
AI sub-package: natural-language → SQL analytics assistant.

Public surface:
    app.ai.routers.ai_query.router   FastAPI router mounted at /v1/ai/*

Everything else is internal. The pipeline is intentionally pure-Python and
provider-agnostic so the request flow can be unit-tested without a real
LLM by injecting :class:`app.ai.providers.base.BaseLLMProvider`.
"""
