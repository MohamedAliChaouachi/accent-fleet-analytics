"""
Router tests for POST /v1/ai/query.

These tests stay above the pipeline boundary — the pipeline itself is
covered by ``test_pipeline.py`` and the SQL guard by ``test_sql_guard.py``.
Here we verify only the router's two responsibilities:

  1. **Tenant resolution.** The tenant_id the pipeline receives must
     come from the JWT principal, not the request body, for any
     non-superadmin role. Superadmins must pass it explicitly.

  2. **Stage → HTTP mapping.** A PipelineError carrying ``stage="llm"``
     becomes 502, ``stage="sql_guard"`` becomes 400, etc.

To exercise these without standing up Postgres or hitting an LLM, we:

  * Override ``_require_principal`` via ``app.dependency_overrides`` so
    each test injects whichever Principal it wants.
  * Monkey-patch ``app.ai.routers.ai_query.run`` so the test controls
    what the pipeline appears to return (or raise).
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.ai.routers import ai_query as ai_router_module
from app.ai.schemas.ai import AIQueryResponse
from app.ai.services.pipeline import PipelineError, PipelineInput
from app.auth.deps import _require_principal
from app.auth.principal import Principal
from app.main import app

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _advisory_auth(monkeypatch: pytest.MonkeyPatch):
    """Pin AUTH_ENFORCEMENT=advisory so the suite exercises route logic
    rather than the JWT middleware (which has its own tests). Without
    this fixture, a local `.env` with `AUTH_ENFORCEMENT=enforce` makes
    every request 401 before the route handler runs — same gotcha
    tests/test_api_dashboards.py works around.
    """
    from accent_fleet.config import settings as settings_fn

    monkeypatch.setenv("AUTH_ENFORCEMENT", "advisory")
    settings_fn.cache_clear()
    yield
    settings_fn.cache_clear()


@pytest.fixture
def captured_inputs() -> list[PipelineInput]:
    """Mutable list each test inspects to confirm what the pipeline saw."""
    return []


@pytest.fixture
def stub_pipeline_ok(monkeypatch: pytest.MonkeyPatch, captured_inputs: list[PipelineInput]):
    """Patch the pipeline `run` so it succeeds and records the call.

    The router calls ``run(inp=..., provider=..., settings=...)``; we
    discard the provider/settings (they're real singletons populated by
    factories that we don't want to invoke in a unit test) and append
    the PipelineInput so tests can assert on what was passed in.
    """

    def _fake_run(*, inp: PipelineInput, provider: Any, settings: Any) -> AIQueryResponse:
        captured_inputs.append(inp)
        return AIQueryResponse(
            question=inp.question,
            sql="SELECT 1 AS stub WHERE tenant_id = :tenant_id LIMIT 1",
            rows=[{"stub": 1}],
            row_count=1,
            columns=["stub"],
            summary="ok",
            chart_type="table",
            provider="stub",
            model="stub-1",
            elapsed_ms=1,
        )

    monkeypatch.setattr(ai_router_module, "run", _fake_run)
    # Also stub get_provider so the router doesn't try to import the real
    # OpenAI/Anthropic SDKs when no API key is set.
    monkeypatch.setattr(ai_router_module, "get_provider", lambda: object())
    return _fake_run


@pytest.fixture
def stub_pipeline_raises(monkeypatch: pytest.MonkeyPatch):
    """Factory: configure the patched `run` to raise a given PipelineError."""

    def _install(stage: str, detail: str = "boom", sql: str | None = None) -> None:
        def _raise(**_kw: Any) -> AIQueryResponse:
            raise PipelineError(stage, detail, sql=sql)  # type: ignore[arg-type]

        monkeypatch.setattr(ai_router_module, "run", _raise)
        monkeypatch.setattr(ai_router_module, "get_provider", lambda: object())

    return _install


def _override_principal(principal: Principal | None):
    """Install (or remove) a Principal override on the app."""
    if principal is None:
        app.dependency_overrides.pop(_require_principal, None)
    else:
        app.dependency_overrides[_require_principal] = lambda: principal


@pytest.fixture(autouse=True)
def _cleanup_overrides():
    """Clear any dependency override the test installed."""
    yield
    app.dependency_overrides.pop(_require_principal, None)


@pytest.fixture
def client() -> TestClient:
    # No `with` — startup events open the DB engine, which CI may not
    # have. TestClient still routes requests fine without lifespan.
    return TestClient(app)


# ---------------------------------------------------------------------------
# Tenant resolution
# ---------------------------------------------------------------------------


class TestTenantResolution:
    def test_tenant_user_pipeline_sees_jwt_tenant(
        self, client: TestClient, stub_pipeline_ok, captured_inputs: list[PipelineInput]
    ) -> None:
        """The pipeline must be invoked with the JWT's tenant, never the body's."""
        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )
        r = client.post("/v1/ai/query", json={"question": "how many trips last month?"})
        assert r.status_code == 200, r.text
        assert len(captured_inputs) == 1
        assert captured_inputs[0].tenant_id == 42

    def test_tenant_user_with_matching_body_tenant_passes(
        self, client: TestClient, stub_pipeline_ok, captured_inputs: list[PipelineInput]
    ) -> None:
        """A body tenant_id that matches the JWT is allowed (idempotent)."""
        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )
        r = client.post(
            "/v1/ai/query",
            json={"question": "how many trips last month?", "tenant_id": 42},
        )
        assert r.status_code == 200, r.text
        assert captured_inputs[0].tenant_id == 42

    def test_tenant_user_with_mismatched_body_tenant_returns_403(
        self, client: TestClient, stub_pipeline_ok
    ) -> None:
        """Silently overriding the body would hide a privilege-escalation attempt."""
        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )
        r = client.post(
            "/v1/ai/query",
            json={"question": "how many trips last month?", "tenant_id": 99},
        )
        assert r.status_code == 403, r.text
        body = r.json()
        assert body["detail"]["stage"] == "tenant_filter"

    def test_tenant_admin_same_behavior_as_tenant_user(
        self, client: TestClient, stub_pipeline_ok
    ) -> None:
        """tenant_admin and tenant_user share the same tenant-binding rules."""
        _override_principal(
            Principal(user_id=11, tenant_id=42, role="tenant_admin", email="a@t.local")
        )
        r = client.post(
            "/v1/ai/query",
            json={"question": "how many trips last month?", "tenant_id": 99},
        )
        assert r.status_code == 403

    def test_superadmin_without_body_tenant_returns_400(
        self, client: TestClient, stub_pipeline_ok
    ) -> None:
        """v1 has no cross-tenant mode — superadmin must pick a tenant."""
        _override_principal(
            Principal(user_id=1, tenant_id=None, role="superadmin", email="root@x")
        )
        r = client.post("/v1/ai/query", json={"question": "how many trips last month?"})
        assert r.status_code == 400, r.text
        assert r.json()["detail"]["stage"] == "tenant_filter"

    def test_superadmin_with_body_tenant_passes(
        self, client: TestClient, stub_pipeline_ok, captured_inputs: list[PipelineInput]
    ) -> None:
        _override_principal(
            Principal(user_id=1, tenant_id=None, role="superadmin", email="root@x")
        )
        r = client.post(
            "/v1/ai/query",
            json={"question": "how many trips last month?", "tenant_id": 7},
        )
        assert r.status_code == 200, r.text
        assert captured_inputs[0].tenant_id == 7


# ---------------------------------------------------------------------------
# Stage → HTTP mapping
# ---------------------------------------------------------------------------


class TestStageHttpMapping:
    def test_llm_failure_maps_to_502(self, client: TestClient, stub_pipeline_raises) -> None:
        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )
        stub_pipeline_raises("llm", "openai 5xx")
        r = client.post("/v1/ai/query", json={"question": "trips last month?"})
        assert r.status_code == 502, r.text
        body = r.json()["detail"]
        assert body["stage"] == "llm"
        assert "openai" in body["detail"]

    def test_sql_guard_failure_maps_to_400(
        self, client: TestClient, stub_pipeline_raises
    ) -> None:
        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )
        stub_pipeline_raises(
            "sql_guard",
            "missing `tenant_id = :tenant_id` predicate",
            sql="SELECT 1 FROM marts.v_executive_dashboard",
        )
        r = client.post("/v1/ai/query", json={"question": "anything"})
        assert r.status_code == 400, r.text
        body = r.json()["detail"]
        assert body["stage"] == "sql_guard"
        # The offending SQL is surfaced so the React client can show it.
        assert body["sql"].startswith("SELECT 1")

    def test_execution_failure_maps_to_500(
        self, client: TestClient, stub_pipeline_raises
    ) -> None:
        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )
        stub_pipeline_raises("execution", "statement timeout", sql="SELECT ...")
        r = client.post("/v1/ai/query", json={"question": "anything"})
        assert r.status_code == 500
        assert r.json()["detail"]["stage"] == "execution"

    def test_provider_config_failure_maps_to_500(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A missing-key / missing-SDK error surfaces as 500/config."""
        from app.ai.providers.base import LLMProviderError

        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )

        def _explode() -> Any:
            raise LLMProviderError("OPENAI_API_KEY is required")

        monkeypatch.setattr(ai_router_module, "get_provider", _explode)
        r = client.post("/v1/ai/query", json={"question": "anything"})
        assert r.status_code == 500, r.text
        body = r.json()["detail"]
        assert body["stage"] == "config"
        assert "OPENAI_API_KEY" in body["detail"]


# ---------------------------------------------------------------------------
# Request-body validation
# ---------------------------------------------------------------------------


class TestRequestValidation:
    def test_too_short_question_returns_422(
        self, client: TestClient, stub_pipeline_ok
    ) -> None:
        """Pydantic min_length=3 fires before we even reach the pipeline."""
        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )
        r = client.post("/v1/ai/query", json={"question": "hi"})
        assert r.status_code == 422

    def test_empty_body_returns_422(self, client: TestClient, stub_pipeline_ok) -> None:
        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )
        r = client.post("/v1/ai/query", json={})
        assert r.status_code == 422
