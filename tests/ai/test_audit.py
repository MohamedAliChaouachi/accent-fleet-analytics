"""
Audit-log tests for /v1/ai/query.

Two layers covered here:

  1. The router writes exactly one audit row per request — success or
     failure — with the right fields populated for each stage.

  2. The writer itself is fail-open: a DB error during the INSERT logs a
     warning and does NOT propagate to the caller. (We don't have a real
     ``ai.query_log`` table at unit-test time, so this is the property
     we lean on so the suite stays DB-less.)
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.ai.routers import ai_query as ai_router_module
from app.ai.schemas.ai import AIQueryResponse
from app.ai.services import audit as audit_module
from app.ai.services.pipeline import PipelineError, PipelineInput
from app.auth.deps import _require_principal
from app.auth.principal import Principal
from app.main import app

# ---------------------------------------------------------------------------
# Fixtures (mirrored from test_router.py — kept local for clarity)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _advisory_auth(monkeypatch: pytest.MonkeyPatch):
    """See test_router.py for the rationale."""
    from accent_fleet.config import settings as settings_fn

    monkeypatch.setenv("AUTH_ENFORCEMENT", "advisory")
    settings_fn.cache_clear()
    yield
    settings_fn.cache_clear()


@pytest.fixture(autouse=True)
def _cleanup_overrides():
    yield
    app.dependency_overrides.pop(_require_principal, None)


def _override_principal(principal: Principal) -> None:
    app.dependency_overrides[_require_principal] = lambda: principal


@pytest.fixture
def captured_audit(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Capture every audit row instead of hitting Postgres.

    The router imports ``write_ai_query_event`` from
    ``app.ai.services.audit``, so we patch the symbol on the **router
    module** — patching the original attribute on the audit module
    wouldn't affect the already-bound import.
    """
    rows: list[dict[str, Any]] = []

    def _fake(**kw: Any) -> None:
        rows.append(kw)

    monkeypatch.setattr(ai_router_module, "write_ai_query_event", _fake)
    return rows


@pytest.fixture
def stub_pipeline_ok(monkeypatch: pytest.MonkeyPatch):
    def _fake_run(*, inp: PipelineInput, provider: Any, settings: Any) -> AIQueryResponse:
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
            elapsed_ms=42,
        )

    monkeypatch.setattr(ai_router_module, "run", _fake_run)
    monkeypatch.setattr(ai_router_module, "get_provider", lambda: object())


@pytest.fixture
def stub_pipeline_raises(monkeypatch: pytest.MonkeyPatch):
    def _install(stage: str, detail: str = "boom", sql: str | None = None) -> None:
        def _raise(**_kw: Any) -> AIQueryResponse:
            raise PipelineError(stage, detail, sql=sql)  # type: ignore[arg-type]

        monkeypatch.setattr(ai_router_module, "run", _raise)
        monkeypatch.setattr(ai_router_module, "get_provider", lambda: object())

    return _install


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


# ---------------------------------------------------------------------------
# Router-level audit emission
# ---------------------------------------------------------------------------


class TestRouterAudit:
    def test_success_writes_one_success_row(
        self, client: TestClient, stub_pipeline_ok, captured_audit: list[dict[str, Any]]
    ) -> None:
        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )
        r = client.post("/v1/ai/query", json={"question": "how many trips last month?"})
        assert r.status_code == 200

        assert len(captured_audit) == 1
        row = captured_audit[0]
        assert row["stage"] == "success"
        assert row["user_id"] == 10
        assert row["tenant_id"] == 42
        assert row["question"] == "how many trips last month?"
        assert row["sql_text"].startswith("SELECT 1")
        assert row["row_count"] == 1
        assert row["chart_type"] == "table"
        assert row["provider"] == "stub"
        assert row["model"] == "stub-1"
        assert row["elapsed_ms"] == 42
        # Success rows never carry an error.
        assert row.get("error_detail") is None

    def test_sql_guard_failure_writes_guard_row_with_sql(
        self,
        client: TestClient,
        stub_pipeline_raises,
        captured_audit: list[dict[str, Any]],
    ) -> None:
        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )
        stub_pipeline_raises(
            "sql_guard",
            "missing tenant predicate",
            sql="SELECT 1 FROM marts.v_executive_dashboard",
        )
        r = client.post("/v1/ai/query", json={"question": "what?"})
        assert r.status_code == 400

        assert len(captured_audit) == 1
        row = captured_audit[0]
        assert row["stage"] == "sql_guard"
        assert row["sql_text"] == "SELECT 1 FROM marts.v_executive_dashboard"
        assert row["error_detail"] == "missing tenant predicate"
        # Failure rows have no execution metadata.
        assert row.get("row_count") is None
        assert row.get("elapsed_ms") is None

    def test_llm_failure_writes_llm_row_without_sql(
        self,
        client: TestClient,
        stub_pipeline_raises,
        captured_audit: list[dict[str, Any]],
    ) -> None:
        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )
        stub_pipeline_raises("llm", "upstream 5xx")
        r = client.post("/v1/ai/query", json={"question": "trips last month?"})
        assert r.status_code == 502

        assert len(captured_audit) == 1
        row = captured_audit[0]
        assert row["stage"] == "llm"
        # The LLM never produced anything, so the row has no SQL.
        assert row.get("sql_text") is None
        assert row["error_detail"] == "upstream 5xx"

    def test_config_failure_writes_config_row(
        self,
        client: TestClient,
        monkeypatch: pytest.MonkeyPatch,
        captured_audit: list[dict[str, Any]],
    ) -> None:
        """Provider-construction failure (missing API key) is audited too."""
        from app.ai.providers.base import LLMProviderError

        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )

        def _explode() -> Any:
            raise LLMProviderError("OPENAI_API_KEY is required")

        monkeypatch.setattr(ai_router_module, "get_provider", _explode)
        r = client.post("/v1/ai/query", json={"question": "anything"})
        assert r.status_code == 500
        assert captured_audit[0]["stage"] == "config"
        assert "OPENAI_API_KEY" in captured_audit[0]["error_detail"]

    def test_tenant_filter_rejection_does_not_emit_audit_row(
        self,
        client: TestClient,
        stub_pipeline_ok,
        captured_audit: list[dict[str, Any]],
    ) -> None:
        """The 403 fires INSIDE ``_resolve_tenant`` before any pipeline /
        audit code runs. That's by design — a mismatched body is a request
        error, not an AI event. The router-level access log already records
        the 403 with the principal."""
        _override_principal(
            Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")
        )
        r = client.post(
            "/v1/ai/query",
            json={"question": "what?", "tenant_id": 99},
        )
        assert r.status_code == 403
        assert captured_audit == []


# ---------------------------------------------------------------------------
# Writer fail-open property
# ---------------------------------------------------------------------------


class TestWriterFailOpen:
    def test_writer_swallows_db_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A blown-up engine MUST NOT propagate to the caller."""

        class _ExplodingEngine:
            def begin(self) -> Any:  # noqa: ANN401
                raise RuntimeError("connection refused")

        monkeypatch.setattr(audit_module, "get_engine", lambda: _ExplodingEngine())
        # Should return cleanly, not raise.
        audit_module.write_ai_query_event(
            user_id=1,
            tenant_id=42,
            question="anything",
            stage="success",
        )

    def test_writer_truncates_oversized_question(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Belt-and-braces against a future schema change relaxing the
        pydantic ``max_length=2000``. The writer caps at 8000 chars before
        the INSERT; if it didn't, a malicious 1 MB question would land
        verbatim in ai.query_log."""
        seen: dict[str, Any] = {}

        class _CaptureConn:
            def __enter__(self) -> _CaptureConn:
                return self

            def __exit__(self, *exc: Any) -> None:
                return None

            def execute(self, _stmt: Any, params: dict[str, Any]) -> None:
                seen.update(params)

        class _CaptureEngine:
            def begin(self) -> _CaptureConn:
                return _CaptureConn()

        monkeypatch.setattr(audit_module, "get_engine", lambda: _CaptureEngine())
        audit_module.write_ai_query_event(
            user_id=1,
            tenant_id=42,
            question="x" * 50_000,
            stage="success",
        )
        assert len(seen["question"]) == 8000
