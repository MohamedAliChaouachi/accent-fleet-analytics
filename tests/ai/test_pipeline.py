"""
Pipeline integration test with a stub LLM.

Exercises the full request flow up to the DB call. The executor needs a
live Postgres, so the test is skipped unless one is reachable — same
pattern used by tests/test_api_dashboards.py.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from accent_fleet.db.engine import get_engine
from app.ai.config import AISettings
from app.ai.providers.base import BaseLLMProvider, LLMResponse
from app.ai.services.pipeline import PipelineError, PipelineInput, run


class _SQLOnlyStub(BaseLLMProvider):
    """Returns a hard-coded SQL string the test wants the guard to see."""

    name = "stub"
    model = "stub-1"

    def __init__(self, sql: str, summary: str = "stub summary") -> None:
        self._sql = sql
        self._summary = summary

    def generate_sql(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        return LLMResponse(text=self._sql, model=self.model)

    def summarize_result(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        return LLMResponse(text=self._summary, model=self.model)


@pytest.fixture(scope="module")
def db_available() -> bool:
    try:
        with get_engine().connect() as c:
            c.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def _settings() -> AISettings:
    # Plain instantiation — we don't want the env to leak into tests.
    return AISettings(
        AI_PROVIDER="stub",
        AI_MAX_ROWS=50,
        AI_STATEMENT_TIMEOUT_MS=2000,
        AI_LLM_TIMEOUT_S=5.0,
        AI_SUMMARY_SAMPLE_ROWS=5,
    )  # type: ignore[call-arg]


def test_pipeline_happy_path(db_available):
    if not db_available:
        pytest.skip("postgres not reachable")

    provider = _SQLOnlyStub(
        sql=(
            "SELECT year_month, total_trips FROM marts.v_executive_dashboard "
            "WHERE tenant_id = :tenant_id ORDER BY year_month DESC LIMIT 5"
        )
    )
    resp = run(
        inp=PipelineInput(question="trips per month", tenant_id=1),
        provider=provider,
        settings=_settings(),
    )
    assert resp.provider == "stub"
    assert "v_executive_dashboard" in resp.sql
    assert resp.row_count <= 5
    assert resp.chart_type in {"line", "bar", "table"}


def test_pipeline_rejects_unsafe_sql_with_stage_marker():
    """sql_guard failures must surface as PipelineError(stage='sql_guard')
    so the router can map to 400 instead of leaking a 500."""
    provider = _SQLOnlyStub(sql="DROP TABLE marts.v_executive_dashboard")
    with pytest.raises(PipelineError) as ei:
        run(
            inp=PipelineInput(question="x", tenant_id=1),
            provider=provider,
            settings=_settings(),
        )
    assert ei.value.stage == "sql_guard"


def test_pipeline_rejects_missing_tenant_filter():
    provider = _SQLOnlyStub(
        sql="SELECT year_month FROM marts.v_executive_dashboard LIMIT 5"
    )
    with pytest.raises(PipelineError) as ei:
        run(
            inp=PipelineInput(question="x", tenant_id=1),
            provider=provider,
            settings=_settings(),
        )
    assert ei.value.stage == "sql_guard"
