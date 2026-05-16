"""
Rate-limit tests for /v1/ai/query.

Two layers:
  * **Unit**: the limiter itself — admission, two-scope rejection,
    bucket independence, recovery after the window slides.
  * **Router**: a throttled request returns 429 with Retry-After and
    writes a ``rate_limited`` audit row.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.ai.routers import ai_query as ai_router_module
from app.ai.schemas.ai import AIQueryResponse
from app.ai.services import rate_limit as rate_limit_module
from app.ai.services.pipeline import PipelineInput
from app.ai.services.rate_limit import (
    AIRateLimiter,
    AIRateLimitExceededError,
)
from app.auth.deps import _require_principal
from app.auth.principal import Principal
from app.main import app

# ---------------------------------------------------------------------------
# Unit: the limiter
# ---------------------------------------------------------------------------


class TestLimiterUnit:
    def test_admits_within_user_cap(self) -> None:
        lim = AIRateLimiter(per_user_max=3, per_tenant_max=100, window_seconds=60)
        for _ in range(3):
            lim.check(user_id=1, tenant_id=42)
        # 4th attempt is over the cap.
        with pytest.raises(AIRateLimitExceededError) as ei:
            lim.check(user_id=1, tenant_id=42)
        assert ei.value.scope == "user"
        assert ei.value.retry_after_seconds >= 1

    def test_independent_users_dont_share_a_bucket(self) -> None:
        lim = AIRateLimiter(per_user_max=2, per_tenant_max=100, window_seconds=60)
        lim.check(user_id=1, tenant_id=42)
        lim.check(user_id=1, tenant_id=42)
        # user 2 still has full budget.
        lim.check(user_id=2, tenant_id=42)
        # user 1 over.
        with pytest.raises(AIRateLimitExceededError):
            lim.check(user_id=1, tenant_id=42)

    def test_tenant_cap_fires_before_user_cap(self) -> None:
        """Three users in tenant 42 each within personal cap, but
        together exhaust the tenant cap."""
        lim = AIRateLimiter(per_user_max=10, per_tenant_max=3, window_seconds=60)
        lim.check(user_id=1, tenant_id=42)
        lim.check(user_id=2, tenant_id=42)
        lim.check(user_id=3, tenant_id=42)
        # Next attempt from any user in tenant 42 is throttled.
        with pytest.raises(AIRateLimitExceededError) as ei:
            lim.check(user_id=4, tenant_id=42)
        assert ei.value.scope == "tenant"

    def test_other_tenants_unaffected_by_tenant_throttle(self) -> None:
        lim = AIRateLimiter(per_user_max=10, per_tenant_max=2, window_seconds=60)
        lim.check(user_id=1, tenant_id=42)
        lim.check(user_id=2, tenant_id=42)
        # tenant 42 full…
        with pytest.raises(AIRateLimitExceededError):
            lim.check(user_id=3, tenant_id=42)
        # …but tenant 99 has its own bucket.
        lim.check(user_id=3, tenant_id=99)

    def test_superadmin_request_skips_tenant_bucket(self) -> None:
        """Superadmin requests pass ``tenant_id=None`` to the limiter
        and are charged to user bucket only — they don't poison the
        tenant bucket of whichever tenant they happened to query."""
        lim = AIRateLimiter(per_user_max=10, per_tenant_max=2, window_seconds=60)
        lim.check(user_id=1, tenant_id=None)
        lim.check(user_id=1, tenant_id=None)
        # Tenant 42 has not been touched.
        lim.check(user_id=2, tenant_id=42)
        lim.check(user_id=2, tenant_id=42)
        # Tenant 42 is now at the cap — next attempt by anyone with
        # tenant_id=42 is throttled.
        with pytest.raises(AIRateLimitExceededError):
            lim.check(user_id=3, tenant_id=42)

    def test_window_expiry_restores_budget(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Bucket entries older than the window are dropped on the next check."""
        # Drive `time.monotonic` so the test is deterministic.
        clock = [1000.0]

        def fake_monotonic() -> float:
            return clock[0]

        monkeypatch.setattr(rate_limit_module.time, "monotonic", fake_monotonic)

        lim = AIRateLimiter(per_user_max=2, per_tenant_max=100, window_seconds=60)
        lim.check(user_id=1, tenant_id=42)
        lim.check(user_id=1, tenant_id=42)
        with pytest.raises(AIRateLimitExceededError):
            lim.check(user_id=1, tenant_id=42)
        # Advance past the window — old attempts age out, budget restored.
        clock[0] += 61
        lim.check(user_id=1, tenant_id=42)


# ---------------------------------------------------------------------------
# Router integration: 429 + audit row + Retry-After header
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _advisory_auth(monkeypatch: pytest.MonkeyPatch):
    from accent_fleet.config import settings as settings_fn

    monkeypatch.setenv("AUTH_ENFORCEMENT", "advisory")
    settings_fn.cache_clear()
    yield
    settings_fn.cache_clear()


@pytest.fixture(autouse=True)
def _cleanup():
    """Drop any dep overrides + the limiter singleton between tests."""
    yield
    app.dependency_overrides.pop(_require_principal, None)
    rate_limit_module.reset_ai_rate_limiter()


@pytest.fixture(autouse=True)
def _tight_limits(monkeypatch: pytest.MonkeyPatch):
    """Tighten the env-driven caps so the test runs in milliseconds.

    The limiter reads settings only when :func:`get_ai_rate_limiter`
    builds the singleton, so we reset that too on teardown (see
    ``_cleanup`` above).
    """
    from app.ai.config import ai_settings

    monkeypatch.setenv("AI_RATE_LIMIT_USER_MAX", "2")
    monkeypatch.setenv("AI_RATE_LIMIT_TENANT_MAX", "10")
    monkeypatch.setenv("AI_RATE_LIMIT_WINDOW_S", "60")
    ai_settings.cache_clear()
    rate_limit_module.reset_ai_rate_limiter()
    yield
    ai_settings.cache_clear()


@pytest.fixture
def stub_pipeline_ok(monkeypatch: pytest.MonkeyPatch):
    """A successful pipeline so admitted requests return 200."""

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
            elapsed_ms=1,
        )

    monkeypatch.setattr(ai_router_module, "run", _fake_run)
    monkeypatch.setattr(ai_router_module, "get_provider", lambda: object())


@pytest.fixture
def captured_audit(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    monkeypatch.setattr(
        ai_router_module, "write_ai_query_event", lambda **kw: rows.append(kw)
    )
    return rows


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


class TestRouterRateLimit:
    def _principal(self) -> Principal:
        return Principal(user_id=10, tenant_id=42, role="tenant_user", email="u@t.local")

    def test_429_after_user_cap(
        self, client: TestClient, stub_pipeline_ok, captured_audit
    ) -> None:
        app.dependency_overrides[_require_principal] = lambda: self._principal()
        body = {"question": "how many trips last month?"}

        # First two within cap.
        assert client.post("/v1/ai/query", json=body).status_code == 200
        assert client.post("/v1/ai/query", json=body).status_code == 200
        # Third over the per-user cap → 429.
        r = client.post("/v1/ai/query", json=body)
        assert r.status_code == 429, r.text
        # Retry-After must be a positive integer.
        retry_after = int(r.headers["Retry-After"])
        assert retry_after >= 1

        # Audit log: two successes + one rate_limited row.
        stages = [row["stage"] for row in captured_audit]
        assert stages == ["success", "success", "rate_limited"]
        # The throttle audit row records the bucket scope.
        assert "user" in captured_audit[-1]["error_detail"]

    def test_429_response_body_mentions_retry(
        self, client: TestClient, stub_pipeline_ok, captured_audit
    ) -> None:
        app.dependency_overrides[_require_principal] = lambda: self._principal()
        body = {"question": "anything"}
        client.post("/v1/ai/query", json=body)
        client.post("/v1/ai/query", json=body)
        r = client.post("/v1/ai/query", json=body)
        assert r.status_code == 429
        # Body is a string detail (the limit is not a "stage" of the pipeline).
        assert "rate limit" in r.json()["detail"].lower()
