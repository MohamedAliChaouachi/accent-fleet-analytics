"""
Tests for the /v1 versioning policy.

What we verify:
  1. Each business endpoint is reachable under BOTH /v1 and the legacy
     bare path. (Backwards compatibility — pre-v0.7 clients keep working.)
  2. Legacy responses carry Deprecation + Sunset + Link headers. (RFC 8594 /
     8288 conventions; lets ops grep `curl -I` output to see what's stale.)
  3. /v1 responses do NOT carry those headers. (Canonical mount.)
  4. Operational endpoints (/health, /ready, /metrics, /) are unversioned
     and never carry deprecation headers — those paths are infra contracts.
  5. The OpenAPI schema lists /v1/* paths only (legacy mount is hidden).

All cases run against TestClient. The /score/risk endpoint was a pure-Python
deterministic formula pre-v0.6; from v0.6 onward it requires a loaded
per-tenant Isolation Forest bundle and a tenant_id in the request body. We
override the predictor dependency with a small stub so the versioning tests
keep asserting the mount/header behaviour without depending on MLflow.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from accent_fleet.ml.inference import RiskPrediction, TenantModelMissingError
from app.deps import get_risk_predictor_dep
from app.main import app
from app.versioning import LEGACY_SUNSET_HTTP_DATE


@pytest.fixture(autouse=True)
def _advisory_auth(monkeypatch):
    """
    Pin AUTH_ENFORCEMENT=advisory so the suite exercises route logic rather
    than the auth middleware. Without this, an `.env` file with
    AUTH_ENFORCEMENT=enforce makes every request 401 before it ever reaches
    the /score handlers. Same workaround test_api_score.py uses.
    """
    from accent_fleet.config import settings as settings_fn

    monkeypatch.setenv("AUTH_ENFORCEMENT", "advisory")
    settings_fn.cache_clear()
    yield
    settings_fn.cache_clear()


# Same fixture as test_api_score; duplicated rather than imported because
# test files are otherwise standalone and pytest collection order can vary.
# Mirrors the 13-feature vector declared under
# config/feature_definitions.yaml::risk_score_model.features plus the
# tenant_id required by the v0.6 per-tenant routing.
SAMPLE_FEATURES = {
    "tenant_id": 235,
    "overspeed_per_100km": 5.0,
    "avg_speed_over_limit": 7.5,
    "high_speed_trip_ratio": 0.15,
    "speed_alert_per_100km": 8.0,
    "harsh_brake_per_100km": 1.2,
    "harsh_accel_per_100km": 0.8,
    "harsh_corner_per_100km": 0.3,
    "monthly_idle_ratio": 0.10,
    "high_rpm_minutes_per_day": 12.0,
    "night_trip_ratio": 0.20,
    "rush_hour_trip_ratio": 0.30,
    "stddev_trip_distance": 4.5,
    "short_trip_ratio": 0.18,
}


class _StubRiskPredictor:
    """
    Minimal RiskPredictor stand-in — see test_api_score._StubRiskPredictor
    for the canonical version. Duplicated here for the same standalone-file
    reason SAMPLE_FEATURES is duplicated.
    """
    model_version = "stub-1"

    def predict(self, tenant_id: int, features: dict) -> RiskPrediction:
        known = {235, 238}
        if tenant_id not in known:
            raise TenantModelMissingError(
                f"no model fitted for tenant {tenant_id}; known: {sorted(known)}"
            )
        return RiskPrediction(
            score=42.5,
            category="moderate",
            components={k: 0.0 for k in features if isinstance(features[k], (int, float))},
            model_version=self.model_version,
            model_source="stub",
        )


@pytest.fixture
def stub_predictor():
    """Wire a stub predictor into FastAPI's dependency graph for the test."""
    stub = _StubRiskPredictor()
    app.dependency_overrides[get_risk_predictor_dep] = lambda: stub
    try:
        yield stub
    finally:
        app.dependency_overrides.pop(get_risk_predictor_dep, None)


def test_v1_score_risk_works_without_deprecation_headers(stub_predictor):
    with TestClient(app) as c:
        r = c.post("/v1/score/risk", json=SAMPLE_FEATURES)
    assert r.status_code == 200, r.text
    # Canonical mount: no deprecation signaling.
    assert "deprecation" not in {k.lower() for k in r.headers}
    assert "sunset" not in {k.lower() for k in r.headers}


def test_legacy_score_risk_still_works_but_signals_deprecation(stub_predictor):
    with TestClient(app) as c:
        r = c.post("/score/risk", json=SAMPLE_FEATURES)
    # Body must be identical — same handler, just a different mount.
    assert r.status_code == 200, r.text
    # Deprecation headers per RFC 8594 / draft RFC 9745.
    assert r.headers.get("Deprecation") == "true"
    assert r.headers.get("Sunset") == LEGACY_SUNSET_HTTP_DATE
    # Link header points clients at the successor namespace.
    link = r.headers.get("Link", "")
    assert "/v1" in link and 'rel="successor-version"' in link


def test_health_is_unversioned_and_undeprecated():
    """Health/ready/metrics are operational endpoints — never versioned."""
    with TestClient(app) as c:
        # /v1/health must NOT exist (we deliberately didn't mount it there).
        r_v1 = c.get("/v1/health")
        r_legacy = c.get("/health")
    assert r_v1.status_code == 404
    assert r_legacy.status_code == 200
    assert "deprecation" not in {k.lower() for k in r_legacy.headers}


def test_metrics_endpoint_is_unversioned():
    with TestClient(app) as c:
        r = c.get("/metrics")
    assert r.status_code == 200
    # Prometheus text exposition format.
    assert r.headers["content-type"].startswith("text/plain")
    assert "deprecation" not in {k.lower() for k in r.headers}


def test_openapi_schema_lists_v1_paths_only():
    """
    Legacy routes are mounted with include_in_schema=False so the /docs
    page and downstream OpenAPI clients only see the canonical /v1/* paths.
    """
    with TestClient(app) as c:
        schema = c.get("/openapi.json").json()
    paths = set(schema["paths"].keys())
    # At least one v1 business path must be present.
    assert any(p.startswith("/v1/score") for p in paths), (
        f"expected /v1/score/* in OpenAPI schema, got: {sorted(paths)}"
    )
    # No bare /score/* business path should appear.
    business_legacy = {p for p in paths if p.startswith(("/score/", "/devices/", "/admin/"))}
    assert business_legacy == set(), (
        f"legacy business paths leaked into OpenAPI schema: {business_legacy}"
    )
    # Operational paths stay unversioned and visible.
    assert "/health" in paths
    assert "/ready" in paths


def test_root_advertises_v1_and_sunset():
    """The service-info endpoint helps humans find the current API version."""
    with TestClient(app) as c:
        r = c.get("/")
    body = r.json()
    assert body["api_version"] == "v1"
    assert "legacy_sunset" in body
