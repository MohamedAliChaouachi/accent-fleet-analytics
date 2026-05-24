"""
Tests for /v1/score/risk and /v1/score/cluster.

The risk endpoint is backed by a per-tenant Isolation Forest bundle loaded
either from MLflow or from a local joblib fallback. In a unit-test
environment neither source exists, so we override the predictor dependency
with a small stub that exercises the route's contract surface:

  - structurally valid request without tenant_id  → 422
  - request for a tenant the stub doesn't know     → 503 (TenantModelMissingError)
  - happy-path request                             → 200 with expected shape
  - dependency raising RuntimeError ("no model")   → 503

The cluster endpoint is similarly stubbed or — when no model is loaded —
returns the documented 503 with an actionable message.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from accent_fleet.ml.inference import RiskPrediction, TenantModelMissingError
from app.deps import get_risk_predictor_dep
from app.main import app


@pytest.fixture(autouse=True)
def _advisory_auth(monkeypatch):
    """
    Pin AUTH_ENFORCEMENT=advisory so the suite exercises route logic
    rather than the auth middleware (which has its own tests in
    test_auth_middleware.py). Without this, an `.env` file with
    `AUTH_ENFORCEMENT=enforce` makes every request 401 before it ever
    reaches the /score handlers. Same gotcha test_api_dashboards.py
    works around.
    """
    from accent_fleet.config import settings as settings_fn

    monkeypatch.setenv("AUTH_ENFORCEMENT", "advisory")
    settings_fn.cache_clear()
    yield
    settings_fn.cache_clear()


# A complete feature vector matching the 13 features declared under
# config/feature_definitions.yaml::risk_score_model.features. The route
# doesn't validate values numerically (predictor handles missing keys as
# zeros) but a realistic vector keeps the test honest.
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


# ---------------------------------------------------------------------------
# Stub predictor — minimal surface that mirrors RiskPredictor for the route's
# perspective. We intentionally don't subclass the real class: the test must
# pass even if RiskPredictor adds methods later that the stub doesn't.
# ---------------------------------------------------------------------------
class _StubRiskPredictor:
    """In-memory RiskPredictor stand-in. Tenants 235 and 238 are 'known'."""
    model_version = "stub-1"

    def predict(self, tenant_id: int, features: dict) -> RiskPrediction:
        known = {235, 238}
        if tenant_id not in known:
            raise TenantModelMissingError(
                f"no model fitted for tenant {tenant_id}; known: {sorted(known)}"
            )
        # Deterministic toy score so the happy-path test can assert shape
        # without depending on any sklearn output.
        return RiskPrediction(
            score=42.5,
            category="moderate",
            components={k: 0.0 for k in features if isinstance(features[k], (int, float))},
            model_version=self.model_version,
            model_source="stub",
        )


@pytest.fixture
def stub_predictor():
    """
    Yield a stub predictor wired into the FastAPI dependency graph.

    Cleans up after the test so unrelated test files keep seeing the real
    (unloaded) predictor that the lifespan handler put on app.state.
    """
    stub = _StubRiskPredictor()
    app.dependency_overrides[get_risk_predictor_dep] = lambda: stub
    try:
        yield stub
    finally:
        app.dependency_overrides.pop(get_risk_predictor_dep, None)


# ---------------------------------------------------------------------------
# /v1/score/risk
# ---------------------------------------------------------------------------
def test_score_risk_happy_path(stub_predictor):
    with TestClient(app) as c:
        r = c.post("/score/risk", json=SAMPLE_FEATURES)
    assert r.status_code == 200
    body = r.json()
    assert {"risk_score", "category", "components", "model_version", "version"} <= set(body)
    assert 0 <= body["risk_score"] <= 100
    assert body["category"] in {"low", "moderate", "high", "critical"}
    assert body["model_version"] == "stub-1"
    # The stub's per-feature components dict was built from SAMPLE_FEATURES;
    # at minimum it must be non-empty (the API contract) and have the right
    # shape (str -> float).
    assert isinstance(body["components"], dict) and body["components"]
    for k, v in body["components"].items():
        assert isinstance(k, str)
        assert isinstance(v, (int, float))


def test_score_risk_missing_tenant_id_returns_422(stub_predictor):
    """
    A request without tenant_id is structurally valid JSON but semantically
    incomplete. The route raises 422 — same status pydantic uses for missing
    fields, so clients can treat both cases uniformly.
    """
    payload = {k: v for k, v in SAMPLE_FEATURES.items() if k != "tenant_id"}
    with TestClient(app) as c:
        r = c.post("/score/risk", json=payload)
    assert r.status_code == 422
    assert "tenant_id" in r.json()["detail"].lower()


def test_score_risk_unknown_tenant_returns_503(stub_predictor):
    """
    The stub raises TenantModelMissingError for tenant_id=999. The route translates
    that to 503 because the API itself is healthy; the model just hasn't been
    retrained to include this tenant yet. Dashboards can show a "feature not
    ready for this tenant" message instead of a 5xx alarm.
    """
    payload = dict(SAMPLE_FEATURES, tenant_id=999)
    with TestClient(app) as c:
        r = c.post("/score/risk", json=payload)
    assert r.status_code == 503
    detail = r.json()["detail"]
    assert "999" in detail or "tenant" in detail.lower()


def test_score_risk_no_model_loaded_returns_503():
    """
    Without the stub override, the real (lifespan-constructed but unloaded)
    RiskPredictor raises RuntimeError on first .predict() call. The route
    must translate that to 503 with an actionable message — distinct from
    422 (bad input) so clients can retry once a model lands.

    We don't install the `stub_predictor` fixture here, so the real
    dependency runs end-to-end. If the test environment happens to have a
    Production model registered (e.g. CI ran training first), we accept
    200 as well: the test's job is to assert "503 OR 200", never 5xx.
    """
    with TestClient(app) as c:
        r = c.post("/score/risk", json=SAMPLE_FEATURES)
    assert r.status_code in (200, 503), r.text
    if r.status_code == 503:
        detail = r.json()["detail"].lower()
        # We don't pin a specific phrase — the predictor's RuntimeError
        # message has evolved over versions. Just sanity-check it's an
        # error string with at least one of the canonical keywords.
        assert any(w in detail for w in ("model", "load", "train", "mlflow", "local"))


# ---------------------------------------------------------------------------
# /v1/score/cluster — unchanged contract; the v0.6 migration only touched
# the risk pathway. Keeping this test guards against accidental regressions.
# ---------------------------------------------------------------------------
def test_score_cluster_without_model_returns_503():
    """No MLflow + no joblib on disk → 503 with actionable message."""
    with TestClient(app) as c:
        # tenant_id is ignored on /score/cluster but pass it anyway so the
        # same payload works for both endpoints during smoke tests.
        r = c.post("/score/cluster", json=SAMPLE_FEATURES)
    if r.status_code == 503:
        assert "train" in r.json()["detail"].lower() or "model" in r.json()["detail"].lower()
    else:
        # CI may have a model loaded; accept either outcome but assert
        # the success-path shape.
        assert r.status_code == 200
        body = r.json()
        assert "cluster_id" in body and "distance" in body
