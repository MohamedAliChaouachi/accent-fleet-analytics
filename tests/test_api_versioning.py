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

All cases run against TestClient — no Postgres, no MLflow needed because
/score/risk is pure-Python.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app
from app.versioning import LEGACY_SUNSET_HTTP_DATE

# Same fixture as test_api_score; duplicated rather than imported because
# test files are otherwise standalone and pytest collection order can vary.
SAMPLE_FEATURES = {
    "overspeed_per_100km": 5.0,
    "overspeed_count": 100,
    "high_speed_trip_ratio": 0.15,
    "night_trip_ratio": 0.20,
    "avg_max_speed_kmh": 130.0,
}


def test_v1_score_risk_works_without_deprecation_headers():
    with TestClient(app) as c:
        r = c.post("/v1/score/risk", json=SAMPLE_FEATURES)
    assert r.status_code == 200
    # Canonical mount: no deprecation signaling.
    assert "deprecation" not in {k.lower() for k in r.headers}
    assert "sunset" not in {k.lower() for k in r.headers}


def test_legacy_score_risk_still_works_but_signals_deprecation():
    with TestClient(app) as c:
        r = c.post("/score/risk", json=SAMPLE_FEATURES)
    # Body must be identical — same handler, just a different mount.
    assert r.status_code == 200
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
