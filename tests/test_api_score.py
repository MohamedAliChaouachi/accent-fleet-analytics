"""
Tests for /score/risk and /score/cluster.

The risk endpoint is pure-Python — no external dependencies. It must agree
numerically with the underlying RiskScorer (which itself is pinned to the
SQL view by test_feature_computation.py).

The cluster endpoint requires a trained model. Without one, the API returns
503 with a helpful message — that contract is verified here.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from accent_fleet.features.risk_score import load_risk_scorer

from app.main import app


SAMPLE_FEATURES = {
    "overspeed_per_100km": 5.0,
    "overspeed_count": 100,
    "overspeed_severity_high": 20,
    "overspeed_severity_extreme": 5,
    "high_speed_trip_ratio": 0.15,
    "speed_alert_per_100km": 8.0,
    "night_trip_ratio": 0.20,
    "avg_max_speed_kmh": 130.0,
}


def test_score_risk_returns_expected_shape():
    with TestClient(app) as c:
        r = c.post("/score/risk", json=SAMPLE_FEATURES)
    assert r.status_code == 200
    body = r.json()
    assert {"risk_score", "category", "components", "version"} <= set(body)
    assert 0 <= body["risk_score"] <= 100
    assert body["category"] in {"low", "moderate", "high", "critical"}
    assert body["components"]  # non-empty per-factor breakdown


def test_score_risk_matches_python_reference():
    """The API total must equal the direct RiskScorer.score() result."""
    expected = load_risk_scorer().score(SAMPLE_FEATURES)
    with TestClient(app) as c:
        r = c.post("/score/risk", json=SAMPLE_FEATURES)
    assert r.status_code == 200
    assert r.json()["risk_score"] == pytest.approx(expected, abs=0.1)


def test_score_risk_handles_missing_fields():
    """Missing keys default to 0 (matches SQL view's COALESCE)."""
    with TestClient(app) as c:
        r = c.post("/score/risk", json={})
    assert r.status_code == 200
    body = r.json()
    assert body["risk_score"] == 0.0
    assert body["category"] == "low"


def test_score_risk_components_sum_to_total_within_rounding():
    """The per-factor components are the weighted contributions."""
    with TestClient(app) as c:
        r = c.post("/score/risk", json=SAMPLE_FEATURES)
    body = r.json()
    summed = sum(body["components"].values())
    # Each component is rounded to 2 dp and total to 1 dp; allow a small slack.
    assert summed == pytest.approx(body["risk_score"], abs=0.5)


def test_score_cluster_without_model_returns_503():
    """No MLflow + no joblib on disk -> 503 with actionable message."""
    with TestClient(app) as c:
        r = c.post("/score/cluster", json=SAMPLE_FEATURES)
    # 503 is the contract when no model is available yet.
    if r.status_code == 503:
        assert "train" in r.json()["detail"].lower()
    else:
        # If a model happens to be present (e.g. CI ran training first),
        # the endpoint should return a well-formed payload instead.
        assert r.status_code == 200
        body = r.json()
        assert "cluster_id" in body and "distance" in body
