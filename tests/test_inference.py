"""
Tests for accent_fleet.ml.inference helpers (no DB, no MLflow).
"""

from __future__ import annotations

import pytest

from accent_fleet.features.risk_score import load_risk_scorer
from accent_fleet.ml.inference import (
    DEFAULT_MODEL_DIR,
    ClusterPredictor,
    get_risk_scorer,
    score_with_components,
)


SAMPLE = {
    "overspeed_per_100km": 4.0,
    "overspeed_count": 80,
    "overspeed_severity_high": 15,
    "overspeed_severity_extreme": 3,
    "high_speed_trip_ratio": 0.10,
    "speed_alert_per_100km": 6.0,
    "night_trip_ratio": 0.15,
    "avg_max_speed_kmh": 120.0,
}


def test_get_risk_scorer_returns_singleton():
    a = get_risk_scorer()
    b = get_risk_scorer()
    assert a is b


def test_score_with_components_total_matches_scorer_score():
    scorer = load_risk_scorer()
    expected = scorer.score(SAMPLE)
    total, components = score_with_components(scorer, SAMPLE)
    assert total == pytest.approx(expected, abs=0.1)
    # One component per configured factor.
    assert set(components) == {f.name for f in scorer.factors}


def test_score_with_components_zero_features_zero_score():
    scorer = load_risk_scorer()
    total, components = score_with_components(scorer, {})
    assert total == 0.0
    assert all(v == 0.0 for v in components.values())


def test_cluster_predictor_raises_when_no_model(monkeypatch):
    """
    Without an MLflow Production model AND no local joblib, predict() raises.

    We point the local-load path at a guaranteed-empty temp directory so a
    pre-existing models/clustering/ tree (created by an earlier train run)
    doesn't make the predictor succeed and flip this test red. We also pin
    MLFLOW_TRACKING_URI to an unreachable host so the registry path can't
    succeed either.
    """
    monkeypatch.setattr(
        "accent_fleet.ml.inference.DEFAULT_MODEL_DIR",
        DEFAULT_MODEL_DIR.parent / "__no_such_dir_for_test__",
    )
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:0")

    p = ClusterPredictor()
    # The predictor is intentionally lazy: instantiation never throws.
    assert p.is_loaded is False
    with pytest.raises(RuntimeError) as excinfo:
        p.predict(SAMPLE)
    assert "clustering model" in str(excinfo.value).lower()
