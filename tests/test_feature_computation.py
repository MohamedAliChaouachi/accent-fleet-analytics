"""
Unit tests for the feature registry and risk-score reference implementation.
"""

from __future__ import annotations

import pytest

from accent_fleet.features import load_feature_registry, load_risk_scorer


# ---------------------------------------------------------------------------
def test_registry_has_all_six_groups():
    reg = load_feature_registry()
    assert set(reg.groups()) == {
        "trip_volume_patterns",
        "speed_behavior",
        "overspeed_events",
        "alerts",
        "stops_idling",
        "temporal_patterns",
    }


def test_registry_count_matches_spec():
    """The v1 design advertises 35+ features. We expect at least 34."""
    reg = load_feature_registry()
    assert len(reg) >= 34


def test_known_features_present():
    reg = load_feature_registry()
    for name in [
        "total_trips",
        "overspeed_per_100km",
        "p95_max_speed",
        "night_trip_ratio",
    ]:
        assert name in reg


# ---------------------------------------------------------------------------
def test_risk_score_zero_for_safe_driver():
    """A driver with no overspeed/alerts/night driving should score near 0."""
    scorer = load_risk_scorer()
    features = {
        "overspeed_per_100km": 0.0,
        "overspeed_count": 0,
        "overspeed_severity_high": 0,
        "overspeed_severity_extreme": 0,
        "high_speed_trip_ratio": 0.0,
        "speed_alert_per_100km": 0.0,
        "night_trip_ratio": 0.0,
        "avg_max_speed_kmh": 40.0,    # not zero — avg speed still maps to non-zero
    }
    score = scorer.score(features)
    # max_speed factor gives 0.10 * (40/200) = 0.02 -> 2.0
    assert 0 <= score < 10
    assert scorer.categorize(score) == "low"


def test_risk_score_high_for_reckless_driver():
    """A driver pinned at the upper cap of every factor should hit 100."""
    scorer = load_risk_scorer()
    features = {
        "overspeed_per_100km": 100.0,             # far above cap
        "overspeed_count": 10,
        "overspeed_severity_high": 5,
        "overspeed_severity_extreme": 5,
        "high_speed_trip_ratio": 1.0,
        "speed_alert_per_100km": 100.0,
        "night_trip_ratio": 1.0,
        "avg_max_speed_kmh": 200.0,
    }
    score = scorer.score(features)
    assert score >= 90
    assert scorer.categorize(score) in {"high", "critical"}


def test_risk_category_bands():
    scorer = load_risk_scorer()
    assert scorer.categorize(0) == "low"
    assert scorer.categorize(19) == "low"
    assert scorer.categorize(20) == "moderate"
    assert scorer.categorize(44) == "moderate"
    assert scorer.categorize(45) == "high"
    assert scorer.categorize(69) == "high"
    assert scorer.categorize(70) == "critical"
    assert scorer.categorize(100) == "critical"


@pytest.mark.parametrize("missing_feature", [
    "overspeed_per_100km",
    "high_speed_trip_ratio",
    "speed_alert_per_100km",
])
def test_risk_score_handles_missing_features(missing_feature):
    """Missing feature keys must not raise — they are treated as 0."""
    scorer = load_risk_scorer()
    features = {
        "overspeed_per_100km": 5.0,
        "overspeed_count": 3,
        "overspeed_severity_high": 1,
        "overspeed_severity_extreme": 0,
        "high_speed_trip_ratio": 0.2,
        "speed_alert_per_100km": 10.0,
        "night_trip_ratio": 0.15,
        "avg_max_speed_kmh": 120.0,
    }
    features.pop(missing_feature, None)
    score = scorer.score(features)
    assert 0 <= score <= 100
