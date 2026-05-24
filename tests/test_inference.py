"""
Tests for accent_fleet.ml.inference (no DB, no MLflow, no sklearn fit).

Two predictors live in that module:
  - RiskPredictor — bundled per-tenant Isolation Forest artifact
  - ClusterPredictor — KMeans + StandardScaler pipeline

For unit-test isolation we never load real models. We either:
  (a) inject a synthetic in-memory artifact and assert the predict path
      uses the stub's outputs verbatim, or
  (b) starve the loader of both MLflow and local-disk paths so the
      defined RuntimeError message surfaces.

Pure helpers (``_rescale_one``, ``_rescale_array``, ``_categorize_one``,
``_categorize_array``) are exercised directly — they hold the
training-time vs. inference-time math contract and are the cheapest
regression surface to guard.
"""

from __future__ import annotations

import numpy as np
import pytest

from accent_fleet.ml.inference import (
    CLUSTERING_MODEL_DIR,
    RISK_MODEL_DIR,
    ClusterPredictor,
    RiskPredictor,
    TenantModelMissingError,
    _categorize_array,
    _categorize_one,
    _rescale_array,
    _rescale_one,
)


# ---------------------------------------------------------------------------
# Pure helpers — exercised first because everything below relies on this math.
# ---------------------------------------------------------------------------
def test_rescale_one_maps_min_to_zero_and_max_to_one_hundred():
    assert _rescale_one(0.0, 0.0, 1.0) == pytest.approx(0.0)
    assert _rescale_one(1.0, 0.0, 1.0) == pytest.approx(100.0)
    assert _rescale_one(0.5, 0.0, 1.0) == pytest.approx(50.0)


def test_rescale_one_clips_below_zero_and_above_one_hundred():
    """Out-of-band raw values clip to the 0..100 band, not silently extrapolate."""
    assert _rescale_one(-5.0, 0.0, 1.0) == pytest.approx(0.0)
    assert _rescale_one(99.0, 0.0, 1.0) == pytest.approx(100.0)


def test_rescale_one_handles_degenerate_span():
    """When raw_max == raw_min the epsilon floor avoids divide-by-zero."""
    # raw == raw_min → after epsilon-floor, (raw - raw_min) / eps == 0,
    # which clips to 0.0 * 100 = 0.0. Anything > raw_min explodes upward
    # and clips at 100, by design — degenerate-tenant case.
    assert _rescale_one(5.0, 5.0, 5.0) == pytest.approx(0.0)
    assert _rescale_one(5.000001, 5.0, 5.0) == pytest.approx(100.0)


def test_rescale_array_vectorises_the_scalar_contract():
    raw = np.array([-1.0, 0.0, 0.25, 0.5, 1.0, 2.0])
    out = _rescale_array(raw, 0.0, 1.0)
    np.testing.assert_allclose(out, [0.0, 0.0, 25.0, 50.0, 100.0, 100.0])


def test_categorize_one_uses_strict_band_boundaries():
    thresholds = {"moderate": 25.0, "high": 50.0, "critical": 90.0}
    # Below 25 is low.
    assert _categorize_one(0.0, thresholds) == "low"
    assert _categorize_one(24.99, thresholds) == "low"
    # 25 inclusive is moderate.
    assert _categorize_one(25.0, thresholds) == "moderate"
    assert _categorize_one(49.99, thresholds) == "moderate"
    # 50 inclusive is high.
    assert _categorize_one(50.0, thresholds) == "high"
    assert _categorize_one(89.99, thresholds) == "high"
    # 90 inclusive is critical (the >= comparison).
    assert _categorize_one(90.0, thresholds) == "critical"
    assert _categorize_one(100.0, thresholds) == "critical"


def test_categorize_array_matches_scalar_function():
    thresholds = {"moderate": 25.0, "high": 50.0, "critical": 90.0}
    scores = np.array([0.0, 24.99, 25.0, 49.99, 50.0, 89.99, 90.0, 100.0])
    out = _categorize_array(scores, thresholds)
    expected = [_categorize_one(float(s), thresholds) for s in scores]
    assert list(out) == expected


# ---------------------------------------------------------------------------
# Synthetic per-tenant artifact — duck-typed scaler + IF stand-ins.
# ---------------------------------------------------------------------------
class _ZeroScaler:
    """Returns the input as-is. Mimics a fitted StandardScaler.transform."""

    def transform(self, X):  # noqa: N803
        return np.asarray(X, dtype=float)


class _SumIF:
    """
    Stand-in for sklearn IsolationForest with a deterministic anomaly score.

    ``decision_function`` returns the negated row-sum, so rows with bigger
    feature values get more anomalous scores after the predictor's
    ``raw = -decision_function`` flip. That's the contract the real IF
    obeys (more anomalous → smaller decision_function → larger raw).
    """

    def decision_function(self, X):  # noqa: N803
        return -np.asarray(X, dtype=float).sum(axis=1)


def _build_synthetic_artifact(
    feature_order: list[str],
    tenants: list[int],
    raw_min: float = 0.0,
    raw_max: float = 10.0,
) -> dict:
    """Build the dict shape RiskPredictor expects from MLflow / joblib."""
    thresholds = {"moderate": 25.0, "high": 50.0, "critical": 90.0}
    return {
        "feature_order": list(feature_order),
        "tenants": {
            tid: {
                "scaler": _ZeroScaler(),
                "model": _SumIF(),
                "raw_min": raw_min,
                "raw_max": raw_max,
                "thresholds": thresholds,
                "feature_means": [0.0] * len(feature_order),
                "feature_stds": [1.0] * len(feature_order),
                "n_rows": 100,
                "score_share": {
                    "low": 0.6, "moderate": 0.25, "high": 0.10, "critical": 0.05,
                },
            }
            for tid in tenants
        },
    }


def _inject_artifact(p: RiskPredictor, artifact: dict, version: str = "synthetic-1") -> None:
    """Bypass MLflow / disk loading for unit tests."""
    p._artifact = artifact
    p._feature_order = list(artifact["feature_order"])
    p._model_version = version
    p._source = "test"


# ---------------------------------------------------------------------------
# RiskPredictor — lazy-load contract
# ---------------------------------------------------------------------------
def test_risk_predictor_starts_unloaded():
    """Instantiation is cheap and never touches MLflow or disk."""
    p = RiskPredictor()
    assert p.is_loaded is False
    assert p.model_version == "unloaded"
    assert p.source == "none"
    assert p.tenants == []


def test_risk_predictor_raises_when_no_model_anywhere(monkeypatch):
    """
    Without an MLflow Production model AND no local joblib, predict() raises
    RuntimeError with the operator-friendly "train one with ..." message.

    We point the local-load directory at a guaranteed-empty path so a
    previously-trained models/risk_score/ tree doesn't make this pass
    accidentally. We also pin MLFLOW_TRACKING_URI to an unreachable URL so
    the registry path can't succeed either.
    """
    monkeypatch.setattr(
        "accent_fleet.ml.inference.RISK_MODEL_DIR",
        RISK_MODEL_DIR.parent / "__no_such_dir_for_risk_test__",
    )
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:0")

    p = RiskPredictor()
    assert p.is_loaded is False
    with pytest.raises(RuntimeError) as excinfo:
        p.predict(tenant_id=235, features={})
    msg = str(excinfo.value).lower()
    assert "risk" in msg or "model" in msg
    # The error should hint at the next operator action — either training
    # or registering. We don't pin the exact phrase.
    assert any(w in msg for w in ("train", "register", "mlflow"))


# ---------------------------------------------------------------------------
# RiskPredictor — happy-path with an injected synthetic artifact
# ---------------------------------------------------------------------------
def test_risk_predictor_predict_returns_well_formed_prediction():
    p = RiskPredictor()
    features = ["overspeed_per_100km", "harsh_brake_per_100km", "night_trip_ratio"]
    _inject_artifact(p, _build_synthetic_artifact(features, tenants=[235]))

    # Sum of [0, 0, 0] = 0; raw = -0 = 0 → midway between raw_min(0) and
    # raw_max(10) is below moderate(25) → category low, score 0.
    pred = p.predict(tenant_id=235, features=dict.fromkeys(features, 0.0))
    assert pred.score == pytest.approx(0.0)
    assert pred.category == "low"
    assert pred.model_version == "synthetic-1"
    assert pred.model_source == "test"
    # Components are post-scaler z-scores keyed by feature name. With the
    # identity scaler, components == input feature values.
    assert set(pred.components) == set(features)


def test_risk_predictor_predict_clips_extreme_values_to_one_hundred():
    """Anything above raw_max clips to score=100 → critical band."""
    p = RiskPredictor()
    features = ["a", "b"]
    _inject_artifact(
        p,
        _build_synthetic_artifact(features, tenants=[235], raw_min=0.0, raw_max=1.0),
    )
    # Sum = 100 → raw = -100? No: decision_function returns -sum,
    # then predictor does `raw = -decision_function` = sum. So raw = 100.
    pred = p.predict(tenant_id=235, features={"a": 50.0, "b": 50.0})
    assert pred.score == pytest.approx(100.0)
    assert pred.category == "critical"


def test_risk_predictor_raises_tenant_model_missing_for_unknown_tenant():
    p = RiskPredictor()
    features = ["a", "b"]
    _inject_artifact(p, _build_synthetic_artifact(features, tenants=[235, 238]))

    with pytest.raises(TenantModelMissingError) as excinfo:
        p.predict(tenant_id=999, features={"a": 1.0, "b": 1.0})
    assert "999" in str(excinfo.value)
    # The exception should list the trained tenants so the operator
    # immediately sees the cohort the model knows about.
    assert "235" in str(excinfo.value) and "238" in str(excinfo.value)


def test_risk_predictor_predict_handles_missing_features_as_zero():
    """Same convention as the SQL view's COALESCE: missing keys → 0."""
    p = RiskPredictor()
    features = ["a", "b", "c"]
    _inject_artifact(p, _build_synthetic_artifact(features, tenants=[235]))

    # Only provide 'a'; b and c default to 0 → sum = 3 → raw = 3 →
    # (3 - 0) / (10 - 0) = 0.3 → 30 → moderate band.
    pred = p.predict(tenant_id=235, features={"a": 3.0})
    assert pred.score == pytest.approx(30.0)
    assert pred.category == "moderate"


def test_risk_predictor_predict_batch_returns_aligned_arrays():
    """Vectorised path: one score + label per input row."""
    import pandas as pd

    p = RiskPredictor()
    features = ["a", "b"]
    _inject_artifact(p, _build_synthetic_artifact(features, tenants=[235]))

    df = pd.DataFrame({"a": [0.0, 5.0, 100.0], "b": [0.0, 5.0, 100.0]})
    scores, labels = p.predict_batch(tenant_id=235, features_df=df)
    assert scores.shape == (3,)
    assert labels.shape == (3,)
    # Hand-computed: sums 0/10/200, raw 0/10/200, rescaled 0/100/100, the
    # last two clip to the top. Labels: low, critical, critical.
    np.testing.assert_allclose(scores, [0.0, 100.0, 100.0])
    assert list(labels) == ["low", "critical", "critical"]


def test_risk_predictor_predict_batch_empty_returns_empty_arrays():
    """No rows in → no rows out. Don't blow up on the edge case."""
    import pandas as pd

    p = RiskPredictor()
    features = ["a", "b"]
    _inject_artifact(p, _build_synthetic_artifact(features, tenants=[235]))

    scores, labels = p.predict_batch(
        tenant_id=235, features_df=pd.DataFrame(columns=features)
    )
    assert scores.shape == (0,)
    assert labels.shape == (0,)


def test_risk_predictor_tenants_listing_is_sorted_and_typed():
    p = RiskPredictor()
    _inject_artifact(p, _build_synthetic_artifact(["a"], tenants=[7486, 235, 1787]))
    assert p.tenants == [235, 1787, 7486]
    assert p.has_tenant(235) is True
    assert p.has_tenant(999) is False


# ---------------------------------------------------------------------------
# ClusterPredictor — unchanged contract; the test mostly guards against
# the loader regressing into eager mode.
# ---------------------------------------------------------------------------
def test_cluster_predictor_raises_when_no_model(monkeypatch):
    """
    Without an MLflow Production model AND no local joblib, predict() raises.

    Same fail-soft setup as test_risk_predictor_raises_when_no_model_anywhere
    — point the local-load directory at a guaranteed-empty path and pin
    MLFLOW_TRACKING_URI to an unreachable URL.
    """
    monkeypatch.setattr(
        "accent_fleet.ml.inference.CLUSTERING_MODEL_DIR",
        CLUSTERING_MODEL_DIR.parent / "__no_such_dir_for_cluster_test__",
    )
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:0")

    p = ClusterPredictor()
    # The predictor is intentionally lazy: instantiation never throws.
    assert p.is_loaded is False
    with pytest.raises(RuntimeError) as excinfo:
        p.predict({})
    assert "clustering model" in str(excinfo.value).lower()
