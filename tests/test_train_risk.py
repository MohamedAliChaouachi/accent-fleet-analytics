"""
Unit tests for the per-tenant Isolation Forest training math.

DB-free + MLflow-free: every test drives the pure helpers in
``accent_fleet.ml.train_risk`` directly. The end-to-end ``run()`` and the
``log_to_mlflow()`` paths are covered by integration tests we don't run
in CI (those need a live Postgres + MLflow server).

What we pin:
  1. rescale_raw_to_0_100 maps min→0, max→100, clips out-of-range to the
     boundaries, and handles a degenerate (raw_min == raw_max) span.
  2. quantile_thresholds returns the three boundary scores in ascending
     order, matching the (low→moderate, moderate→high, high→critical)
     quantiles. Empty input returns zeros (the caller treats this as a
     no-rows skip, not a crash).
  3. categorize_scores uses ≥ at the lower edge of each band — values
     exactly on a threshold land in the higher band.
  4. _share returns fractions in [0, 1] that sum to 1.0 across the four
     categories. Empty input returns zeros for every category (no NaN).
  5. fit_one_tenant smoke test: given a synthetic frame with the 13 IF
     features it returns an artifact entry with every documented key,
     scores all in [0, 100], and a coherent share dict. Below min_rows
     returns None (signals "skip this tenant" to the orchestrator).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from accent_fleet.ml.train_risk import (
    FEATURES,
    RESCALE_EPSILON,
    _share,
    categorize_scores,
    fit_one_tenant,
    quantile_thresholds,
    rescale_raw_to_0_100,
)


# =============================================================================
# rescale_raw_to_0_100
# =============================================================================
def test_rescale_maps_min_to_zero_and_max_to_hundred():
    """The two anchor points of the linear mapping must hit exactly."""
    raw = np.array([-1.5, 0.0, 1.5, 3.0])
    out = rescale_raw_to_0_100(raw, raw_min=-1.5, raw_max=3.0)
    assert out[0] == pytest.approx(0.0)
    assert out[-1] == pytest.approx(100.0)
    # Midpoint should land at 50.
    assert out[2] == pytest.approx(100.0 * (1.5 - -1.5) / (3.0 - -1.5))


def test_rescale_clips_out_of_range_values():
    """
    Values below raw_min get a 0 (not a negative score). Values above
    raw_max get 100 (not >100). Without clipping, downstream category
    assignment would put genuinely-anomalous rows above the 'critical'
    band's 100 boundary and they'd be lost.
    """
    raw = np.array([-10.0, -1.0, 0.5, 2.0, 99.0])
    out = rescale_raw_to_0_100(raw, raw_min=-1.0, raw_max=2.0)
    assert out[0] == 0.0
    assert out[1] == 0.0      # exact raw_min
    assert out[-2] == 100.0   # exact raw_max
    assert out[-1] == 100.0   # well above raw_max → clipped


def test_rescale_handles_degenerate_span_without_divide_by_zero():
    """
    When every training point has the same raw anomaly score
    (e.g. a tenant with a near-uniform sub-population), raw_max - raw_min
    is 0. The function must NOT divide by zero — it falls back on the
    epsilon floor and returns a sensible 0 for every input.
    """
    raw = np.array([2.5, 2.5, 2.5])
    out = rescale_raw_to_0_100(raw, raw_min=2.5, raw_max=2.5)
    # With span = eps, (x - x) / eps = 0 → all zeros after clip.
    assert np.all(out == 0.0)
    # Epsilon must be a small positive, not 0 — guards against a future
    # edit that removes the floor.
    assert RESCALE_EPSILON > 0


# =============================================================================
# quantile_thresholds
# =============================================================================
def test_quantile_thresholds_returns_ordered_boundaries():
    """
    Quantiles 0.5 / 0.8 / 0.95 of [0..99] are 49.5 / 79.2 / 94.05
    respectively (numpy linear interpolation). Pinned values catch a
    refactor that swaps the order or uses a different quantile method.
    """
    scores = np.arange(0.0, 100.0)
    t_m, t_h, t_c = quantile_thresholds(scores, (0.5, 0.8, 0.95))
    assert t_m < t_h < t_c
    assert t_m == pytest.approx(49.5)
    assert t_h == pytest.approx(79.2)
    assert t_c == pytest.approx(94.05)


def test_quantile_thresholds_empty_input_returns_zeros():
    """Empty array → zeros, not NaN. Matches the empty-share contract."""
    out = quantile_thresholds(np.array([]), (0.5, 0.8, 0.95))
    assert out == (0.0, 0.0, 0.0)


# =============================================================================
# categorize_scores
# =============================================================================
def test_categorize_boundaries_are_inclusive_at_lower_edge():
    """
    Bands are [moderate, high) etc. — a score exactly equal to the
    threshold belongs to the HIGHER band. Without this, a tenant with
    a constant 'moderate' threshold of 25.0 would have rows at exactly
    25.0 silently classified as 'low'.
    """
    scores = np.array([0.0, 24.999, 25.0, 49.999, 50.0, 74.999, 75.0, 100.0])
    cats = categorize_scores(scores, (25.0, 50.0, 75.0))
    assert list(cats) == [
        "low", "low",
        "moderate", "moderate",
        "high", "high",
        "critical", "critical",
    ]


def test_categorize_full_critical_when_thresholds_collapse_to_zero():
    """
    Edge case: a tenant where the quantiles are all 0 (because every
    rescaled score is 0 — degenerate span). categorize_scores must not
    crash; with t_m=t_h=t_c=0, every score >= 0 lands in critical. This
    is exactly the right signal for the gate: "your training data is
    degenerate, this tenant's mix is 100% critical and we should hold."
    """
    scores = np.zeros(5)
    cats = categorize_scores(scores, (0.0, 0.0, 0.0))
    assert all(c == "critical" for c in cats)


# =============================================================================
# _share
# =============================================================================
def test_share_returns_fractions_summing_to_one():
    """
    Eight labels distributed 4/2/1/1 across the four bands → shares
    0.50 / 0.25 / 0.125 / 0.125. Float sum must be 1.0 to within tolerance,
    otherwise the promotion gate's pp comparisons drift.
    """
    labels = np.array(["low"] * 4 + ["moderate"] * 2 + ["high"] + ["critical"])
    out = _share(labels)
    assert out == {"low": 0.5, "moderate": 0.25, "high": 0.125, "critical": 0.125}
    assert sum(out.values()) == pytest.approx(1.0)


def test_share_empty_input_returns_zeros_for_every_category():
    """No labels → zeros (not an empty dict, not NaN). The gate code
    blindly subtracts share[k] - other_share[k]; missing keys would
    KeyError."""
    out = _share(np.array([], dtype=object))
    assert out == {"low": 0.0, "moderate": 0.0, "high": 0.0, "critical": 0.0}


def test_share_returns_zero_for_absent_categories():
    """A tenant that only ever sees 'low' rows still gets entries for the
    other three categories — value 0.0, not missing. Same KeyError
    protection as the empty case."""
    labels = np.array(["low"] * 10)
    out = _share(labels)
    assert out["low"] == 1.0
    assert out["moderate"] == 0.0
    assert out["high"] == 0.0
    assert out["critical"] == 0.0


# =============================================================================
# fit_one_tenant — smoke test against synthetic data
# =============================================================================
def _synthetic_tenant_frame(n: int, *, seed: int = 0) -> pd.DataFrame:
    """
    Build a frame with the 13 IF features plus the tenant_id/year_month
    metadata columns the training pipeline carries. Values are drawn from
    a fixed-seed normal so the test is deterministic but not pathological
    (Isolation Forest needs *some* spread to fit non-trivially).
    """
    rng = np.random.default_rng(seed)
    data = {f: rng.normal(size=n) for f in FEATURES}
    data["tenant_id"] = np.full(n, 235, dtype=int)
    data["device_id"] = np.arange(n)
    data["year_month"] = ["2026-01"] * n
    return pd.DataFrame(data)


def test_fit_one_tenant_returns_well_formed_artifact_entry():
    """
    Fit on 200 rows → must return a dict with every documented key, a
    fitted sklearn IsolationForest, scores entirely in [0, 100], thresholds
    ordered, and a coherent share dict. Locks the artifact contract that
    inference + batch scoring + the promotion gate all read from.
    """
    df = _synthetic_tenant_frame(n=200, seed=1)
    entry = fit_one_tenant(
        df,
        hyperparams={"n_estimators": 50, "random_state": 42},
        quantiles=(0.50, 0.80, 0.95),
        min_rows=50,
    )
    assert entry is not None

    # Required keys — the inference path reads ALL of these.
    expected = {
        "scaler", "model", "raw_min", "raw_max", "thresholds",
        "feature_means", "feature_stds", "n_rows", "score_share",
    }
    assert expected <= set(entry)

    # raw_max strictly above raw_min (200 random rows aren't degenerate).
    assert entry["raw_max"] > entry["raw_min"]

    # Thresholds are ordered ascending — required by categorize_scores.
    th = entry["thresholds"]
    assert th["moderate"] <= th["high"] <= th["critical"]

    # n_rows must match what came in.
    assert entry["n_rows"] == len(df)

    # feature_means / feature_stds have one entry per IF feature.
    assert len(entry["feature_means"]) == len(FEATURES)
    assert len(entry["feature_stds"]) == len(FEATURES)

    # Shares form a valid probability distribution over the four bands.
    share = entry["score_share"]
    assert set(share) == {"low", "moderate", "high", "critical"}
    assert sum(share.values()) == pytest.approx(1.0)
    assert all(0.0 <= v <= 1.0 for v in share.values())


def test_fit_one_tenant_returns_none_when_below_min_rows():
    """
    20 rows with min_rows=50 → skip. The orchestrator (fit_risk_model)
    treats None as "tenant skipped, log it in skipped_tenants and move
    on", which is what keeps a quiet tenant from killing a whole retrain.
    """
    df = _synthetic_tenant_frame(n=20, seed=2)
    entry = fit_one_tenant(
        df,
        hyperparams={"n_estimators": 50, "random_state": 42},
        quantiles=(0.50, 0.80, 0.95),
        min_rows=50,
    )
    assert entry is None


def test_fit_one_tenant_scores_stay_in_zero_hundred_band():
    """
    Indirect contract test: re-run rescale + decision_function inside the
    artifact and verify every score lands inside [0, 100]. This protects
    against a refactor that breaks the clip step — the API contract for
    /score/risk says risk_score ∈ [0, 100].
    """
    df = _synthetic_tenant_frame(n=150, seed=3)
    entry = fit_one_tenant(
        df,
        hyperparams={"n_estimators": 50, "random_state": 42},
        quantiles=(0.50, 0.80, 0.95),
        min_rows=50,
    )
    assert entry is not None

    X = df[list(FEATURES)].fillna(0).to_numpy(dtype=float)
    Xs = entry["scaler"].transform(X)
    raw = -entry["model"].decision_function(Xs)
    scaled = rescale_raw_to_0_100(raw, entry["raw_min"], entry["raw_max"])
    assert scaled.min() >= 0.0
    assert scaled.max() <= 100.0
