"""
Unit tests for the PSI drift computation.

DB-free: every test exercises the pure-Python math layer
(`compute_psi`, `compare_frames`, `derive_reference_window`). The
DB-backed driver (`detect_drift_for_months`) is covered in an integration
test that we don't run in CI.

What we pin:
  1. Identical distributions → PSI ≈ 0.
  2. Wildly different distributions → PSI exceeds the 0.25 alert threshold.
  3. Empty inputs return 0.0, not NaN, so downstream comparisons don't blow up.
  4. Constant-valued reference returns 0.0 (no valid bin edges).
  5. The full DriftReport flags exactly the features that crossed the threshold.
  6. The reference-window helper walks calendar months correctly across year
     boundaries.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from accent_fleet.ml.drift import (
    PSI_ALERT_THRESHOLD,
    compare_frames,
    compute_psi,
    derive_reference_window,
)


def test_psi_identical_distributions_is_near_zero():
    rng = np.random.default_rng(0)
    sample = rng.normal(loc=0.0, scale=1.0, size=5000)
    # Take two independent draws from the same distribution — PSI should
    # be ~0 (any non-zero value comes from finite-sample noise).
    a = rng.normal(loc=0.0, scale=1.0, size=5000)
    psi = compute_psi(sample, a)
    assert psi < 0.01, f"identical distributions should yield PSI≈0, got {psi}"


def test_psi_large_shift_exceeds_alert_threshold():
    rng = np.random.default_rng(1)
    reference = rng.normal(loc=0.0, scale=1.0, size=5000)
    # Shift the mean by 2σ — banking-monitoring literature considers this
    # a major change. PSI must exceed 0.25.
    current = rng.normal(loc=2.0, scale=1.0, size=5000)
    psi = compute_psi(reference, current)
    assert psi > PSI_ALERT_THRESHOLD, (
        f"2σ mean shift should trigger drift alert; got PSI={psi}"
    )


def test_psi_empty_inputs_returns_zero():
    """No data → no signal. Returning 0.0 keeps comparisons trivial."""
    assert compute_psi(np.array([]), np.array([1.0, 2.0])) == 0.0
    assert compute_psi(np.array([1.0, 2.0]), np.array([])) == 0.0
    assert compute_psi(np.array([]), np.array([])) == 0.0


def test_psi_constant_reference_returns_zero():
    """
    A reference distribution that is a single repeated value has no
    meaningful bin edges — PSI is mathematically undefined. We return 0
    so the caller doesn't have to special-case NaN.
    """
    ref = np.zeros(100)
    cur = np.array([0.1, 0.2, 0.3, 1.0, 2.0])
    assert compute_psi(ref, cur) == 0.0


def test_compare_frames_flags_only_drifted_features():
    rng = np.random.default_rng(7)
    n = 3000
    # Two features in the reference: stable + shifted-in-current.
    reference = pd.DataFrame({
        "stable_feature": rng.normal(0.0, 1.0, n),
        "shifted_feature": rng.normal(0.0, 1.0, n),
    })
    current = pd.DataFrame({
        "stable_feature": rng.normal(0.0, 1.0, n),
        "shifted_feature": rng.normal(2.5, 1.0, n),  # large mean shift
    })
    report = compare_frames(
        reference,
        current,
        features=["stable_feature", "shifted_feature"],
    )
    by_name = {f.feature: f for f in report.features}
    assert by_name["stable_feature"].drifted is False
    assert by_name["shifted_feature"].drifted is True
    assert report.drifted_features == ["shifted_feature"]
    assert report.any_drifted is True


def test_compare_frames_skips_features_missing_from_view():
    """
    If a configured feature isn't present in the view (e.g. renamed
    column not yet wired through), we log a warning and skip — we don't
    crash the whole drift report.
    """
    rng = np.random.default_rng(11)
    reference = pd.DataFrame({"a": rng.normal(size=100)})
    current = pd.DataFrame({"a": rng.normal(size=100)})
    report = compare_frames(
        reference, current, features=["a", "missing_column"]
    )
    feats = [f.feature for f in report.features]
    assert feats == ["a"]


def test_derive_reference_window_walks_across_year_boundary():
    """A January anchor must give us months from the prior year."""
    out = derive_reference_window(["2026-02"], n_months=4)
    # 4 months strictly before 2026-02 → 2025-10, 2025-11, 2025-12, 2026-01.
    assert out == ["2025-10", "2025-11", "2025-12", "2026-01"]


def test_derive_reference_window_empty_input_returns_empty():
    assert derive_reference_window([], n_months=6) == []


def test_derive_reference_window_anchors_to_earliest_current_month():
    """
    Backfill scenario: when the scored batch spans multiple months, the
    reference window must precede the earliest one — otherwise the
    reference and current overlap.
    """
    out = derive_reference_window(["2026-03", "2026-01", "2026-02"], n_months=3)
    # min = 2026-01 → ref window is 2025-10..2025-12.
    assert out == ["2025-10", "2025-11", "2025-12"]
    assert all(m not in out for m in ["2026-01", "2026-02", "2026-03"])
