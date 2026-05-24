"""
Unit tests for the PSI drift computation.

DB-free: every test exercises the pure-Python math layer
(`compute_psi`, `compare_frames`, `derive_reference_window`). The
DB-backed driver (`detect_drift_for_months`) is covered in an integration
test that we don't run in CI.

For ``compute_score_drift`` we mock ``pd.read_sql`` at the drift-module
boundary so the function still goes through its real SQL-text branch
(catches dumb refactors that drop the year_month filter) while never
opening a real connection.

What we pin:
  1. Identical distributions → PSI ≈ 0.
  2. Wildly different distributions → PSI exceeds the 0.25 alert threshold.
  3. Empty inputs return 0.0, not NaN, so downstream comparisons don't blow up.
  4. Constant-valued reference returns 0.0 (no valid bin edges).
  5. The full DriftReport flags exactly the features that crossed the threshold.
  6. The reference-window helper walks calendar months correctly across year
     boundaries.
  7. compute_score_drift returns None on empty input + on empty current/ref.
  8. compute_score_drift returns a finite PSI on a clear shift.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from accent_fleet.ml import drift as drift_mod
from accent_fleet.ml.drift import (
    PSI_ALERT_THRESHOLD,
    compare_frames,
    compute_psi,
    compute_score_drift,
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


# =============================================================================
# compute_score_drift — PSI of the IF risk-score column between two windows
# =============================================================================
# We mock at the pandas boundary (``pd.read_sql``) and at the engine boundary
# (``get_engine``) so the function runs end-to-end without a Postgres. The
# fake connection is a no-op context manager: drift only uses ``conn`` as a
# handle to pass to read_sql, which we've already intercepted.


class _FakeConn:
    def __enter__(self):
        return self
    def __exit__(self, *exc):
        return False


def _install_fake_engine(monkeypatch, by_month: dict[str, list[float]]) -> dict:
    """
    Intercept the engine + pd.read_sql call inside drift.compute_score_drift
    so it returns the canned scores for whichever months were requested.

    Returns a `calls` dict that records every (months,) tuple read_sql saw,
    so tests can assert on the reference vs. current windows the function
    derived. Catches a refactor that accidentally re-orders the queries or
    skips one of them.
    """
    calls: dict[str, list[list[str]]] = {"months_seen": []}

    def fake_engine():
        class _Eng:
            def connect(self_inner):
                return _FakeConn()
        return _Eng()

    def fake_read_sql(sql, conn, params=None, **kwargs):
        months = list((params or {}).get("months") or [])
        calls["months_seen"].append(months)
        rows: list[float] = []
        for m in months:
            rows.extend(by_month.get(m, []))
        return pd.DataFrame({"year_month": ["x"] * len(rows), "risk_score": rows})

    monkeypatch.setattr(drift_mod, "get_engine", fake_engine)
    monkeypatch.setattr(drift_mod.pd, "read_sql", fake_read_sql)
    return calls


def test_compute_score_drift_empty_current_months_returns_none(monkeypatch):
    """No months scored this run → no signal to compute drift against."""
    _install_fake_engine(monkeypatch, by_month={})
    assert compute_score_drift([]) is None


def test_compute_score_drift_no_reference_rows_returns_none(monkeypatch):
    """
    Cold start: the current window has rows but the derived 6-month
    reference window is entirely empty (we've literally never scored
    before). Returning None lets the promotion gate treat PSI as
    "unknown, no veto" instead of falsely promoting on bogus zero.
    """
    _install_fake_engine(
        monkeypatch,
        by_month={"2026-05": [10.0, 20.0, 30.0, 40.0, 50.0]},
    )
    assert compute_score_drift(["2026-05"]) is None


def test_compute_score_drift_returns_low_psi_on_identical_distributions(monkeypatch):
    """
    Same scores in reference and current windows → PSI ≈ 0 (well under
    the 0.25 alert threshold). Sanity that the function actually wires
    the data through, not that it accidentally returns a constant.
    """
    rng = np.random.default_rng(42)
    sample = rng.uniform(0, 100, size=2000).tolist()

    # 6 reference months derived from current=["2026-05"] → 2025-11..2026-04.
    by_month = {f"2025-{m:02d}": sample for m in range(11, 13)}
    by_month.update({f"2026-{m:02d}": sample for m in range(1, 6)})

    _install_fake_engine(monkeypatch, by_month=by_month)
    psi = compute_score_drift(["2026-05"])
    assert psi is not None
    assert psi < PSI_ALERT_THRESHOLD, f"identical scores should yield PSI<<0.25, got {psi}"


def test_compute_score_drift_returns_high_psi_on_shift(monkeypatch):
    """
    Reference centered at 10, current at 80 → must trip the 0.25 alert
    threshold. This is the exact pattern that triggers a hold in the
    risk promotion gate.
    """
    rng = np.random.default_rng(1)
    ref_scores = rng.normal(loc=10.0, scale=5.0, size=2000).clip(0, 100).tolist()
    cur_scores = rng.normal(loc=80.0, scale=5.0, size=2000).clip(0, 100).tolist()

    by_month = {f"2025-{m:02d}": ref_scores for m in range(11, 13)}
    by_month.update({f"2026-{m:02d}": ref_scores for m in range(1, 5)})
    by_month["2026-05"] = cur_scores

    _install_fake_engine(monkeypatch, by_month=by_month)
    psi = compute_score_drift(["2026-05"])
    assert psi is not None
    assert psi > PSI_ALERT_THRESHOLD, f"large mean shift should trip the gate, got {psi}"


def test_compute_score_drift_queries_reference_then_current(monkeypatch):
    """
    Two SQL calls — first for the reference window, then for the current
    window. If a refactor accidentally drops one (or sends the same
    months twice), the calls list catches it.
    """
    rng = np.random.default_rng(0)
    ref_scores = rng.uniform(0, 50, size=200).tolist()
    cur_scores = rng.uniform(50, 100, size=200).tolist()

    by_month = {f"2025-{m:02d}": ref_scores for m in range(11, 13)}
    by_month.update({f"2026-{m:02d}": ref_scores for m in range(1, 5)})
    by_month["2026-05"] = cur_scores

    calls = _install_fake_engine(monkeypatch, by_month=by_month)
    psi = compute_score_drift(["2026-05"])
    assert psi is not None
    assert len(calls["months_seen"]) == 2
    # First query is reference, second is current. Reference must NOT
    # include 2026-05; current MUST be exactly ['2026-05'].
    ref_call, cur_call = calls["months_seen"]
    assert "2026-05" not in ref_call
    assert cur_call == ["2026-05"]
    # And the reference window has the expected 6-month shape.
    assert len(ref_call) == 6
