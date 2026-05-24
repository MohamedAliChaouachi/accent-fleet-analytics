"""
Unit tests for the gated promotion logic.

DB-free + MLflow-free: every test exercises the pure decision functions
``decide_promotion`` (clustering / silhouette) and ``decide_risk_promotion``
(risk score / stability) directly. The MLflow-backed entrypoints
(``get_current_production_silhouette``, ``get_current_production_risk_share``,
``retrain_with_gate``, ``retrain_risk_with_gate``) are covered by integration
tests against a live tracking server, which we don't run in CI.

What we pin for the clustering gate:
  1. Cold start: no current model → promote unconditionally.
  2. Strict improvement → promote.
  3. Within-tolerance regression → promote (noise floor).
  4. Outside-tolerance regression → hold.
  5. Exactly-at-floor → promote (inclusive boundary).
  6. Tolerance of 0 → strict ≥ behaviour.
  7. The decision's reason string is human-readable and contains the
     numbers that informed it (audit-friendliness).

What we pin for the risk gate:
  1. Cold start: no current share → promote unconditionally.
  2. Share within both per-pp tolerances + PSI under threshold → promote.
  3. Δ%critical > tolerance → hold (and the reason names the offender).
  4. Δ%high > tolerance → hold.
  5. PSI ≥ threshold → hold.
  6. PSI = None → treated as "unknown, no veto" (the share gate still applies).
  7. Multiple violations → all listed in the reason.
  8. Explicit per-call tolerance overrides YAML defaults.
  9. RiskPromotionDecision is frozen.
"""

from __future__ import annotations

from accent_fleet.ml.promotion import (
    DEFAULT_RISK_MAX_CRITICAL_SHIFT_PP,
    DEFAULT_RISK_MAX_HIGH_SHIFT_PP,
    DEFAULT_RISK_MAX_SCORE_PSI,
    DEFAULT_SILHOUETTE_TOLERANCE,
    decide_promotion,
    decide_risk_promotion,
)


def test_cold_start_promotes_unconditionally():
    """No baseline → first model has to ship. Otherwise the API stays empty."""
    d = decide_promotion(candidate_silhouette=0.10, current_silhouette=None)
    assert d.promote is True
    assert d.reason == "no_current_production_model"
    assert d.current_silhouette is None
    assert d.candidate_silhouette == 0.10


def test_strict_improvement_promotes():
    d = decide_promotion(candidate_silhouette=0.45, current_silhouette=0.40)
    assert d.promote is True
    assert "candidate 0.4500" in d.reason
    assert "floor 0.3800" in d.reason  # 0.40 - 0.02 = 0.38


def test_within_tolerance_regression_still_promotes():
    """
    A small drop (≤ tolerance) is treated as noise. Without this the model
    freezes after one lucky run — silhouette has run-to-run variance from
    KMeans re-fits.
    """
    d = decide_promotion(candidate_silhouette=0.385, current_silhouette=0.40)
    assert d.promote is True
    # The 0.015 drop is within the default 0.02 tolerance.
    assert d.candidate_silhouette < d.current_silhouette  # type: ignore[operator]


def test_outside_tolerance_regression_holds():
    d = decide_promotion(candidate_silhouette=0.30, current_silhouette=0.40)
    assert d.promote is False
    assert "regression" in d.reason
    # Audit string must show both numbers + the computed floor.
    assert "0.3000" in d.reason and "0.4000" in d.reason


def test_boundary_exactly_at_floor_promotes():
    """
    floor = current - tolerance. Candidate exactly at floor → still ≥ floor,
    so promote. Otherwise the inclusive contract in the docstring lies.
    """
    current = 0.40
    floor = current - DEFAULT_SILHOUETTE_TOLERANCE
    d = decide_promotion(candidate_silhouette=floor, current_silhouette=current)
    assert d.promote is True


def test_zero_tolerance_demands_strict_non_regression():
    """
    Setting tolerance=0 turns the gate into "strict ≥". Useful when a team
    decides any silhouette drop is unacceptable (e.g. they've stabilised
    the training pipeline enough that variance shouldn't show up).
    """
    d_equal = decide_promotion(
        candidate_silhouette=0.40, current_silhouette=0.40, tolerance=0.0
    )
    assert d_equal.promote is True

    d_drop = decide_promotion(
        candidate_silhouette=0.3999, current_silhouette=0.40, tolerance=0.0
    )
    assert d_drop.promote is False


def test_decision_is_frozen_and_carries_inputs():
    """
    PromotionDecision is frozen so logs / Prometheus tags can't be
    accidentally rewritten downstream. It must also carry the input
    values for audit, not just the outcome.
    """
    d = decide_promotion(
        candidate_silhouette=0.55,
        current_silhouette=0.50,
        tolerance=0.03,
    )
    assert d.candidate_silhouette == 0.55
    assert d.current_silhouette == 0.50
    assert d.tolerance == 0.03

    # Frozen dataclass → mutation raises.
    import dataclasses

    try:
        d.promote = False  # type: ignore[misc]
    except dataclasses.FrozenInstanceError:
        pass
    else:
        raise AssertionError("PromotionDecision should be frozen")


# =============================================================================
# Risk-gate (Isolation Forest) tests
# =============================================================================
# Shares are dicts keyed by the four risk categories, values in [0, 1].
# The gate compares the operationally meaningful tails — 'critical' and
# 'high' — and tolerates moves in the 'low'/'moderate' boundary because
# those don't trigger work for anyone.

def _share(low: float, moderate: float, high: float, critical: float) -> dict:
    return {"low": low, "moderate": moderate, "high": high, "critical": critical}


def test_risk_cold_start_promotes_unconditionally():
    """No prior Production share → first risk model has to ship."""
    d = decide_risk_promotion(
        candidate_share=_share(0.6, 0.25, 0.10, 0.05),
        current_share=None,
        score_psi=None,
    )
    assert d.promote is True
    assert d.reason == "no_current_production_risk_model"
    assert d.current_share is None
    assert d.candidate_share["critical"] == 0.05


def test_risk_within_tolerance_promotes():
    """
    Critical share moves 2pp (≤ 5pp default), high moves 1pp, PSI 0.10 < 0.25
    → all checks pass, promote.
    """
    d = decide_risk_promotion(
        candidate_share=_share(0.58, 0.25, 0.11, 0.06),
        current_share=_share(0.60, 0.25, 0.10, 0.05),
        score_psi=0.10,
    )
    assert d.promote is True
    # Reason should include the deltas for audit purposes.
    assert "0.10pp" in d.reason or "1.00pp" in d.reason
    assert "score_psi=0.1000" in d.reason


def test_risk_critical_shift_above_tolerance_holds():
    """Δ%critical = 7pp > 5pp default → hold; reason names the offender."""
    d = decide_risk_promotion(
        candidate_share=_share(0.50, 0.25, 0.13, 0.12),  # +7pp critical
        current_share=_share(0.60, 0.25, 0.10, 0.05),
        score_psi=0.05,
    )
    assert d.promote is False
    assert "critical" in d.reason.lower()
    assert "7" in d.reason or "7.00" in d.reason  # the offending delta


def test_risk_high_shift_above_tolerance_holds():
    """Δ%high = 8pp > 5pp default → hold; reason names the offender."""
    d = decide_risk_promotion(
        candidate_share=_share(0.55, 0.20, 0.18, 0.07),  # +8pp high
        current_share=_share(0.60, 0.25, 0.10, 0.05),
        score_psi=0.05,
    )
    assert d.promote is False
    assert "high" in d.reason.lower()
    assert "8" in d.reason


def test_risk_high_psi_holds():
    """
    Both share shifts within tolerance, but PSI 0.30 >= 0.25 → hold.
    This protects against "category mix looks fine but the underlying
    score distribution has shifted shape" — which is invisible to a
    pure category check.
    """
    d = decide_risk_promotion(
        candidate_share=_share(0.60, 0.25, 0.10, 0.05),  # zero share shift
        current_share=_share(0.60, 0.25, 0.10, 0.05),
        score_psi=0.30,
    )
    assert d.promote is False
    assert "score_psi=0.3000" in d.reason
    assert ">=" in d.reason or ">= 0.2500" in d.reason


def test_risk_psi_none_treated_as_unknown():
    """
    PSI = None (cold start, or compute_score_drift failed soft) → no veto
    from the PSI dimension; the share gate still applies.
    """
    d = decide_risk_promotion(
        candidate_share=_share(0.60, 0.25, 0.10, 0.05),
        current_share=_share(0.60, 0.25, 0.10, 0.05),
        score_psi=None,
    )
    assert d.promote is True
    assert "n/a" in d.reason.lower() or "none" in d.reason.lower()


def test_risk_multiple_violations_all_listed_in_reason():
    """A gate that flags 3 things should report 3 things — not just the first."""
    d = decide_risk_promotion(
        candidate_share=_share(0.40, 0.20, 0.20, 0.20),  # +10pp critical, +10pp high
        current_share=_share(0.60, 0.25, 0.10, 0.05),
        score_psi=0.50,
    )
    assert d.promote is False
    assert "critical" in d.reason.lower()
    assert "high" in d.reason.lower()
    assert "psi" in d.reason.lower()


def test_risk_explicit_tolerance_overrides_yaml_defaults():
    """
    Per-call kwargs win over YAML defaults. Tighten critical tolerance to 1pp
    and a 2pp delta that would normally pass now holds.
    """
    candidate = _share(0.58, 0.25, 0.10, 0.07)  # +2pp critical
    current = _share(0.60, 0.25, 0.10, 0.05)

    # Default tolerance is 5pp → would promote.
    d_default = decide_risk_promotion(
        candidate_share=candidate, current_share=current, score_psi=0.05
    )
    assert d_default.promote is True

    # Tightened to 1pp → holds.
    d_strict = decide_risk_promotion(
        candidate_share=candidate,
        current_share=current,
        score_psi=0.05,
        max_critical_shift_pp=1.0,
    )
    assert d_strict.promote is False
    assert d_strict.max_critical_shift_pp == 1.0


def test_risk_decision_is_frozen():
    """RiskPromotionDecision is frozen so audit fields can't be rewritten."""
    d = decide_risk_promotion(
        candidate_share=_share(0.6, 0.25, 0.10, 0.05),
        current_share=None,
        score_psi=None,
    )
    import dataclasses

    try:
        d.promote = False  # type: ignore[misc]
    except dataclasses.FrozenInstanceError:
        pass
    else:
        raise AssertionError("RiskPromotionDecision should be frozen")


def test_risk_defaults_match_yaml_documented_values():
    """
    Sanity-check the YAML-default constants haven't drifted from the design
    contract documented in config/feature_definitions.yaml. Catches a
    careless edit of the constant that bypasses the YAML.
    """
    assert DEFAULT_RISK_MAX_CRITICAL_SHIFT_PP == 5.0
    assert DEFAULT_RISK_MAX_HIGH_SHIFT_PP == 5.0
    assert DEFAULT_RISK_MAX_SCORE_PSI == 0.25
