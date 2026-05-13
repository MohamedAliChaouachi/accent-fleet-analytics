"""
Unit tests for the silhouette-gated promotion logic.

DB-free + MLflow-free: every test exercises ``decide_promotion`` directly.
The MLflow-backed entrypoints (``get_current_production_silhouette``,
``retrain_with_gate``) are covered in integration tests that require a
live tracking server, which we don't run in CI.

What we pin:
  1. Cold start: no current model → promote unconditionally.
  2. Strict improvement → promote.
  3. Within-tolerance regression → promote (noise floor).
  4. Outside-tolerance regression → hold.
  5. Exactly-at-floor → promote (inclusive boundary).
  6. Tolerance of 0 → strict ≥ behaviour.
  7. The decision's reason string is human-readable and contains the
     numbers that informed it (audit-friendliness).
"""

from __future__ import annotations

from accent_fleet.ml.promotion import (
    DEFAULT_SILHOUETTE_TOLERANCE,
    decide_promotion,
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
