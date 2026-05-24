"""
Gated promotion of model candidates (CRISP-DM §2.5 cont.).

Two parallel gates live here:

  - ``decide_promotion`` / ``retrain_with_gate``          — KMeans clustering,
                                                            silhouette-based.
  - ``decide_risk_promotion`` / ``retrain_risk_with_gate`` — Isolation Forest
                                                            risk score,
                                                            stability-based.

Today's training entry points unconditionally transition the freshly-
registered version to Production. That works for the very first model
but is dangerous once a Production model exists: a regression on
silhouette / a sudden shift in the risk category mix ships straight to
the API.

Clustering gate (silhouette):

  promote IFF candidate_silhouette >= current_silhouette - tolerance

Default tolerance is 0.02 (clustering literature: silhouette movement of
±0.01 is noise, ±0.02 is meaningful but small, ±0.05+ is a real signal).
The tolerance is per-percentage-point of silhouette, not per-pct — so
the absolute drop allowed is small. Tunable for code-driven A/B.

Risk gate (stability):

  promote IFF |Δ%critical| ≤ max_critical_shift_pp
          AND |Δ%high|     ≤ max_high_shift_pp
          AND PSI(score)   <  max_score_psi

The risk model is unsupervised — silhouette doesn't apply. We instead
require that the category mix doesn't move much from the current
Production version AND that the score distribution stays stable
(PSI < 0.25 by default, same alert threshold as feature drift). This
protects against "all devices became critical overnight" failure modes
that would otherwise ship invisibly.

Why a tolerance band rather than strict >=:
  Silhouette has sampling variance run-to-run because we re-fit KMeans
  with n_init=10 against a different (newer) data slice. Demanding
  strict improvement would freeze the model after the first lucky run.
  Letting it drop slightly accepts that the population may have shifted
  in ways that mildly hurt cohesion without hurting actionability. The
  risk gate's tolerances exist for the same reason.

The first-model case (no existing Production version) is handled by
the decide_*_promotion functions: when current metrics are None,
promotion is ALLOWED — there's nothing to compare against, and a fresh
stack needs a Production model to serve.

This module never raises on MLflow-side failures. Promotion errors are
reported in the ``RetrainResult.reason`` / ``RiskRetrainResult.reason``
fields so the calling Prefect task can publish a warning and move on.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from accent_fleet.config import load_feature_definitions, settings
from accent_fleet.ml.train_clustering import (
    TrainResult,
    fit_clustering,
    load_training_frame,
    log_to_mlflow,
    save_local,
)
from accent_fleet.ml.train_risk import TrainResult as RiskTrainResult

logger = logging.getLogger("accent_fleet.ml.promotion")


# Industry rule of thumb for KMeans silhouette stability: 0.02 is the
# threshold below which run-to-run noise from re-fits dominates. Public
# so tests and the Prefect task can reference the same number.
DEFAULT_SILHOUETTE_TOLERANCE = 0.02


@dataclass(frozen=True)
class PromotionDecision:
    """
    Outcome of comparing a candidate to the current Production model.
    Frozen because we log this verbatim — accidental mutation in a
    handler would silently change the audit trail.
    """
    promote: bool
    reason: str
    candidate_silhouette: float
    current_silhouette: float | None
    tolerance: float


@dataclass
class RetrainResult:
    """Returned from retrain_with_gate."""
    candidate_version: str | None
    candidate_silhouette: float
    current_silhouette: float | None
    promoted: bool
    reason: str
    train: TrainResult


# ---------------------------------------------------------------------------
# Pure decision function — no I/O, trivially testable.
# ---------------------------------------------------------------------------
def decide_promotion(
    candidate_silhouette: float,
    current_silhouette: float | None,
    tolerance: float = DEFAULT_SILHOUETTE_TOLERANCE,
) -> PromotionDecision:
    """
    Decide whether to promote a candidate over the current Production model.

    Rules:
      1. No current model → always promote. (Cold start.)
      2. candidate >= current - tolerance → promote.
      3. Otherwise → hold.
    """
    if current_silhouette is None:
        return PromotionDecision(
            promote=True,
            reason="no_current_production_model",
            candidate_silhouette=candidate_silhouette,
            current_silhouette=None,
            tolerance=tolerance,
        )

    floor = current_silhouette - tolerance
    if candidate_silhouette >= floor:
        return PromotionDecision(
            promote=True,
            reason=(
                f"candidate {candidate_silhouette:.4f} >= "
                f"floor {floor:.4f} (current {current_silhouette:.4f} - tol {tolerance})"
            ),
            candidate_silhouette=candidate_silhouette,
            current_silhouette=current_silhouette,
            tolerance=tolerance,
        )
    return PromotionDecision(
        promote=False,
        reason=(
            f"silhouette regression: candidate {candidate_silhouette:.4f} "
            f"< floor {floor:.4f} (current {current_silhouette:.4f} - tol {tolerance})"
        ),
        candidate_silhouette=candidate_silhouette,
        current_silhouette=current_silhouette,
        tolerance=tolerance,
    )


# ---------------------------------------------------------------------------
# MLflow lookups — fail soft so retraining works on a fresh stack.
# ---------------------------------------------------------------------------
def get_current_production_silhouette() -> float | None:
    """
    Return the `silhouette` metric of the current Production-stage version.

    Returns None when:
      • mlflow isn't installed
      • no Production version exists yet
      • the metric is missing (older training run before we logged it)

    Never raises — a failed lookup defaults to "no baseline" which
    short-circuits the gate into the cold-start branch.
    """
    try:
        import mlflow
        from mlflow.tracking import MlflowClient
    except ImportError:
        logger.warning("mlflow not installed; cannot read current silhouette")
        return None

    s = settings()
    try:
        mlflow.set_tracking_uri(s.mlflow_tracking_uri)
        client = MlflowClient()
        versions = client.get_latest_versions(
            s.mlflow_model_name, stages=[s.mlflow_model_stage]
        )
        if not versions:
            return None
        run_id = versions[0].run_id
        run = client.get_run(run_id)
        sil = run.data.metrics.get("silhouette")
        return float(sil) if sil is not None else None
    except Exception as exc:  # noqa: BLE001 — fail-soft per docstring
        logger.warning("mlflow current-silhouette lookup failed: %s", exc)
        return None


def _transition_to_production(version: str) -> tuple[bool, str]:
    """
    Move ``version`` to the configured Production stage, archiving prior
    Production versions. Returns (success, reason). Never raises.
    """
    try:
        import mlflow
        from mlflow.tracking import MlflowClient
    except ImportError:
        return False, "mlflow_not_installed"

    s = settings()
    try:
        mlflow.set_tracking_uri(s.mlflow_tracking_uri)
        client = MlflowClient()
        client.transition_model_version_stage(
            name=s.mlflow_model_name,
            version=version,
            stage=s.mlflow_model_stage,
            archive_existing_versions=True,
        )
        return True, f"promoted v{version} to {s.mlflow_model_stage}"
    except Exception as exc:  # noqa: BLE001
        return False, f"promotion_failed: {exc}"


# ---------------------------------------------------------------------------
# Top-level orchestration — used by the Prefect flow and the CLI.
# ---------------------------------------------------------------------------
def retrain_with_gate(
    month_from: str = "2025-01",
    tolerance: float = DEFAULT_SILHOUETTE_TOLERANCE,
) -> RetrainResult:
    """
    Train a candidate model, register it (no immediate promotion), then
    promote IFF the gate allows. The local joblib artifacts are written
    regardless — they serve as the API's disk-fallback path if MLflow is
    unavailable.

    Returns a RetrainResult describing what happened. Caller should log
    the result and emit metrics; this function only does the decision.
    """
    df = load_training_frame(month_from=month_from)
    kmeans, scaler, train = fit_clustering(df)
    train.training_window = f">= {month_from}"

    # Local artifacts first — these are the cheapest insurance against
    # MLflow being down: the API can still load `models/clustering/`.
    save_local(kmeans, scaler, train)

    # Register without auto-promoting so the gate can run first.
    version = log_to_mlflow(kmeans, scaler, train, promote=False)

    current = get_current_production_silhouette()
    decision = decide_promotion(
        candidate_silhouette=train.silhouette,
        current_silhouette=current,
        tolerance=tolerance,
    )

    promoted = False
    reason = decision.reason
    if decision.promote and version is not None:
        ok, msg = _transition_to_production(version)
        promoted = ok
        # Append the transition outcome so a "promote=true but transition
        # failed" case is loud in logs / metrics.
        reason = f"{decision.reason}; {msg}"
    elif decision.promote and version is None:
        reason = "gate_passed_but_mlflow_unavailable"

    return RetrainResult(
        candidate_version=version,
        candidate_silhouette=train.silhouette,
        current_silhouette=current,
        promoted=promoted,
        reason=reason,
        train=train,
    )


# =============================================================================
# Risk-score promotion gate (Isolation Forest)
# =============================================================================
# These constants mirror the YAML defaults under
# config/feature_definitions.yaml::risk_score_model.promotion_gate. They
# live here too so tests have a stable handle to reference.
DEFAULT_RISK_MAX_CRITICAL_SHIFT_PP = 5.0
DEFAULT_RISK_MAX_HIGH_SHIFT_PP = 5.0
DEFAULT_RISK_MAX_SCORE_PSI = 0.25


def _load_risk_gate_defaults() -> tuple[float, float, float]:
    """Read promotion-gate tolerances from feature_definitions.yaml."""
    cfg = (load_feature_definitions().get("risk_score_model") or {})
    gate = cfg.get("promotion_gate") or {}
    return (
        float(gate.get("max_critical_share_shift_pp", DEFAULT_RISK_MAX_CRITICAL_SHIFT_PP)),
        float(gate.get("max_high_share_shift_pp", DEFAULT_RISK_MAX_HIGH_SHIFT_PP)),
        float(gate.get("max_score_psi", DEFAULT_RISK_MAX_SCORE_PSI)),
    )


def _risk_model_name() -> str:
    cfg = (load_feature_definitions().get("risk_score_model") or {})
    return str(cfg.get("mlflow_model_name", "device-risk-score"))


def _risk_model_stage() -> str:
    cfg = (load_feature_definitions().get("risk_score_model") or {})
    return str(cfg.get("mlflow_model_stage", settings().mlflow_model_stage))


@dataclass(frozen=True)
class RiskPromotionDecision:
    """Outcome of comparing a risk-score candidate to current Production."""
    promote: bool
    reason: str
    candidate_share: dict[str, float]
    current_share: dict[str, float] | None
    score_psi: float | None
    max_critical_shift_pp: float
    max_high_shift_pp: float
    max_score_psi: float


@dataclass
class RiskRetrainResult:
    """Returned from retrain_risk_with_gate."""
    candidate_version: str | None
    candidate_share: dict[str, float]
    current_share: dict[str, float] | None
    score_psi: float | None
    promoted: bool
    reason: str
    train: RiskTrainResult


# ---------------------------------------------------------------------------
# Pure decision function — no I/O, trivially testable.
# ---------------------------------------------------------------------------
def decide_risk_promotion(
    candidate_share: dict[str, float],
    current_share: dict[str, float] | None,
    score_psi: float | None,
    *,
    max_critical_shift_pp: float | None = None,
    max_high_shift_pp: float | None = None,
    max_score_psi: float | None = None,
) -> RiskPromotionDecision:
    """
    Decide whether to promote a risk-score candidate.

    Rules:
      1. No current Production share → always promote. (Cold start.)
      2. |Δ%critical| ≤ tolerance AND |Δ%high| ≤ tolerance AND
         (psi unknown OR psi < max_score_psi) → promote.
      3. Otherwise → hold.

    ``candidate_share`` and ``current_share`` are dicts keyed by the four
    risk categories, with values in [0, 1]. The function compares the
    'critical' and 'high' shares (in percentage points) because those are
    the operationally meaningful tails — a swing in the 'low' / 'moderate'
    boundary doesn't trigger work for anyone.
    """
    cap_crit, cap_high, cap_psi = _load_risk_gate_defaults()
    if max_critical_shift_pp is not None:
        cap_crit = max_critical_shift_pp
    if max_high_shift_pp is not None:
        cap_high = max_high_shift_pp
    if max_score_psi is not None:
        cap_psi = max_score_psi

    if current_share is None:
        return RiskPromotionDecision(
            promote=True,
            reason="no_current_production_risk_model",
            candidate_share=candidate_share,
            current_share=None,
            score_psi=score_psi,
            max_critical_shift_pp=cap_crit,
            max_high_shift_pp=cap_high,
            max_score_psi=cap_psi,
        )

    # Shares are in [0, 1]; multiply by 100 to express the tolerance in
    # percentage points, which is how the YAML / config talks about them.
    delta_crit_pp = (
        abs(candidate_share.get("critical", 0.0) - current_share.get("critical", 0.0))
        * 100
    )
    delta_high_pp = (
        abs(candidate_share.get("high", 0.0) - current_share.get("high", 0.0))
        * 100
    )

    reasons: list[str] = []
    if delta_crit_pp > cap_crit:
        reasons.append(
            f"|Δcritical|={delta_crit_pp:.2f}pp > {cap_crit:.2f}pp"
        )
    if delta_high_pp > cap_high:
        reasons.append(
            f"|Δhigh|={delta_high_pp:.2f}pp > {cap_high:.2f}pp"
        )
    if score_psi is not None and score_psi >= cap_psi:
        reasons.append(f"score_psi={score_psi:.4f} >= {cap_psi:.4f}")

    if not reasons:
        return RiskPromotionDecision(
            promote=True,
            reason=(
                f"within tolerances: Δcritical={delta_crit_pp:.2f}pp, "
                f"Δhigh={delta_high_pp:.2f}pp, "
                f"score_psi={'n/a' if score_psi is None else f'{score_psi:.4f}'}"
            ),
            candidate_share=candidate_share,
            current_share=current_share,
            score_psi=score_psi,
            max_critical_shift_pp=cap_crit,
            max_high_shift_pp=cap_high,
            max_score_psi=cap_psi,
        )
    return RiskPromotionDecision(
        promote=False,
        reason="stability_regression: " + "; ".join(reasons),
        candidate_share=candidate_share,
        current_share=current_share,
        score_psi=score_psi,
        max_critical_shift_pp=cap_crit,
        max_high_shift_pp=cap_high,
        max_score_psi=cap_psi,
    )


# ---------------------------------------------------------------------------
# MLflow lookups — fail soft so retraining works on a fresh stack.
# ---------------------------------------------------------------------------
def get_current_production_risk_share() -> dict[str, float] | None:
    """
    Return the overall_share_* metrics of the current Production-stage risk
    model version. Returns None when nothing is registered yet, mlflow isn't
    installed, or the metrics are missing — keeping the gate cold-start safe.
    """
    try:
        import mlflow
        from mlflow.tracking import MlflowClient
    except ImportError:
        logger.warning("mlflow not installed; cannot read current risk share")
        return None

    s = settings()
    name = _risk_model_name()
    stage = _risk_model_stage()
    try:
        mlflow.set_tracking_uri(s.mlflow_tracking_uri)
        client = MlflowClient()
        versions = client.get_latest_versions(name, stages=[stage])
        if not versions:
            return None
        run = client.get_run(versions[0].run_id)
        out: dict[str, float] = {}
        for cat in ("low", "moderate", "high", "critical"):
            metric = run.data.metrics.get(f"overall_share_{cat}")
            if metric is None:
                continue
            out[cat] = float(metric)
        return out or None
    except Exception as exc:  # noqa: BLE001 — fail-soft per docstring
        logger.warning("mlflow current-risk-share lookup failed: %s", exc)
        return None


def _transition_risk_to_production(version: str) -> tuple[bool, str]:
    """Same as _transition_to_production but for the risk-model registry."""
    try:
        import mlflow
        from mlflow.tracking import MlflowClient
    except ImportError:
        return False, "mlflow_not_installed"

    s = settings()
    name = _risk_model_name()
    stage = _risk_model_stage()
    try:
        mlflow.set_tracking_uri(s.mlflow_tracking_uri)
        client = MlflowClient()
        client.transition_model_version_stage(
            name=name,
            version=version,
            stage=stage,
            archive_existing_versions=True,
        )
        return True, f"promoted v{version} to {stage}"
    except Exception as exc:  # noqa: BLE001
        return False, f"promotion_failed: {exc}"


# ---------------------------------------------------------------------------
# Top-level orchestration — used by the Prefect risk-retrain flow and CLI.
# ---------------------------------------------------------------------------
def retrain_risk_with_gate(
    month_from: str = "2025-01",
    *,
    max_critical_shift_pp: float | None = None,
    max_high_shift_pp: float | None = None,
    max_score_psi: float | None = None,
    score_psi_provider: Any = None,
) -> RiskRetrainResult:
    """
    Train a candidate risk model, register it (no immediate promotion),
    then promote IFF the stability gate allows. The local joblib artifacts
    are written regardless — they serve as the API's disk-fallback path
    if MLflow is unavailable.

    ``score_psi_provider`` is an optional callable that takes
    ``(month_from)`` and returns the PSI between the candidate's score
    distribution and the current Production one (None when no baseline).
    It's wired by the Prefect flow to ``ml.drift.compute_score_drift``.
    Kept as a parameter to avoid a cycle: drift imports promotion's
    decision dataclass for the report, not the other way around.
    """
    # Local module import to avoid an import cycle and keep test
    # monkeypatching live (attribute lookup, not name-bound at import).
    # ``train_clustering`` and ``train_risk`` both export ``save_local`` /
    # ``log_to_mlflow`` / ``load_training_frame`` — using the module
    # qualifier here avoids name shadowing with the clustering imports
    # at the top of this file.
    from accent_fleet.ml import train_risk

    df = train_risk.load_training_frame(month_from=month_from)
    artifact, train = train_risk.fit_risk_model(df)
    train.training_window = f">= {month_from}"

    # Local artifacts first — cheapest insurance against MLflow being down.
    train_risk.save_local(artifact, train)

    # Register without auto-promoting so the gate can run first.
    version = train_risk.log_to_mlflow(artifact, train, promote=False)

    current_share = get_current_production_risk_share()
    score_psi: float | None = None
    if score_psi_provider is not None:
        try:
            score_psi = float(score_psi_provider(month_from))
        except Exception as exc:  # noqa: BLE001 — drift is informational
            logger.warning("score_psi_provider failed: %s — treating as unknown", exc)
            score_psi = None

    decision = decide_risk_promotion(
        candidate_share=train.overall_share,
        current_share=current_share,
        score_psi=score_psi,
        max_critical_shift_pp=max_critical_shift_pp,
        max_high_shift_pp=max_high_shift_pp,
        max_score_psi=max_score_psi,
    )

    promoted = False
    reason = decision.reason
    if decision.promote and version is not None:
        ok, msg = _transition_risk_to_production(version)
        promoted = ok
        reason = f"{decision.reason}; {msg}"
    elif decision.promote and version is None:
        reason = "gate_passed_but_mlflow_unavailable"

    return RiskRetrainResult(
        candidate_version=version,
        candidate_share=train.overall_share,
        current_share=current_share,
        score_psi=score_psi,
        promoted=promoted,
        reason=reason,
        train=train,
    )
