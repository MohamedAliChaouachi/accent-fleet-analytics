"""
Gated promotion of clustering-model candidates (CRISP-DM §2.5 cont.).

Today's training entry point (``train_clustering.run(promote=True)``)
unconditionally transitions the freshly-registered version to Production.
That works for the very first model but is dangerous once a Production
model exists: a regression on silhouette ships straight to the API.

This module wraps training with a silhouette-based gate:

  promote IFF candidate_silhouette >= current_silhouette - tolerance

Default tolerance is 0.02 (clustering literature: silhouette movement of
±0.01 is noise, ±0.02 is meaningful but small, ±0.05+ is a real signal).
The tolerance is per-percentage-point of silhouette, not per-pct — so
the absolute drop allowed is small. Tunable for code-driven A/B.

Why a tolerance band rather than strict >=:
  Silhouette has sampling variance run-to-run because we re-fit KMeans
  with n_init=10 against a different (newer) data slice. Demanding
  strict improvement would freeze the model after the first lucky run.
  Letting it drop slightly accepts that the population may have shifted
  in ways that mildly hurt cohesion without hurting actionability.

The first-model case (no existing Production version) is handled by
``decide_promotion``: when current_silhouette is None, promotion is
ALLOWED — there's nothing to compare against, and a fresh stack needs
a Production model to serve.

This module never raises on MLflow-side failures. Promotion errors are
reported in the ``RetrainResult.reason`` field so the calling Prefect
task can publish a warning and move on.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from accent_fleet.config import settings
from accent_fleet.ml.train_clustering import (
    TrainResult,
    fit_clustering,
    load_training_frame,
    log_to_mlflow,
    save_local,
)

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
