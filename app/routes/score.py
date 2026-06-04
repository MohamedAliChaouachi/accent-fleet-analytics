"""
Scoring endpoints.

  POST /score/risk     -> per-tenant Isolation Forest 0-100 anomaly score
                          with category band and per-feature z-scores.
  POST /score/cluster  -> KMeans cluster id + distance to centroid

Both endpoints take a free-form feature dict; missing keys default to 0
(consistent with the SQL view's COALESCE behaviour).

/score/risk requires a ``tenant_id`` in the payload because the model is
fitted per tenant — a vector without a tenant cannot be routed to a model.
A request for a tenant we don't have a model for yet returns 503 (not 404),
so dashboards can tell "feature not ready" apart from "feature broken".
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from accent_fleet.config import config_hash
from accent_fleet.ml.inference import (
    ClusterPredictor,
    RiskPredictor,
    TenantModelMissingError,
)
from app.deps import ClusterPredictorDep, RiskPredictorDep
from app.schemas.score import (
    ClusterScoreResponse,
    FeatureVector,
    RiskScoreResponse,
)

# Scoring router — risk (per-tenant Isolation Forest) and cluster (KMeans).
router = APIRouter(prefix="/score", tags=["scoring"])


@router.post("/risk", response_model=RiskScoreResponse)
def score_risk(
    payload: FeatureVector,
    predictor: RiskPredictor = RiskPredictorDep,
) -> RiskScoreResponse:
    """Compute the 0-100 anomaly score for a single device-month feature vector."""
    if payload.tenant_id is None:
        # 422 is the right code: the input is structurally valid JSON but
        # semantically incomplete. FastAPI uses 422 for pydantic errors;
        # we mirror the convention for cross-field requirements.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="tenant_id is required for /score/risk",
        )

    # Route the vector to the tenant's model; map "not ready" failures to 503.
    try:
        prediction = predictor.predict(
            tenant_id=int(payload.tenant_id),
            features=payload.as_dict(),
        )
    except TenantModelMissingError as exc:
        # We have a model loaded, just not one for this tenant. 503 because
        # this is a "feature unavailable for this dimension" condition that
        # will resolve once a retrain includes this tenant.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    except RuntimeError as exc:
        # No risk model loaded at all (e.g. fresh stack, no training run yet).
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc

    # Shape the score, category band, per-feature z-scores, and version stamp.
    return RiskScoreResponse(
        risk_score=prediction.score,
        category=prediction.category,
        components=prediction.components,
        model_version=prediction.model_version,
        version=config_hash(),
    )


@router.post("/cluster", response_model=ClusterScoreResponse)
def score_cluster(
    payload: FeatureVector,
    predictor: ClusterPredictor = ClusterPredictorDep,
) -> ClusterScoreResponse:
    """Assign the feature vector to a KMeans cluster."""
    # Predict cluster + centroid distance; 503 when no model is trained yet.
    try:
        cluster_id, distance = predictor.predict(payload.as_dict())
    except RuntimeError as exc:
        # No trained model available yet — distinct status so callers can
        # show a helpful "train a model" message rather than a generic 500.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc

    return ClusterScoreResponse(
        cluster_id=cluster_id,
        distance=round(distance, 4),
        model_name="device-clustering",
        model_version=predictor.model_version,
    )
