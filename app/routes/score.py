"""
Scoring endpoints.

  POST /score/risk     -> composite risk score + per-factor breakdown
  POST /score/cluster  -> KMeans cluster id + distance to centroid

Both endpoints take a free-form feature dict; missing keys default to 0
(consistent with the SQL view's COALESCE behaviour).
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from accent_fleet.config import config_hash
from accent_fleet.features.risk_score import RiskScorer
from accent_fleet.ml.inference import ClusterPredictor, score_with_components
from app.deps import ClusterPredictorDep, RiskScorerDep
from app.schemas.score import (
    ClusterScoreResponse,
    FeatureVector,
    RiskScoreResponse,
)

router = APIRouter(prefix="/score", tags=["scoring"])


@router.post("/risk", response_model=RiskScoreResponse)
def score_risk(
    payload: FeatureVector,
    scorer: RiskScorer = RiskScorerDep,
) -> RiskScoreResponse:
    """Compute the 0-100 composite risk score for a single feature vector."""
    features = payload.as_dict()
    total, components = score_with_components(scorer, features)
    return RiskScoreResponse(
        risk_score=total,
        category=scorer.categorize(total),
        components=components,
        version=config_hash(),
    )


@router.post("/cluster", response_model=ClusterScoreResponse)
def score_cluster(
    payload: FeatureVector,
    predictor: ClusterPredictor = ClusterPredictorDep,
) -> ClusterScoreResponse:
    """Assign the feature vector to a KMeans cluster."""
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
