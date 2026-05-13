"""
Request/response schemas for the scoring endpoints.

The feature vector is intentionally a free-form dict because the canonical
schema is `config/feature_definitions.yaml`. Validation that all required
keys are present happens inside RiskScorer (missing keys default to 0,
matching the SQL view's COALESCE behaviour).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class FeatureVector(BaseModel):
    """A single device-month feature dict, as produced by v_ml_features_full."""

    model_config = ConfigDict(extra="allow")

    # Optional identifiers — present when the dashboard or pipeline forwards
    # an existing marts row. Not required for ad-hoc what-if scoring.
    device_id: int | None = Field(default=None, description="device dim id")
    month: str | None = Field(default=None, description="YYYY-MM partition key")

    # Common numeric features. Other keys are accepted via extra="allow".
    overspeed_per_100km: float | None = None
    overspeed_count: float | None = None
    overspeed_severity_high: float | None = None
    overspeed_severity_extreme: float | None = None
    high_speed_trip_ratio: float | None = None
    speed_alert_per_100km: float | None = None
    night_trip_ratio: float | None = None
    avg_max_speed_kmh: float | None = None

    def as_dict(self) -> dict[str, Any]:
        """Flatten to plain dict (drops None identifiers)."""
        return self.model_dump(exclude_none=False)


class RiskScoreResponse(BaseModel):
    risk_score: float = Field(..., ge=0, le=100)
    category: str = Field(..., description="low | moderate | high | critical")
    components: dict[str, float] = Field(
        default_factory=dict,
        description="Per-factor contribution to the score (factor name -> weighted value).",
    )
    version: str = Field(..., description="Config hash that produced this score.")


class ClusterScoreResponse(BaseModel):
    cluster_id: int
    distance: float = Field(..., description="Euclidean distance to the assigned centroid.")
    model_name: str
    model_version: str = Field(..., description="MLflow model version or 'unloaded'.")
