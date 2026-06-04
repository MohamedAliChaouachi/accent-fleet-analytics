"""
Request/response schemas for the scoring endpoints.

The feature vector is intentionally a free-form dict because the canonical
schema is ``config/feature_definitions.yaml`` (block ``risk_score_model``).
Missing feature keys default to 0 inside RiskPredictor, matching the SQL
view's COALESCE behaviour.

``tenant_id`` is REQUIRED on /v1/score/risk because the risk model is fitted
per tenant — a vector without a tenant cannot be routed to a model. The
clustering endpoint is tenant-agnostic and reuses the same FeatureVector
schema; the extra tenant_id field there is simply ignored.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class FeatureVector(BaseModel):
    """A single device-month feature dict, as produced by v_ml_features_full."""

    model_config = ConfigDict(extra="allow")

    # REQUIRED for /v1/score/risk — selects which per-tenant Isolation Forest
    # bundle inside the loaded artifact will score this vector. Optional only
    # for backward-compatibility on the clustering endpoint, which is
    # tenant-agnostic; the risk route asserts non-null.
    tenant_id: int | None = Field(
        default=None,
        description=(
            "Tenant id selecting the per-tenant risk model. "
            "Required for /v1/score/risk; ignored by /v1/score/cluster."
        ),
    )

    # Optional identifiers — present when the dashboard or pipeline forwards
    # an existing marts row. Not required for ad-hoc what-if scoring.
    device_id: int | None = Field(default=None, description="device dim id")
    month: str | None = Field(default=None, description="YYYY-MM partition key")

    # Common numeric features. Other keys are accepted via extra="allow".
    overspeed_per_100km: float | None = None
    avg_speed_over_limit: float | None = None
    high_speed_trip_ratio: float | None = None
    speed_alert_per_100km: float | None = None
    harsh_brake_per_100km: float | None = None
    harsh_accel_per_100km: float | None = None
    harsh_corner_per_100km: float | None = None
    monthly_idle_ratio: float | None = None
    high_rpm_minutes_per_day: float | None = None
    night_trip_ratio: float | None = None
    rush_hour_trip_ratio: float | None = None
    stddev_trip_distance: float | None = None
    short_trip_ratio: float | None = None

    def as_dict(self) -> dict[str, Any]:
        """Flatten to plain dict (keeps None values; predictor defaults to 0)."""
        return self.model_dump(exclude_none=False)


# Risk scoring result: 0-100 score, category band, and z-score components.
class RiskScoreResponse(BaseModel):
    risk_score: float = Field(..., ge=0, le=100)
    category: str = Field(..., description="low | moderate | high | critical")
    components: dict[str, float] = Field(
        default_factory=dict,
        description=(
            "Per-feature z-score from the tenant's StandardScaler "
            "(feature name -> standardised value). Replaces the old "
            "per-factor weighted contributions."
        ),
    )
    model_version: str = Field(
        ...,
        description=(
            "MLflow model version that produced this score, or "
            "'local:<v>' when served from the joblib fallback."
        ),
    )
    version: str = Field(..., description="Config hash that produced this score.")


# Cluster assignment result: cluster id + distance to its centroid.
class ClusterScoreResponse(BaseModel):
    cluster_id: int
    distance: float = Field(..., description="Euclidean distance to the assigned centroid.")
    model_name: str
    model_version: str = Field(..., description="MLflow model version or 'unloaded'.")
