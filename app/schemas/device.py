"""Response schemas for device lookup endpoints."""

from __future__ import annotations

from pydantic import BaseModel, Field


class DeviceMonthRow(BaseModel):
    """One month from marts.mart_device_monthly_behavior."""

    tenant_id: int | None = None
    device_id: int
    year_month: str
    total_trips: int | None = None
    total_distance_km: float | None = None
    overspeed_count: int | None = None
    overspeed_per_100km: float | None = None
    high_speed_trip_ratio: float | None = None
    night_trip_ratio: float | None = None
    avg_max_speed_kmh: float | None = None


class DeviceRollingRisk(BaseModel):
    """Latest 3-month rolling risk row from marts.v_device_risk_profile."""

    tenant_id: int | None = None
    device_id: int
    latest_month: str | None = None
    trips_3m: int | None = None
    distance_3m: float | None = None
    overspeed_3m: int | None = None
    severe_overspeed_3m: int | None = None
    alerts_3m: int | None = None
    risk_score: float | None = None
    risk_category: str | None = None


# Combined single-device payload: rolling risk + recent monthly history.
class DeviceProfileResponse(BaseModel):
    device_id: int
    rolling: DeviceRollingRisk | None = Field(
        default=None,
        description="Latest 3-month rolling risk (None if device has <10 trips/3m).",
    )
    history: list[DeviceMonthRow] = Field(default_factory=list)


# One entry in the ranked top-risk device list.
class TopRiskDevice(BaseModel):
    tenant_id: int | None = None
    device_id: int
    latest_month: str | None = None
    risk_score: float
    risk_category: str


# Top-N highest-risk devices response wrapper.
class TopRiskResponse(BaseModel):
    n: int = Field(..., ge=1)
    devices: list[TopRiskDevice]
