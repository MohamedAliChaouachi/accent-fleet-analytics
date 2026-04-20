"""
Row-level contracts for pipeline boundaries.

Why pydantic models when the data comes from Postgres with typed columns?

1. In **streaming mode** rows arrive as JSON from Kafka. A contract is the
   only thing standing between a malformed event and the warehouse.
2. In **batch mode** they give us a single, typed, testable representation
   that the cleaning and transform modules consume — no more dict-of-Any
   floating around.
3. Schema evolution: adding a field here and failing loudly on missing
   required fields is the right behaviour.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class _BaseEvent(BaseModel):
    """Common shape: all events carry tenant, device, and event-time."""

    model_config = ConfigDict(
        extra="ignore",            # staging may have extra columns we ignore
        frozen=False,
        str_strip_whitespace=True,
    )

    tenant_id: int
    device_id: int
    event_time: datetime           # canonical event-time (varies per source)


class PathEvent(_BaseEvent):
    """One trip from staging.path."""

    end_path_time: datetime | None = None
    distance_driven: float | None = Field(None, ge=0)
    path_duration: int | None = Field(None, ge=0)     # seconds
    max_speed: int | None = Field(None, ge=0)
    fuel_used: float | None = None
    start_odo: float | None = None
    end_odo: float | None = None
    begin_path_latitude: float | None = None
    begin_path_longitude: float | None = None
    end_path_latitude: float | None = None
    end_path_longitude: float | None = None


class StopEvent(_BaseEvent):
    """One stop from staging.stop."""

    stop_end: datetime | None = None
    stop_duration: int | None = Field(None, ge=0)
    in_path: str | None = None
    stop_lat: float | None = None
    stop_long: float | None = None
    stop_adress: str | None = None


class OverspeedEvent(_BaseEvent):
    """One overspeed segment from staging.rep_overspeed."""

    end_path_time: datetime | None = None
    max_speed: int | None = Field(None, ge=0)
    path_duration: int | None = Field(None, ge=0)
    distance_driven: float | None = Field(None, ge=0)


class NotificationEvent(_BaseEvent):
    """One speed alert from staging.notification."""

    notif_id: int
    description: str            # alert type (SPEED, SPEED_HIGHWAY, ...)
    alert_value: str | None = None
    lat: float | None = None
    lng: float | None = None


class ActivityEvent(_BaseEvent):
    """One daily activity row from staging.rep_activity_daily."""

    activity_end_time: datetime | None = None
    activity_start_odo: float | None = None
    activity_end_odo: float | None = None
    working_time: float | None = Field(None, ge=0)   # milliseconds
