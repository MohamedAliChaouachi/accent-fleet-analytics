"""
Device lookup endpoints.

  GET /devices/{device_id}/profile?months=12
        Rolling risk row + last-N-months descriptive stats.

  GET /devices/top-risk?n=20&tenant_id=...
        Highest-risk devices from the rolling view.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.engine import Connection

from app.deps import DbDep
from app.schemas.device import (
    DeviceMonthRow,
    DeviceProfileResponse,
    DeviceRollingRisk,
    TopRiskDevice,
    TopRiskResponse,
)

router = APIRouter(prefix="/devices", tags=["devices"])


# ---------------------------------------------------------------------------
@router.get("/{device_id}/profile", response_model=DeviceProfileResponse)
def device_profile(
    device_id: int,
    months: int = Query(default=12, ge=1, le=60, description="Number of past months to return"),
    conn: Connection = DbDep,
) -> DeviceProfileResponse:
    """Return rolling risk + recent monthly stats for a single device."""

    rolling_row = conn.execute(
        text(
            """
            SELECT tenant_id, device_id, latest_month, trips_3m, distance_3m,
                   overspeed_3m, severe_overspeed_3m, alerts_3m,
                   risk_score, risk_category
              FROM marts.v_device_risk_profile
             WHERE device_id = :device_id
             LIMIT 1
            """
        ),
        {"device_id": device_id},
    ).mappings().first()

    history_rows = conn.execute(
        text(
            """
            SELECT tenant_id, device_id, year_month::text AS year_month,
                   total_trips, total_distance_km, overspeed_count,
                   overspeed_per_100km, high_speed_trip_ratio,
                   night_trip_ratio, avg_max_speed_kmh
              FROM marts.mart_device_monthly_behavior
             WHERE device_id = :device_id
             ORDER BY year_month DESC
             LIMIT :months
            """
        ),
        {"device_id": device_id, "months": months},
    ).mappings().all()

    if not rolling_row and not history_rows:
        raise HTTPException(status_code=404, detail=f"device {device_id} not found")

    return DeviceProfileResponse(
        device_id=device_id,
        rolling=DeviceRollingRisk(**dict(rolling_row)) if rolling_row else None,
        history=[DeviceMonthRow(**dict(r)) for r in history_rows],
    )


# ---------------------------------------------------------------------------
@router.get("/top-risk", response_model=TopRiskResponse)
def top_risk(
    n: int = Query(default=20, ge=1, le=200),
    tenant_id: int | None = Query(default=None),
    conn: Connection = DbDep,
) -> TopRiskResponse:
    """Highest-risk devices from the rolling 3-month view."""

    sql = """
        SELECT tenant_id, device_id, latest_month::text AS latest_month,
               risk_score, risk_category
          FROM marts.v_device_risk_profile
         WHERE (:tenant_id IS NULL OR tenant_id = :tenant_id)
         ORDER BY risk_score DESC NULLS LAST
         LIMIT :n
    """
    rows = conn.execute(text(sql), {"n": n, "tenant_id": tenant_id}).mappings().all()
    return TopRiskResponse(
        n=n,
        devices=[TopRiskDevice(**dict(r)) for r in rows],
    )
