-- =====================================================================
--  05_kpi_extraction.sql
--  Tenant-month KPI bundle for executive and operational dashboards
-- =====================================================================

WITH base AS (
    SELECT
        tenant_id,
        year_month,
        SUM(total_trips)                AS total_trips,
        SUM(total_distance_km)          AS total_distance_km,
        AVG(avg_max_speed_kmh)          AS avg_max_speed_kmh,
        AVG(p95_max_speed)              AS p95_max_speed,
        AVG(overspeed_per_100km)        AS overspeed_per_100km,
        AVG(harsh_brake_per_100km)      AS harsh_brake_per_100km,
        AVG(harsh_accel_per_100km)      AS harsh_accel_per_100km,
        AVG(monthly_idle_ratio)         AS monthly_idle_ratio,
        AVG(night_trip_ratio)           AS night_trip_ratio,
        AVG(rush_hour_trip_ratio)       AS rush_hour_trip_ratio
    FROM marts.mart_device_monthly_behavior
    GROUP BY tenant_id, year_month
)
SELECT
    tenant_id,
    year_month,
    total_trips,
    total_distance_km,
    avg_max_speed_kmh,
    p95_max_speed,
    overspeed_per_100km,
    harsh_brake_per_100km,
    harsh_accel_per_100km,
    monthly_idle_ratio,
    night_trip_ratio,
    rush_hour_trip_ratio,
    -- Month-over-month delta on overspeed_per_100km
    overspeed_per_100km
    - LAG(overspeed_per_100km) OVER (PARTITION BY tenant_id ORDER BY year_month) AS overspeed_mom_delta,
    -- 3-month rolling average of harsh_brake_per_100km
    AVG(harsh_brake_per_100km) OVER (
        PARTITION BY tenant_id
        ORDER BY year_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS harsh_brake_3m_rolling
FROM base
ORDER BY tenant_id, year_month;

-- High-risk leaderboard per tenant (top 20 device-months)
SELECT
    tenant_id, device_id, year_month, risk_score, risk_band
FROM marts.v_device_risk_profile
WHERE risk_band = 'high'
ORDER BY tenant_id, risk_score DESC
LIMIT 20;
