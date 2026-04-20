-- =============================================================================
-- 21_v_device_risk_profile.sql
-- =============================================================================
-- Rolling 3-month composite risk score per device. Formula mirrors
-- src/accent_fleet/features/risk_score.py exactly.
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_device_risk_profile AS
WITH
latest_3m AS (
  -- 3 most recent year_months that have data for each device
  SELECT
    tenant_id, device_id,
    year_month,
    ROW_NUMBER() OVER (
      PARTITION BY tenant_id, device_id
      ORDER BY year_month DESC
    ) AS rn
  FROM marts.mart_device_monthly_behavior
),
rolling AS (
  SELECT
    m.tenant_id, m.device_id,
    MAX(m.year_month)                             AS latest_month,
    SUM(m.total_trips)                             AS trips_3m,
    SUM(m.total_distance_km)                       AS distance_3m,
    SUM(m.overspeed_count)                         AS overspeed_3m,
    SUM(m.overspeed_severity_high
        + m.overspeed_severity_extreme)            AS severe_overspeed_3m,
    SUM(m.speed_alert_count)                       AS alerts_3m,
    AVG(m.high_speed_trip_ratio)                   AS high_speed_trip_ratio_3m,
    AVG(m.night_trip_ratio)                        AS night_trip_ratio_3m,
    MAX(m.avg_max_speed_kmh)                       AS max_recorded_speed_3m
  FROM marts.mart_device_monthly_behavior m
  JOIN latest_3m l USING (tenant_id, device_id, year_month)
  WHERE l.rn <= 3
  GROUP BY m.tenant_id, m.device_id
),
normalized AS (
  SELECT
    *,
    -- Normalize each factor to [0, 1] using the caps from feature_definitions.yaml
    LEAST(1.0, (COALESCE(overspeed_3m, 0) / NULLIF(distance_3m, 0) * 100) / 10.0)
      AS n_overspeed_rate,
    CASE WHEN overspeed_3m > 0
         THEN severe_overspeed_3m::DOUBLE PRECISION / overspeed_3m
         ELSE 0 END                                AS n_severe_share,
    LEAST(1.0, COALESCE(high_speed_trip_ratio_3m, 0) / 0.3)
                                                   AS n_high_speed_ratio,
    LEAST(1.0, (COALESCE(alerts_3m, 0) / NULLIF(distance_3m, 0) * 100) / 20.0)
      AS n_alert_rate,
    LEAST(1.0, COALESCE(night_trip_ratio_3m, 0) / 0.3)
                                                   AS n_night,
    LEAST(1.0, COALESCE(max_recorded_speed_3m, 0) / 200.0)
                                                   AS n_max_speed
  FROM rolling
  WHERE trips_3m >= 10                              -- gate
)
SELECT
  tenant_id,
  device_id,
  latest_month,
  trips_3m,
  distance_3m,
  overspeed_3m,
  severe_overspeed_3m,
  alerts_3m,
  -- The composite score, weighted per feature_definitions.yaml
  ROUND((
      0.30 * n_overspeed_rate
    + 0.20 * n_severe_share
    + 0.15 * n_high_speed_ratio
    + 0.15 * n_alert_rate
    + 0.10 * n_night
    + 0.10 * n_max_speed
  ) * 100)::INTEGER                                 AS risk_score,
  CASE
    WHEN (0.30 * n_overspeed_rate + 0.20 * n_severe_share
        + 0.15 * n_high_speed_ratio + 0.15 * n_alert_rate
        + 0.10 * n_night + 0.10 * n_max_speed) * 100 < 20 THEN 'low'
    WHEN (0.30 * n_overspeed_rate + 0.20 * n_severe_share
        + 0.15 * n_high_speed_ratio + 0.15 * n_alert_rate
        + 0.10 * n_night + 0.10 * n_max_speed) * 100 < 45 THEN 'moderate'
    WHEN (0.30 * n_overspeed_rate + 0.20 * n_severe_share
        + 0.15 * n_high_speed_ratio + 0.15 * n_alert_rate
        + 0.10 * n_night + 0.10 * n_max_speed) * 100 < 70 THEN 'high'
    ELSE                                                        'critical'
  END                                               AS risk_category
FROM normalized;
