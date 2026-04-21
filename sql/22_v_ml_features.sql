-- =============================================================================
-- 22_v_ml_features.sql
-- =============================================================================
-- The single contract Phase 4 (Modelling) consumes. Column list is FROZEN for
-- the duration of modelling to avoid feature drift. All 35 features are
-- guaranteed non-null via COALESCE. vehicle_class is integer-encoded.
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_ml_features_driver_behavior AS
SELECT
  -- Identity (kept for join-back, EXCLUDED from modelling)
  m.tenant_id,
  m.device_id,
  m.year_month,

  -- Vehicle class as integer
  CASE dv.vehicle_class
    WHEN 'heavy'  THEN 1
    WHEN 'medium' THEN 2
    WHEN 'light'  THEN 3
    ELSE 0
  END                                                           AS vehicle_class_enc,

  -- Group 1
  COALESCE(m.total_trips, 0)                                    AS total_trips,
  COALESCE(m.total_distance_km, 0.0)                            AS total_distance_km,
  COALESCE(m.avg_trip_distance_km, 0.0)                         AS avg_trip_distance_km,
  COALESCE(m.stddev_trip_distance, 0.0)                         AS stddev_trip_distance,
  COALESCE(m.short_trip_ratio, 0.0)                             AS short_trip_ratio,

  -- Group 2
  COALESCE(m.avg_max_speed_kmh, 0.0)                            AS avg_max_speed_kmh,
  COALESCE(m.p95_max_speed, 0.0)                                AS p95_max_speed,
  COALESCE(m.avg_speed_ratio, 0.0)                              AS avg_speed_ratio,
  COALESCE(m.high_speed_trip_ratio, 0.0)                        AS high_speed_trip_ratio,

  -- Group 3
  COALESCE(m.overspeed_count, 0)                                AS overspeed_count,
  COALESCE(m.overspeed_per_100km, 0.0)                          AS overspeed_per_100km,
  COALESCE(m.overspeed_per_trip, 0.0)                           AS overspeed_per_trip,
  COALESCE(m.overspeed_severity_low, 0)                         AS overspeed_severity_low,
  COALESCE(m.overspeed_severity_medium, 0)                      AS overspeed_severity_medium,
  COALESCE(m.overspeed_severity_high, 0)                        AS overspeed_severity_high,
  COALESCE(m.overspeed_severity_extreme, 0)                     AS overspeed_severity_extreme,
  COALESCE(m.avg_speed_over_limit, 0.0)                         AS avg_speed_over_limit,

  -- Group 4
  COALESCE(m.speed_alert_count, 0)                              AS speed_alert_count,
  COALESCE(m.speed_alert_per_100km, 0.0)                        AS speed_alert_per_100km,
  COALESCE(m.highway_alert_count, 0)                            AS highway_alert_count,
  COALESCE(m.non_highway_alert_count, 0)                        AS non_highway_alert_count,

  -- Group 5
  COALESCE(m.total_stops, 0)                                    AS total_stops,
  COALESCE(m.in_path_stop_ratio, 0.0)                           AS in_path_stop_ratio,
  COALESCE(m.stops_per_trip, 0.0)                               AS stops_per_trip,
  COALESCE(m.micro_stop_count, 0)                               AS micro_stop_count,
  COALESCE(m.short_stop_count, 0)                               AS short_stop_count,
  COALESCE(m.medium_stop_count, 0)                              AS medium_stop_count,
  COALESCE(m.long_stop_count, 0)                                AS long_stop_count,

  -- Group 6
  COALESCE(m.night_trip_ratio, 0.0)                             AS night_trip_ratio,
  COALESCE(m.weekend_trip_ratio, 0.0)                           AS weekend_trip_ratio,
  COALESCE(m.rush_hour_trip_ratio, 0.0)                         AS rush_hour_trip_ratio,
  COALESCE(m.active_days, 0)                                    AS active_days,
  COALESCE(m.avg_working_hours, 0.0)                            AS avg_working_hours
FROM marts.mart_device_monthly_behavior m
LEFT JOIN warehouse.dim_device dd
  ON dd.tenant_id = m.tenant_id AND dd.device_id = m.device_id
LEFT JOIN warehouse.dim_vehicle dv
  ON dv.vehicle_sk = dd.vehicle_sk
WHERE m.total_trips >= 5;                     -- minimum-trips gate
