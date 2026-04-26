-- =============================================================================
-- 26_v_ml_features_full.sql
-- =============================================================================
-- The FULL ML feature contract for Project 1 (Driver Behavior Scoring & Risk
-- Classification). Extends marts.v_ml_features_driver_behavior with the
-- archive-derived (harsh-event + telemetry) features.
--
-- Why a SEPARATE view rather than altering 22_v_ml_features.sql?
--   - The original 35-feature contract is consumed by existing notebooks /
--     baseline risk score config. Keep it stable.
--   - Adding ~20 new features doubles the column count; consumers should
--     opt-in by querying this view instead.
--   - LEFT JOIN against the new telemetry mart so devices with no archive
--     pings still produce a row (with telemetry features = 0 / NULL).
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_ml_features_full AS
SELECT
  -- All 35 baseline columns from v_ml_features_driver_behavior
  base.*,

  -- ---------------------------------------------------------------------------
  -- Archive-derived features (Group T1–T7)
  -- ---------------------------------------------------------------------------
  -- Group T1: Raw harsh counts
  COALESCE(t.harsh_brake_count, 0)                                AS harsh_brake_count,
  COALESCE(t.harsh_accel_count, 0)                                AS harsh_accel_count,
  COALESCE(t.harsh_corner_count, 0)                               AS harsh_corner_count,
  COALESCE(t.harsh_event_total, 0)                                AS harsh_event_total,

  -- Group T2: Severity tiers
  COALESCE(t.harsh_moderate_count, 0)                             AS harsh_moderate_count,
  COALESCE(t.harsh_high_count, 0)                                 AS harsh_high_count,
  COALESCE(t.harsh_extreme_count, 0)                              AS harsh_extreme_count,

  -- Group T3: Per-100km rates (computed here using base.total_distance_km)
  CASE WHEN base.total_distance_km > 0
       THEN COALESCE(t.harsh_brake_count, 0) * 100.0 / base.total_distance_km
       ELSE 0.0 END                                                AS harsh_brake_per_100km,
  CASE WHEN base.total_distance_km > 0
       THEN COALESCE(t.harsh_accel_count, 0) * 100.0 / base.total_distance_km
       ELSE 0.0 END                                                AS harsh_accel_per_100km,
  CASE WHEN base.total_distance_km > 0
       THEN COALESCE(t.harsh_corner_count, 0) * 100.0 / base.total_distance_km
       ELSE 0.0 END                                                AS harsh_corner_per_100km,
  CASE WHEN base.total_distance_km > 0
       THEN COALESCE(t.harsh_event_total, 0) * 100.0 / base.total_distance_km
       ELSE 0.0 END                                                AS harsh_events_per_100km,

  -- Group T4: Engine / idling
  COALESCE(t.total_ignition_on_minutes, 0.0)                      AS total_ignition_on_minutes,
  COALESCE(t.total_moving_minutes, 0.0)                           AS total_moving_minutes,
  COALESCE(t.total_idle_minutes, 0.0)                             AS total_idle_minutes,
  COALESCE(t.monthly_idle_ratio, 0.0)                             AS monthly_idle_ratio,
  COALESCE(t.active_telemetry_days, 0)                            AS active_telemetry_days,

  -- Group T5: RPM
  COALESCE(t.avg_rpm, 0.0)                                        AS avg_rpm,
  COALESCE(t.max_rpm, 0)                                          AS max_rpm,
  COALESCE(t.total_high_rpm_seconds, 0.0)                         AS total_high_rpm_seconds,
  COALESCE(t.high_rpm_minutes_per_day, 0.0)                       AS high_rpm_minutes_per_day,

  -- Group T6: Telemetry-side speed
  COALESCE(t.avg_telemetry_speed_kmh, 0.0)                        AS avg_telemetry_speed_kmh,
  COALESCE(t.max_telemetry_speed_kmh, 0)                          AS max_telemetry_speed_kmh,

  -- Group T7: Fuel (alternative measure)
  COALESCE(t.total_fuel_used_archive, 0.0)                        AS total_fuel_used_archive

FROM marts.v_ml_features_driver_behavior base
LEFT JOIN marts.mart_device_monthly_telemetry t
  ON  t.tenant_id  = base.tenant_id
  AND t.device_id  = base.device_id
  AND t.year_month = base.year_month;
