-- =============================================================================
-- 20_mart_device_monthly_behavior.sql
-- =============================================================================
-- The central feature table for ML. One row per (tenant, device, year_month)
-- with 35+ engineered features. Incremental semantics: the Python loader
-- determines which (tenant, device, year_month) tuples were touched by the
-- latest fact load and passes them as :touched_months. Only those rows
-- are recomputed.
--
-- Parameter:
--   :touched_months  TEXT[]   e.g. ARRAY['2026-03','2026-04']
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.mart_device_monthly_behavior (
  tenant_id                     INTEGER NOT NULL,
  device_id                     BIGINT NOT NULL,
  year_month                    CHAR(7) NOT NULL,

  -- Group 1: Trip Volume & Patterns
  total_trips                   INTEGER NOT NULL DEFAULT 0,
  total_distance_km             DOUBLE PRECISION NOT NULL DEFAULT 0,
  avg_trip_distance_km          DOUBLE PRECISION,
  avg_trip_duration_minutes     DOUBLE PRECISION,
  avg_fuel_used_l               DOUBLE PRECISION,
  stddev_trip_distance          DOUBLE PRECISION,
  short_trip_ratio              DOUBLE PRECISION,

  -- Group 2: Speed Behavior
  avg_max_speed_kmh             DOUBLE PRECISION,
  p95_max_speed                 DOUBLE PRECISION,
  avg_speed_ratio               DOUBLE PRECISION,
  high_speed_trip_ratio         DOUBLE PRECISION,

  -- Group 3: Overspeed
  overspeed_count               INTEGER NOT NULL DEFAULT 0,
  overspeed_per_100km           DOUBLE PRECISION,
  overspeed_per_trip            DOUBLE PRECISION,
  overspeed_severity_low        INTEGER NOT NULL DEFAULT 0,
  overspeed_severity_medium     INTEGER NOT NULL DEFAULT 0,
  overspeed_severity_high       INTEGER NOT NULL DEFAULT 0,
  overspeed_severity_extreme    INTEGER NOT NULL DEFAULT 0,
  avg_speed_over_limit          DOUBLE PRECISION,

  -- Group 4: Alerts
  speed_alert_count             INTEGER NOT NULL DEFAULT 0,
  speed_alert_per_100km         DOUBLE PRECISION,
  highway_alert_count           INTEGER NOT NULL DEFAULT 0,
  non_highway_alert_count       INTEGER NOT NULL DEFAULT 0,

  -- Group 5: Stops & Idling
  total_stops                   INTEGER NOT NULL DEFAULT 0,
  in_path_stop_ratio            DOUBLE PRECISION,
  stops_per_trip                DOUBLE PRECISION,
  micro_stop_count              INTEGER NOT NULL DEFAULT 0,
  short_stop_count              INTEGER NOT NULL DEFAULT 0,
  medium_stop_count             INTEGER NOT NULL DEFAULT 0,
  long_stop_count               INTEGER NOT NULL DEFAULT 0,

  -- Group 6: Temporal
  night_trip_ratio              DOUBLE PRECISION,
  weekend_trip_ratio            DOUBLE PRECISION,
  rush_hour_trip_ratio          DOUBLE PRECISION,
  active_days                   INTEGER NOT NULL DEFAULT 0,
  avg_working_hours             DOUBLE PRECISION,

  -- Lineage
  _etl_run_id                   BIGINT,
  _computed_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (tenant_id, device_id, year_month)
);

CREATE INDEX IF NOT EXISTS idx_mart_device_monthly_ym
  ON marts.mart_device_monthly_behavior (year_month);
CREATE INDEX IF NOT EXISTS idx_mart_device_monthly_tenant
  ON marts.mart_device_monthly_behavior (tenant_id, year_month);

-- -----------------------------------------------------------------------------
-- The recompute: a single MERGE-style INSERT...ON CONFLICT covering all 35
-- features. The CTE chain keeps each feature group's logic visually separable.
-- -----------------------------------------------------------------------------

WITH
target_months AS (
  -- Either a specific list passed in, or "all months currently represented".
  -- For backfill the loader passes NULL and we derive from data.
  SELECT DISTINCT TO_CHAR(begin_path_time, 'YYYY-MM') AS year_month
  FROM warehouse.fact_trip
  WHERE (CAST(:touched_months AS text[]) IS NULL
         OR TO_CHAR(begin_path_time, 'YYYY-MM') = ANY(CAST(:touched_months AS text[])))
),

-- Group 1 + 2 + 6a: everything from fact_trip
trip_agg AS (
  SELECT
    ft.tenant_id,
    ft.device_id,
    TO_CHAR(ft.begin_path_time, 'YYYY-MM')                    AS year_month,
    COUNT(*)::INTEGER                                          AS total_trips,
    SUM(ft.distance_km)                                        AS total_distance_km,
    AVG(ft.distance_km)                                        AS avg_trip_distance_km,
    AVG(ft.duration_seconds) / 60.0                            AS avg_trip_duration_minutes,
    AVG(ft.fuel_used)                                          AS avg_fuel_used_l,
    STDDEV_SAMP(ft.distance_km)                                AS stddev_trip_distance,
    AVG(CASE WHEN ft.is_short_trip THEN 1.0 ELSE 0.0 END)      AS short_trip_ratio,
    AVG(ft.max_speed_kmh)                                      AS avg_max_speed_kmh,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ft.max_speed_kmh) AS p95_max_speed,
    AVG(ft.speed_ratio)                                        AS avg_speed_ratio,
    AVG(CASE WHEN ft.max_speed_kmh > 100 THEN 1.0 ELSE 0.0 END) AS high_speed_trip_ratio,
    AVG(CASE WHEN ft.is_night_trip THEN 1.0 ELSE 0.0 END)       AS night_trip_ratio,
    AVG(CASE WHEN ft.is_weekend_trip THEN 1.0 ELSE 0.0 END)     AS weekend_trip_ratio,
    AVG(CASE WHEN ft.is_rush_hour_trip THEN 1.0 ELSE 0.0 END)   AS rush_hour_trip_ratio,
    COUNT(DISTINCT ft.trip_date)::INTEGER                       AS active_days
  FROM warehouse.fact_trip ft
  JOIN target_months tm ON TO_CHAR(ft.begin_path_time, 'YYYY-MM') = tm.year_month
  GROUP BY ft.tenant_id, ft.device_id, TO_CHAR(ft.begin_path_time, 'YYYY-MM')
),

-- Group 3: overspeed
overspeed_agg AS (
  SELECT
    fo.tenant_id, fo.device_id,
    TO_CHAR(fo.begin_path_time, 'YYYY-MM')    AS year_month,
    COUNT(*)::INTEGER                          AS overspeed_count,
    COUNT(*) FILTER (WHERE fo.severity = 'low')::INTEGER     AS overspeed_severity_low,
    COUNT(*) FILTER (WHERE fo.severity = 'medium')::INTEGER  AS overspeed_severity_medium,
    COUNT(*) FILTER (WHERE fo.severity = 'high')::INTEGER    AS overspeed_severity_high,
    COUNT(*) FILTER (WHERE fo.severity = 'extreme')::INTEGER AS overspeed_severity_extreme,
    AVG(fo.speed_over_limit)                   AS avg_speed_over_limit
  FROM warehouse.fact_overspeed fo
  JOIN target_months tm ON TO_CHAR(fo.begin_path_time, 'YYYY-MM') = tm.year_month
  GROUP BY fo.tenant_id, fo.device_id, TO_CHAR(fo.begin_path_time, 'YYYY-MM')
),

-- Group 4: alerts
alert_agg AS (
  SELECT
    fn.tenant_id, fn.device_id,
    TO_CHAR(fn.created_at, 'YYYY-MM')          AS year_month,
    COUNT(*)::INTEGER                           AS speed_alert_count,
    COUNT(*) FILTER (WHERE fn.alert_type = 'SPEED_HIGHWAY')::INTEGER     AS highway_alert_count,
    COUNT(*) FILTER (WHERE fn.alert_type = 'SPEED_NOT_HIGHWAY')::INTEGER AS non_highway_alert_count
  FROM warehouse.fact_speed_notification fn
  JOIN target_months tm ON TO_CHAR(fn.created_at, 'YYYY-MM') = tm.year_month
  GROUP BY fn.tenant_id, fn.device_id, TO_CHAR(fn.created_at, 'YYYY-MM')
),

-- Group 5: stops
stop_agg AS (
  SELECT
    fs.tenant_id, fs.device_id,
    TO_CHAR(fs.stop_start, 'YYYY-MM')          AS year_month,
    COUNT(*)::INTEGER                           AS total_stops,
    AVG(CASE WHEN fs.is_in_path THEN 1.0 ELSE 0.0 END) AS in_path_stop_ratio,
    COUNT(*) FILTER (WHERE fs.stop_type = 'micro')::INTEGER  AS micro_stop_count,
    COUNT(*) FILTER (WHERE fs.stop_type = 'short')::INTEGER  AS short_stop_count,
    COUNT(*) FILTER (WHERE fs.stop_type = 'medium')::INTEGER AS medium_stop_count,
    COUNT(*) FILTER (WHERE fs.stop_type = 'long')::INTEGER   AS long_stop_count
  FROM warehouse.fact_stop fs
  JOIN target_months tm ON TO_CHAR(fs.stop_start, 'YYYY-MM') = tm.year_month
  GROUP BY fs.tenant_id, fs.device_id, TO_CHAR(fs.stop_start, 'YYYY-MM')
),

-- Group 6b: daily activity
activity_agg AS (
  SELECT
    fda.tenant_id, fda.device_id,
    TO_CHAR(fda.activity_start_time, 'YYYY-MM') AS year_month,
    AVG(fda.working_hours)                      AS avg_working_hours
  FROM warehouse.fact_daily_activity fda
  JOIN target_months tm ON TO_CHAR(fda.activity_start_time, 'YYYY-MM') = tm.year_month
  GROUP BY fda.tenant_id, fda.device_id, TO_CHAR(fda.activity_start_time, 'YYYY-MM')
)

INSERT INTO marts.mart_device_monthly_behavior (
  tenant_id, device_id, year_month,
  total_trips, total_distance_km, avg_trip_distance_km, avg_trip_duration_minutes, avg_fuel_used_l,
  stddev_trip_distance, short_trip_ratio,
  avg_max_speed_kmh, p95_max_speed, avg_speed_ratio, high_speed_trip_ratio,
  overspeed_count, overspeed_per_100km, overspeed_per_trip,
  overspeed_severity_low, overspeed_severity_medium, overspeed_severity_high, overspeed_severity_extreme,
  avg_speed_over_limit,
  speed_alert_count, speed_alert_per_100km, highway_alert_count, non_highway_alert_count,
  total_stops, in_path_stop_ratio, stops_per_trip,
  micro_stop_count, short_stop_count, medium_stop_count, long_stop_count,
  night_trip_ratio, weekend_trip_ratio, rush_hour_trip_ratio, active_days, avg_working_hours,
  _etl_run_id
)
SELECT
  t.tenant_id, t.device_id, t.year_month,
  t.total_trips, t.total_distance_km, t.avg_trip_distance_km, t.avg_trip_duration_minutes,
  t.avg_fuel_used_l, t.stddev_trip_distance, t.short_trip_ratio,
  t.avg_max_speed_kmh, t.p95_max_speed, t.avg_speed_ratio, t.high_speed_trip_ratio,
  COALESCE(o.overspeed_count, 0),
  CASE WHEN t.total_distance_km > 0 THEN COALESCE(o.overspeed_count, 0) / t.total_distance_km * 100 END,
  CASE WHEN t.total_trips > 0 THEN COALESCE(o.overspeed_count, 0)::DOUBLE PRECISION / t.total_trips END,
  COALESCE(o.overspeed_severity_low, 0),
  COALESCE(o.overspeed_severity_medium, 0),
  COALESCE(o.overspeed_severity_high, 0),
  COALESCE(o.overspeed_severity_extreme, 0),
  o.avg_speed_over_limit,
  COALESCE(a.speed_alert_count, 0),
  CASE WHEN t.total_distance_km > 0 THEN COALESCE(a.speed_alert_count, 0) / t.total_distance_km * 100 END,
  COALESCE(a.highway_alert_count, 0),
  COALESCE(a.non_highway_alert_count, 0),
  COALESCE(s.total_stops, 0), s.in_path_stop_ratio,
  CASE WHEN t.total_trips > 0 THEN COALESCE(s.total_stops, 0)::DOUBLE PRECISION / t.total_trips END,
  COALESCE(s.micro_stop_count, 0),
  COALESCE(s.short_stop_count, 0),
  COALESCE(s.medium_stop_count, 0),
  COALESCE(s.long_stop_count, 0),
  t.night_trip_ratio, t.weekend_trip_ratio, t.rush_hour_trip_ratio, t.active_days,
  act.avg_working_hours,
  :etl_run_id
FROM trip_agg t
LEFT JOIN overspeed_agg o USING (tenant_id, device_id, year_month)
LEFT JOIN alert_agg     a USING (tenant_id, device_id, year_month)
LEFT JOIN stop_agg      s USING (tenant_id, device_id, year_month)
LEFT JOIN activity_agg  act USING (tenant_id, device_id, year_month)

ON CONFLICT (tenant_id, device_id, year_month) DO UPDATE SET
  total_trips                = EXCLUDED.total_trips,
  total_distance_km          = EXCLUDED.total_distance_km,
  avg_trip_distance_km       = EXCLUDED.avg_trip_distance_km,
  avg_trip_duration_minutes  = EXCLUDED.avg_trip_duration_minutes,
  avg_fuel_used_l            = EXCLUDED.avg_fuel_used_l,
  stddev_trip_distance       = EXCLUDED.stddev_trip_distance,
  short_trip_ratio           = EXCLUDED.short_trip_ratio,
  avg_max_speed_kmh          = EXCLUDED.avg_max_speed_kmh,
  p95_max_speed              = EXCLUDED.p95_max_speed,
  avg_speed_ratio            = EXCLUDED.avg_speed_ratio,
  high_speed_trip_ratio      = EXCLUDED.high_speed_trip_ratio,
  overspeed_count            = EXCLUDED.overspeed_count,
  overspeed_per_100km        = EXCLUDED.overspeed_per_100km,
  overspeed_per_trip         = EXCLUDED.overspeed_per_trip,
  overspeed_severity_low     = EXCLUDED.overspeed_severity_low,
  overspeed_severity_medium  = EXCLUDED.overspeed_severity_medium,
  overspeed_severity_high    = EXCLUDED.overspeed_severity_high,
  overspeed_severity_extreme = EXCLUDED.overspeed_severity_extreme,
  avg_speed_over_limit       = EXCLUDED.avg_speed_over_limit,
  speed_alert_count          = EXCLUDED.speed_alert_count,
  speed_alert_per_100km      = EXCLUDED.speed_alert_per_100km,
  highway_alert_count        = EXCLUDED.highway_alert_count,
  non_highway_alert_count    = EXCLUDED.non_highway_alert_count,
  total_stops                = EXCLUDED.total_stops,
  in_path_stop_ratio         = EXCLUDED.in_path_stop_ratio,
  stops_per_trip             = EXCLUDED.stops_per_trip,
  micro_stop_count           = EXCLUDED.micro_stop_count,
  short_stop_count           = EXCLUDED.short_stop_count,
  medium_stop_count          = EXCLUDED.medium_stop_count,
  long_stop_count            = EXCLUDED.long_stop_count,
  night_trip_ratio           = EXCLUDED.night_trip_ratio,
  weekend_trip_ratio         = EXCLUDED.weekend_trip_ratio,
  rush_hour_trip_ratio       = EXCLUDED.rush_hour_trip_ratio,
  active_days                = EXCLUDED.active_days,
  avg_working_hours          = EXCLUDED.avg_working_hours,
  _etl_run_id                = EXCLUDED._etl_run_id,
  _computed_at               = NOW();
