-- =============================================================================
-- 25_mart_device_monthly_telemetry.sql
-- =============================================================================
-- mart_device_monthly_telemetry: companion to 20_mart_device_monthly_behavior.
-- One row per (tenant, device, year_month) carrying the archive-derived
-- features needed for Project 1 (Driver Behavior Scoring & Risk
-- Classification) — harsh-event counts, idling ratios, RPM signals, and
-- normalized "per-100km" rates.
--
-- Kept SEPARATE from the existing mart (rather than altering it) so the
-- existing tested SQL remains untouched. The unified ML view (sql/26)
-- LEFT JOINs the two on the natural key.
--
-- Parameter:
--   :touched_months  TEXT[]   e.g. ARRAY['2026-03','2026-04']
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.mart_device_monthly_telemetry (
  tenant_id                    INTEGER NOT NULL,
  device_id                    BIGINT NOT NULL,
  year_month                   CHAR(7) NOT NULL,

  -- Group T1: Harsh-event counts (raw)
  harsh_brake_count            INTEGER NOT NULL DEFAULT 0,
  harsh_accel_count            INTEGER NOT NULL DEFAULT 0,
  harsh_corner_count           INTEGER NOT NULL DEFAULT 0,
  harsh_event_total            INTEGER NOT NULL DEFAULT 0,

  -- Group T2: Harsh-event severity breakdown
  harsh_moderate_count         INTEGER NOT NULL DEFAULT 0,
  harsh_high_count             INTEGER NOT NULL DEFAULT 0,
  harsh_extreme_count          INTEGER NOT NULL DEFAULT 0,

  -- Group T3: Normalized rates (need monthly distance from the trip mart;
  -- computed in the unified view, NOT here, to avoid coupling the marts)
  -- → harsh_brake_per_100km, harsh_accel_per_100km, harsh_corner_per_100km
  --   live in sql/26_v_ml_features_full.sql

  -- Group T4: Engine / idling
  total_observation_count      BIGINT NOT NULL DEFAULT 0,
  total_ignition_on_minutes    DOUBLE PRECISION,
  total_moving_minutes         DOUBLE PRECISION,
  total_idle_minutes           DOUBLE PRECISION,
  monthly_idle_ratio           DOUBLE PRECISION,            -- idle / ignition_on
  active_telemetry_days        INTEGER NOT NULL DEFAULT 0,

  -- Group T5: RPM signals
  avg_rpm                      DOUBLE PRECISION,
  max_rpm                      INTEGER,
  total_high_rpm_seconds       DOUBLE PRECISION,
  high_rpm_minutes_per_day     DOUBLE PRECISION,

  -- Group T6: Speed (telemetry-side, complements trip-side avg_max_speed)
  avg_telemetry_speed_kmh      DOUBLE PRECISION,
  max_telemetry_speed_kmh      INTEGER,

  -- Group T7: Fuel
  total_fuel_used_archive      DOUBLE PRECISION,            -- alt to fact_trip's fuel_used

  -- Lineage
  _etl_run_id                  BIGINT,
  _computed_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (tenant_id, device_id, year_month)
);

CREATE INDEX IF NOT EXISTS idx_mart_telemetry_ym
  ON marts.mart_device_monthly_telemetry (year_month);
CREATE INDEX IF NOT EXISTS idx_mart_telemetry_tenant
  ON marts.mart_device_monthly_telemetry (tenant_id, year_month);

-- -----------------------------------------------------------------------------
-- Recompute. CTE pulls from the two new fact tables; FULL OUTER JOIN so a
-- device that had only telemetry pings (no harsh events) still gets a row.
-- -----------------------------------------------------------------------------
WITH
target_months AS (
  SELECT DISTINCT TO_CHAR(telemetry_date, 'YYYY-MM') AS year_month
  FROM warehouse.fact_telemetry_daily
  WHERE (CAST(:touched_months AS text[]) IS NULL
         OR TO_CHAR(telemetry_date, 'YYYY-MM') = ANY(CAST(:touched_months AS text[])))

  UNION

  SELECT DISTINCT TO_CHAR(event_date, 'YYYY-MM') AS year_month
  FROM warehouse.fact_harsh_event
  WHERE (CAST(:touched_months AS text[]) IS NULL
         OR TO_CHAR(event_date, 'YYYY-MM') = ANY(CAST(:touched_months AS text[])))
),

harsh_agg AS (
  SELECT
    fhe.tenant_id, fhe.device_id,
    TO_CHAR(fhe.event_date, 'YYYY-MM')                                         AS year_month,
    COUNT(*) FILTER (WHERE event_type = 'harsh_brake')::INTEGER                AS harsh_brake_count,
    COUNT(*) FILTER (WHERE event_type = 'harsh_accel')::INTEGER                AS harsh_accel_count,
    COUNT(*) FILTER (WHERE event_type = 'harsh_corner')::INTEGER               AS harsh_corner_count,
    COUNT(*)::INTEGER                                                          AS harsh_event_total,
    COUNT(*) FILTER (WHERE severity = 'moderate')::INTEGER                     AS harsh_moderate_count,
    COUNT(*) FILTER (WHERE severity = 'high')::INTEGER                         AS harsh_high_count,
    COUNT(*) FILTER (WHERE severity = 'extreme')::INTEGER                      AS harsh_extreme_count
  FROM warehouse.fact_harsh_event fhe
  JOIN target_months tm ON TO_CHAR(fhe.event_date, 'YYYY-MM') = tm.year_month
  GROUP BY fhe.tenant_id, fhe.device_id, TO_CHAR(fhe.event_date, 'YYYY-MM')
),

telemetry_agg AS (
  SELECT
    ftd.tenant_id, ftd.device_id,
    TO_CHAR(ftd.telemetry_date, 'YYYY-MM')                                     AS year_month,
    SUM(ftd.observation_count)                                                 AS total_observation_count,
    SUM(ftd.ignition_on_minutes)                                               AS total_ignition_on_minutes,
    SUM(ftd.moving_minutes)                                                    AS total_moving_minutes,
    SUM(ftd.idle_minutes)                                                      AS total_idle_minutes,
    -- Monthly ratio: SUM(idle) / SUM(ignition_on) to weight by day length
    CASE WHEN SUM(ftd.ignition_on_minutes) > 0
         THEN SUM(ftd.idle_minutes) / SUM(ftd.ignition_on_minutes)
         ELSE NULL END                                                         AS monthly_idle_ratio,
    COUNT(DISTINCT ftd.telemetry_date)::INTEGER                                AS active_telemetry_days,
    -- RPM
    AVG(ftd.avg_rpm)                                                           AS avg_rpm,
    MAX(ftd.max_rpm)                                                           AS max_rpm,
    SUM(ftd.high_rpm_seconds)                                                  AS total_high_rpm_seconds,
    CASE WHEN COUNT(DISTINCT ftd.telemetry_date) > 0
         THEN SUM(ftd.high_rpm_seconds) / 60.0 / COUNT(DISTINCT ftd.telemetry_date)
         ELSE NULL END                                                         AS high_rpm_minutes_per_day,
    -- Speed
    AVG(ftd.avg_speed_kmh)                                                     AS avg_telemetry_speed_kmh,
    MAX(ftd.max_speed_kmh)                                                     AS max_telemetry_speed_kmh,
    -- Fuel
    SUM(ftd.total_fuel_used)                                                   AS total_fuel_used_archive
  FROM warehouse.fact_telemetry_daily ftd
  JOIN target_months tm ON TO_CHAR(ftd.telemetry_date, 'YYYY-MM') = tm.year_month
  GROUP BY ftd.tenant_id, ftd.device_id, TO_CHAR(ftd.telemetry_date, 'YYYY-MM')
)

INSERT INTO marts.mart_device_monthly_telemetry (
  tenant_id, device_id, year_month,
  harsh_brake_count, harsh_accel_count, harsh_corner_count, harsh_event_total,
  harsh_moderate_count, harsh_high_count, harsh_extreme_count,
  total_observation_count, total_ignition_on_minutes, total_moving_minutes, total_idle_minutes,
  monthly_idle_ratio, active_telemetry_days,
  avg_rpm, max_rpm, total_high_rpm_seconds, high_rpm_minutes_per_day,
  avg_telemetry_speed_kmh, max_telemetry_speed_kmh, total_fuel_used_archive,
  _etl_run_id
)
SELECT
  COALESCE(t.tenant_id, h.tenant_id),
  COALESCE(t.device_id, h.device_id),
  COALESCE(t.year_month, h.year_month),
  COALESCE(h.harsh_brake_count, 0),
  COALESCE(h.harsh_accel_count, 0),
  COALESCE(h.harsh_corner_count, 0),
  COALESCE(h.harsh_event_total, 0),
  COALESCE(h.harsh_moderate_count, 0),
  COALESCE(h.harsh_high_count, 0),
  COALESCE(h.harsh_extreme_count, 0),
  COALESCE(t.total_observation_count, 0),
  t.total_ignition_on_minutes,
  t.total_moving_minutes,
  t.total_idle_minutes,
  t.monthly_idle_ratio,
  COALESCE(t.active_telemetry_days, 0),
  t.avg_rpm,
  t.max_rpm,
  t.total_high_rpm_seconds,
  t.high_rpm_minutes_per_day,
  t.avg_telemetry_speed_kmh,
  t.max_telemetry_speed_kmh,
  t.total_fuel_used_archive,
  :etl_run_id
FROM telemetry_agg t
FULL OUTER JOIN harsh_agg h
  ON  t.tenant_id  = h.tenant_id
  AND t.device_id  = h.device_id
  AND t.year_month = h.year_month

ON CONFLICT (tenant_id, device_id, year_month) DO UPDATE SET
  harsh_brake_count            = EXCLUDED.harsh_brake_count,
  harsh_accel_count            = EXCLUDED.harsh_accel_count,
  harsh_corner_count           = EXCLUDED.harsh_corner_count,
  harsh_event_total            = EXCLUDED.harsh_event_total,
  harsh_moderate_count         = EXCLUDED.harsh_moderate_count,
  harsh_high_count             = EXCLUDED.harsh_high_count,
  harsh_extreme_count          = EXCLUDED.harsh_extreme_count,
  total_observation_count      = EXCLUDED.total_observation_count,
  total_ignition_on_minutes    = EXCLUDED.total_ignition_on_minutes,
  total_moving_minutes         = EXCLUDED.total_moving_minutes,
  total_idle_minutes           = EXCLUDED.total_idle_minutes,
  monthly_idle_ratio           = EXCLUDED.monthly_idle_ratio,
  active_telemetry_days        = EXCLUDED.active_telemetry_days,
  avg_rpm                      = EXCLUDED.avg_rpm,
  max_rpm                      = EXCLUDED.max_rpm,
  total_high_rpm_seconds       = EXCLUDED.total_high_rpm_seconds,
  high_rpm_minutes_per_day     = EXCLUDED.high_rpm_minutes_per_day,
  avg_telemetry_speed_kmh      = EXCLUDED.avg_telemetry_speed_kmh,
  max_telemetry_speed_kmh      = EXCLUDED.max_telemetry_speed_kmh,
  total_fuel_used_archive      = EXCLUDED.total_fuel_used_archive,
  _etl_run_id                  = EXCLUDED._etl_run_id,
  _computed_at                 = NOW();
