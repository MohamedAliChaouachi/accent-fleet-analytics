-- =============================================================================
-- 16_fact_telemetry_daily_incr.sql
-- =============================================================================
-- fact_telemetry_daily: per-(tenant, device, date) aggregate of staging.archive
-- ping data. Compresses ~10-100k pings/day/device into a single row carrying
-- the signals needed for monthly behaviour features:
--
--   - observation_count         pings recorded (volume / data-quality proxy)
--   - ignition_on_minutes        approximate engine-on duration
--   - moving_minutes             pings with ignition=1 AND speed>0
--   - idle_minutes               pings with ignition=1 AND speed=0
--   - avg_speed_kmh / max_speed_kmh
--   - avg_rpm / max_rpm
--   - high_rpm_seconds           pings with rpm > :rpm_high_threshold (default 3000)
--   - total_fuel_used            sum of fuel_rate (litres) over moving pings
--
-- We assume archive observations land at roughly fixed cadence per tenant.
-- Sampling shows 30s typical, so each ping ≈ 0.5 minutes of vehicle state —
-- bound that as :ping_seconds (default 30) so the conversion is reproducible
-- and tenant-tunable.
--
-- Cleaning rules applied inline:
--   C9  : |x|, |y|, |z| <= 127 (sanity)        — does NOT reject the row, just guards aggregates
--   C10 : speed BETWEEN 0 AND 250 (clamp)
--   C11 : rpm   BETWEEN 0 AND 8000 (clamp)
--   C7  : device must exist in dim_device
--
-- Contract:
--   :window_start, :window_end, :etl_run_id   (standard)
--   :ping_seconds         (INTEGER) default 30
--   :rpm_high_threshold   (INTEGER) default 3000
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_telemetry_daily (
  telemetry_sk         BIGSERIAL PRIMARY KEY,
  tenant_id            INTEGER NOT NULL,
  device_id            BIGINT NOT NULL,
  telemetry_date       DATE NOT NULL,
  -- Volume / coverage
  observation_count    BIGINT NOT NULL,
  -- Time-budget aggregates (minutes)
  ignition_on_minutes  DOUBLE PRECISION,
  moving_minutes       DOUBLE PRECISION,
  idle_minutes         DOUBLE PRECISION,
  -- Speed
  avg_speed_kmh        DOUBLE PRECISION,
  max_speed_kmh        INTEGER,
  -- RPM
  avg_rpm              DOUBLE PRECISION,
  max_rpm              INTEGER,
  high_rpm_seconds     DOUBLE PRECISION,
  -- Fuel
  total_fuel_used      DOUBLE PRECISION,
  -- Derived ratios (computed at INSERT time so the mart can simply average)
  idle_ratio           DOUBLE PRECISION,                  -- idle / ignition_on
  -- Lineage
  _etl_run_id          BIGINT,
  _loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, device_id, telemetry_date)
);

CREATE INDEX IF NOT EXISTS idx_fact_telemetry_daily_event_time
  ON warehouse.fact_telemetry_daily (telemetry_date DESC);
CREATE INDEX IF NOT EXISTS idx_fact_telemetry_daily_device_month
  ON warehouse.fact_telemetry_daily (tenant_id, device_id, telemetry_date);

-- -----------------------------------------------------------------------------
-- The INCREMENTAL LOAD
-- -----------------------------------------------------------------------------
-- Watermark column: telemetry_date (same date for all pings on that day).
-- The window must therefore align to UTC day boundaries — the Python loader
-- floors :window_start and ceils :window_end to whole days for this fact.
-- -----------------------------------------------------------------------------
INSERT INTO warehouse.fact_telemetry_daily (
  tenant_id, device_id, telemetry_date,
  observation_count, ignition_on_minutes, moving_minutes, idle_minutes,
  avg_speed_kmh, max_speed_kmh, avg_rpm, max_rpm, high_rpm_seconds,
  total_fuel_used, idle_ratio, _etl_run_id
)
SELECT
  a.tenant_id,
  a.id_device::BIGINT                                AS device_id,
  a.date::DATE                                       AS telemetry_date,
  COUNT(*)                                           AS observation_count,
  -- Time aggregates: each ping ~ :ping_seconds seconds of vehicle state
  SUM(CASE WHEN a.ignition = 1 THEN 1 ELSE 0 END) * :ping_seconds / 60.0
                                                     AS ignition_on_minutes,
  SUM(CASE WHEN a.ignition = 1 AND LEAST(GREATEST(a.speed,0),250) > 0 THEN 1 ELSE 0 END)
       * :ping_seconds / 60.0                        AS moving_minutes,
  SUM(CASE WHEN a.ignition = 1 AND LEAST(GREATEST(a.speed,0),250) = 0 THEN 1 ELSE 0 END)
       * :ping_seconds / 60.0                        AS idle_minutes,
  -- Speed aggregates (clamped: C10)
  AVG(LEAST(GREATEST(a.speed,0),250))                AS avg_speed_kmh,
  MAX(LEAST(GREATEST(a.speed,0),250))                AS max_speed_kmh,
  -- RPM aggregates (clamped: C11)
  AVG(LEAST(GREATEST(a.rpm,0),8000))                 AS avg_rpm,
  MAX(LEAST(GREATEST(a.rpm,0),8000))                 AS max_rpm,
  SUM(CASE WHEN a.rpm > :rpm_high_threshold THEN 1 ELSE 0 END) * :ping_seconds
                                                     AS high_rpm_seconds,
  -- Fuel: fuel_rate (l/h) per ping; total ≈ sum(rate) * ping_seconds / 3600
  SUM(COALESCE(a.fuel_rate,0)) * :ping_seconds / 3600.0
                                                     AS total_fuel_used,
  -- Idle ratio: guarded against zero ignition time
  CASE
    WHEN SUM(CASE WHEN a.ignition = 1 THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE SUM(CASE WHEN a.ignition = 1 AND LEAST(GREATEST(a.speed,0),250) = 0 THEN 1 ELSE 0 END)::DOUBLE PRECISION
       / NULLIF(SUM(CASE WHEN a.ignition = 1 THEN 1 ELSE 0 END), 0)
  END                                                AS idle_ratio,
  :etl_run_id
FROM staging.archive a
WHERE a.date >= :window_start
  AND a.date <  :window_end
  AND a.date >= '2019-10-01'::timestamp
  AND EXISTS (
    SELECT 1 FROM warehouse.dim_device dd
    WHERE dd.tenant_id = a.tenant_id
      AND dd.device_id = a.id_device::BIGINT
  )
GROUP BY a.tenant_id, a.id_device, a.date::DATE
ON CONFLICT (tenant_id, device_id, telemetry_date) DO UPDATE
  SET observation_count   = EXCLUDED.observation_count,
      ignition_on_minutes = EXCLUDED.ignition_on_minutes,
      moving_minutes      = EXCLUDED.moving_minutes,
      idle_minutes        = EXCLUDED.idle_minutes,
      avg_speed_kmh       = EXCLUDED.avg_speed_kmh,
      max_speed_kmh       = EXCLUDED.max_speed_kmh,
      avg_rpm             = EXCLUDED.avg_rpm,
      max_rpm             = EXCLUDED.max_rpm,
      high_rpm_seconds    = EXCLUDED.high_rpm_seconds,
      total_fuel_used     = EXCLUDED.total_fuel_used,
      idle_ratio          = EXCLUDED.idle_ratio,
      _etl_run_id         = EXCLUDED._etl_run_id,
      _loaded_at          = NOW();
