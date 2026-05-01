-- =============================================================================
-- 15_fact_harsh_event_incremental.sql
-- =============================================================================
-- fact_harsh_event: discrete harsh-driving events derived from staging.archive
-- accelerometer telemetry (x, y, z). One row per detected event.
--
-- Detection rule (per Project 1 spec — Driver Behavior Scoring & Risk
-- Classification): an "event" is a single archive observation whose absolute
-- accelerometer reading on one axis exceeds a calibrated threshold AND whose
-- ignition is on. Severity is bucketed (moderate/high/extreme) so downstream
-- ML can either count totals or weight by severity.
--
-- Axis convention used here (validated by sampling production data):
--   x  -> longitudinal: positive = acceleration, negative = braking
--   y  -> lateral:      |y| = cornering force
--   z  -> vertical:     used only for impact / pothole signals (NOT scored)
--
-- Default thresholds (overridable via :thresh_* parameters bound by the
-- Python loader). Values are in raw int8 units of the device — the device
-- ships ±2g full-scale, so 40 ≈ 0.31 g, 60 ≈ 0.47 g, 80 ≈ 0.63 g.
--
-- Cleaning rules applied inline:
--   C8 : ignition = 1 (events while parked are sensor noise)
--   C9 : |x|, |y|, |z| <= 127 (int8 sanity, drops corrupted rows)
--   C7 : device must exist in dim_device
--
-- Contract:
--   Input parameters:
--     :window_start  (TIMESTAMP)
--     :window_end    (TIMESTAMP)
--     :etl_run_id    (BIGINT)
--     :thresh_brake  (INTEGER)   default 40   (negative-x threshold magnitude)
--     :thresh_accel  (INTEGER)   default 40
--     :thresh_corner (INTEGER)   default 40
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_harsh_event (
  harsh_event_sk     BIGSERIAL PRIMARY KEY,
  tenant_id          INTEGER NOT NULL,
  device_id          BIGINT NOT NULL,
  event_time         TIMESTAMP NOT NULL,
  event_date         DATE NOT NULL,
  event_type         TEXT NOT NULL,                   -- harsh_brake | harsh_accel | harsh_corner
  severity           TEXT NOT NULL,                   -- moderate | high | extreme
  x_axis             INTEGER,                         -- longitudinal accel
  y_axis             INTEGER,                         -- lateral accel
  z_axis             INTEGER,                         -- vertical accel
  speed_kmh          INTEGER,                         -- speed at the moment of event
  rpm                INTEGER,
  latitude           DOUBLE PRECISION,
  longitude          DOUBLE PRECISION,
  -- Lineage
  _etl_run_id        BIGINT,
  _loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Natural key: an archive row may trigger up to one event per axis-type,
  -- so we include event_type to allow simultaneous brake+corner from the
  -- same observation (rare but possible — heavy braking into a turn).
  UNIQUE (tenant_id, device_id, event_time, event_type)
);

-- Index DDL needs table ownership in PostgreSQL. The loader role may only have
-- DML privileges on an already-provisioned warehouse, so create these optional
-- indexes only when this session owns the table.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'warehouse'
      AND c.relname = 'fact_harsh_event'
      AND c.relkind IN ('r', 'p')
      AND pg_get_userbyid(c.relowner) = current_user
  ) THEN
    CREATE INDEX IF NOT EXISTS idx_fact_harsh_event_event_time
      ON warehouse.fact_harsh_event (event_time DESC);
    CREATE INDEX IF NOT EXISTS idx_fact_harsh_event_device_month
      ON warehouse.fact_harsh_event (tenant_id, device_id, event_date);
    CREATE INDEX IF NOT EXISTS idx_fact_harsh_event_type_sev
      ON warehouse.fact_harsh_event (event_type, severity);
  END IF;
END $$;

-- -----------------------------------------------------------------------------
-- The INCREMENTAL LOAD
-- -----------------------------------------------------------------------------
-- We unpivot via UNION ALL: each axis-type that crosses its threshold
-- emits one row. Severity is computed as a tier of the absolute reading.
-- -----------------------------------------------------------------------------
INSERT INTO warehouse.fact_harsh_event (
  tenant_id, device_id, event_time, event_date, event_type, severity,
  x_axis, y_axis, z_axis, speed_kmh, rpm, latitude, longitude, _etl_run_id
)
WITH src AS (
  SELECT
    a.tenant_id,
    a.id_device::BIGINT AS device_id,
    a.date              AS event_time,
    a.x, a.y, a.z,
    a.speed,
    a.rpm,
    a.latitude,
    a.longitude
  FROM staging.archive a
  WHERE a.date >= :window_start
    AND a.date <  :window_end
    AND a.date >= '2019-10-01'::timestamp
    AND a.ignition = 1                                   -- C8
    AND ABS(a.x) <= 127 AND ABS(a.y) <= 127 AND ABS(a.z) <= 127   -- C9
    AND EXISTS (
      SELECT 1 FROM warehouse.dim_device dd
      WHERE dd.tenant_id = a.tenant_id
        AND dd.device_id = a.id_device::BIGINT             -- C7
    )
)
-- Harsh braking: large negative x
SELECT tenant_id, device_id, event_time, event_time::DATE,
       'harsh_brake' AS event_type,
       CASE
         WHEN -x >= :thresh_extreme THEN 'extreme'
         WHEN -x >= :thresh_high    THEN 'high'
         ELSE                            'moderate'
       END AS severity,
       x, y, z, speed, rpm, latitude, longitude, :etl_run_id
FROM src
WHERE x <= -:thresh_brake

UNION ALL
-- Harsh acceleration: large positive x
SELECT tenant_id, device_id, event_time, event_time::DATE,
       'harsh_accel' AS event_type,
       CASE
         WHEN  x >= :thresh_extreme THEN 'extreme'
         WHEN  x >= :thresh_high    THEN 'high'
         ELSE                            'moderate'
       END AS severity,
       x, y, z, speed, rpm, latitude, longitude, :etl_run_id
FROM src
WHERE x >= :thresh_accel

UNION ALL
-- Harsh cornering: large |y|
SELECT tenant_id, device_id, event_time, event_time::DATE,
       'harsh_corner' AS event_type,
       CASE
         WHEN ABS(y) >= :thresh_extreme THEN 'extreme'
         WHEN ABS(y) >= :thresh_high    THEN 'high'
         ELSE                                'moderate'
       END AS severity,
       x, y, z, speed, rpm, latitude, longitude, :etl_run_id
FROM src
WHERE ABS(y) >= :thresh_corner

ON CONFLICT (tenant_id, device_id, event_time, event_type) DO UPDATE
  SET severity   = EXCLUDED.severity,
      x_axis     = EXCLUDED.x_axis,
      y_axis     = EXCLUDED.y_axis,
      z_axis     = EXCLUDED.z_axis,
      speed_kmh  = EXCLUDED.speed_kmh,
      rpm        = EXCLUDED.rpm,
      latitude   = EXCLUDED.latitude,
      longitude  = EXCLUDED.longitude,
      _etl_run_id= EXCLUDED._etl_run_id,
      _loaded_at = NOW();
