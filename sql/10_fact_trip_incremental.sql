-- =============================================================================
-- 10_fact_trip_incremental.sql
-- =============================================================================
-- fact_trip: the core trip fact. Applies cleaning rules C1, C2, C3, C4, C5
-- at load time. INCREMENTAL via the :window_start / :window_end parameters
-- that the Python loader binds before execution.
--
-- Contract:
--   Input parameters:
--     :window_start  (TIMESTAMP)  - event-time lower bound (inclusive)
--     :window_end    (TIMESTAMP)  - event-time upper bound (exclusive)
--     :etl_run_id    (BIGINT)     - from etl_run_log.run_id
--
--   Output:
--     Rows merged into warehouse.fact_trip.
--     Max(begin_path_time) processed returned for watermark advancement.
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_trip (
  trip_sk              BIGSERIAL PRIMARY KEY,
  tenant_id            INTEGER NOT NULL,
  device_id            BIGINT NOT NULL,
  trip_date            DATE NOT NULL,
  begin_path_time      TIMESTAMP NOT NULL,
  end_path_time        TIMESTAMP,
  duration_seconds     BIGINT,
  distance_km          DOUBLE PRECISION,
  max_speed_kmh        INTEGER,
  avg_speed_kmh        DOUBLE PRECISION,            -- computed
  speed_ratio          DOUBLE PRECISION,            -- max / avg, proxy for aggressiveness
  fuel_used            DOUBLE PRECISION,            -- NULL if C4 nullified
  start_odo            DOUBLE PRECISION,
  end_odo              DOUBLE PRECISION,
  start_lat            DOUBLE PRECISION,
  start_lon            DOUBLE PRECISION,
  end_lat              DOUBLE PRECISION,
  end_lon              DOUBLE PRECISION,
  -- Derived flags
  is_night_trip        BOOLEAN,
  is_weekend_trip      BOOLEAN,
  is_rush_hour_trip    BOOLEAN,
  is_short_trip        BOOLEAN,                     -- distance < 1 km
  is_long_trip         BOOLEAN,                     -- distance > 100 km
  -- Lineage
  _etl_run_id          BIGINT,
  _loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Natural key enforces idempotency under re-runs
  UNIQUE (tenant_id, device_id, begin_path_time)
);

-- Indexes: one for incremental scans by event-time, one per typical analytic query.
CREATE INDEX IF NOT EXISTS idx_fact_trip_event_time
  ON warehouse.fact_trip (begin_path_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_trip_device_month
  ON warehouse.fact_trip (tenant_id, device_id, trip_date);

-- -----------------------------------------------------------------------------
-- The INCREMENTAL LOAD
-- -----------------------------------------------------------------------------
-- Cleaning rules applied inline:
--   C1: begin_path_time >= '2019-10-01'
--   C2: path_duration > 0
--   C3: distance_driven > 0
--   C4: fuel_used nullified if out of [0, 500]
--   C5: max_speed clamped at 200
-- -----------------------------------------------------------------------------
INSERT INTO warehouse.fact_trip (
  tenant_id, device_id, trip_date, begin_path_time, end_path_time,
  duration_seconds, distance_km, max_speed_kmh, avg_speed_kmh, speed_ratio,
  fuel_used, start_odo, end_odo, start_lat, start_lon, end_lat, end_lon,
  is_night_trip, is_weekend_trip, is_rush_hour_trip, is_short_trip, is_long_trip,
  _etl_run_id
)
SELECT
  p.tenant_id,
  p.device_id,
  p.begin_path_time::DATE                               AS trip_date,
  p.begin_path_time,
  p.end_path_time,
  p.path_duration                                        AS duration_seconds,
  p.distance_driven                                      AS distance_km,
  LEAST(p.max_speed, 200)                                AS max_speed_kmh,   -- C5 clamp
  -- avg_speed_kmh = distance / (duration / 3600). NULL if duration is 0.
  CASE WHEN p.path_duration > 0
       THEN p.distance_driven / (p.path_duration / 3600.0)
       ELSE NULL END                                     AS avg_speed_kmh,
  -- speed_ratio: guard against divide-by-zero.
  CASE WHEN p.path_duration > 0
        AND p.distance_driven > 0
        AND (p.distance_driven / (p.path_duration / 3600.0)) > 0
       THEN LEAST(p.max_speed, 200)
          / (p.distance_driven / (p.path_duration / 3600.0))
       ELSE NULL END                                     AS speed_ratio,
  -- C4 nullify out-of-range fuel
  CASE WHEN p.fuel_used BETWEEN 0 AND 500 THEN p.fuel_used ELSE NULL END
                                                         AS fuel_used,
  p.start_odo, p.end_odo,
  p.begin_path_latitude, p.begin_path_longitude,
  p.end_path_latitude,   p.end_path_longitude,
  -- Derived temporal flags
  (EXTRACT(HOUR FROM p.begin_path_time) >= 21
    OR EXTRACT(HOUR FROM p.begin_path_time) <= 5)        AS is_night_trip,
  EXTRACT(DOW FROM p.begin_path_time) IN (0, 6)          AS is_weekend_trip,
  EXTRACT(HOUR FROM p.begin_path_time) IN (7, 8, 17, 18) AS is_rush_hour_trip,
  p.distance_driven < 1.0                                AS is_short_trip,
  p.distance_driven > 100.0                              AS is_long_trip,
  :etl_run_id
FROM staging.path p
WHERE p.begin_path_time >= :window_start                 -- incremental window
  AND p.begin_path_time <  :window_end
  AND p.begin_path_time >= '2019-10-01'::timestamp       -- C1
  AND p.path_duration  >  0                              -- C2
  AND p.distance_driven > 0                              -- C3
  AND EXISTS (
    -- C7: only devices with a clean dim linkage
    SELECT 1 FROM warehouse.dim_device dd
    WHERE dd.tenant_id = p.tenant_id AND dd.device_id = p.device_id
  )
ON CONFLICT (tenant_id, device_id, begin_path_time) DO UPDATE
  SET end_path_time     = EXCLUDED.end_path_time,
      duration_seconds  = EXCLUDED.duration_seconds,
      distance_km       = EXCLUDED.distance_km,
      max_speed_kmh     = EXCLUDED.max_speed_kmh,
      avg_speed_kmh     = EXCLUDED.avg_speed_kmh,
      speed_ratio       = EXCLUDED.speed_ratio,
      fuel_used         = EXCLUDED.fuel_used,
      start_odo         = EXCLUDED.start_odo,
      end_odo           = EXCLUDED.end_odo,
      _etl_run_id       = EXCLUDED._etl_run_id,
      _loaded_at        = NOW();
