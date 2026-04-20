-- =============================================================================
-- 12_fact_stop_incremental.sql
-- =============================================================================
-- Stops. Applies C1 (temporal) and C6 (duration bounds). Derives stop_type
-- from duration bucket.
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_stop (
  stop_sk           BIGSERIAL PRIMARY KEY,
  tenant_id         INTEGER NOT NULL,
  device_id         BIGINT NOT NULL,
  stop_start        TIMESTAMP NOT NULL,
  stop_end          TIMESTAMP,
  duration_seconds  BIGINT,
  is_in_path        BOOLEAN,
  stop_lat          DOUBLE PRECISION,
  stop_lon          DOUBLE PRECISION,
  stop_address      TEXT,
  poi_name          TEXT,
  stop_type         TEXT,                            -- micro | short | medium | long | parked
  _etl_run_id       BIGINT,
  _loaded_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, device_id, stop_start)
);

CREATE INDEX IF NOT EXISTS idx_fact_stop_event_time
  ON warehouse.fact_stop (stop_start DESC);
CREATE INDEX IF NOT EXISTS idx_fact_stop_device
  ON warehouse.fact_stop (tenant_id, device_id, stop_start);

INSERT INTO warehouse.fact_stop (
  tenant_id, device_id, stop_start, stop_end, duration_seconds,
  is_in_path, stop_lat, stop_lon, stop_address, poi_name, stop_type, _etl_run_id
)
SELECT
  s.tenant_id,
  s.device_id,
  s.stop_start,
  s.stop_end,
  s.stop_duration                                    AS duration_seconds,
  CASE
    WHEN UPPER(TRIM(s.in_path)) IN ('TRUE', '1', 'T', 'YES') THEN TRUE
    WHEN UPPER(TRIM(s.in_path)) IN ('FALSE', '0', 'F', 'NO') THEN FALSE
    ELSE NULL
  END                                                AS is_in_path,
  s.stop_lat, s.stop_long,
  s.stop_adress                                      AS stop_address,
  s.adress_according_poi                             AS poi_name,
  CASE
    WHEN s.stop_duration < 120         THEN 'micro'    -- < 2 min
    WHEN s.stop_duration < 900         THEN 'short'    -- 2-15 min
    WHEN s.stop_duration < 3600        THEN 'medium'   -- 15-60 min
    WHEN s.stop_duration < 86400       THEN 'long'     -- 1-24 h
    ELSE                                    'parked'   -- > 24 h
  END                                                AS stop_type,
  :etl_run_id
FROM staging.stop s
WHERE s.stop_start >= :window_start
  AND s.stop_start <  :window_end
  AND s.stop_start >= '2019-10-01'::timestamp        -- C1
  AND s.stop_duration > 0                            -- C6
  AND s.stop_duration < 31536000                     -- C6 (< 1 year)
  AND EXISTS (
    SELECT 1 FROM warehouse.dim_device dd
    WHERE dd.tenant_id = s.tenant_id AND dd.device_id = s.device_id
  )
ON CONFLICT (tenant_id, device_id, stop_start) DO UPDATE
  SET stop_end         = EXCLUDED.stop_end,
      duration_seconds = EXCLUDED.duration_seconds,
      is_in_path       = EXCLUDED.is_in_path,
      stop_lat         = EXCLUDED.stop_lat,
      stop_lon         = EXCLUDED.stop_lon,
      stop_address     = EXCLUDED.stop_address,
      poi_name         = EXCLUDED.poi_name,
      stop_type        = EXCLUDED.stop_type,
      _etl_run_id      = EXCLUDED._etl_run_id,
      _loaded_at       = NOW();
