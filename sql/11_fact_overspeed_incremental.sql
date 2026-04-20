-- =============================================================================
-- 11_fact_overspeed_incremental.sql
-- =============================================================================
-- Overspeed events. Severity derived from max_speed thresholds.
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_overspeed (
  overspeed_sk     BIGSERIAL PRIMARY KEY,
  tenant_id        INTEGER NOT NULL,
  device_id        BIGINT NOT NULL,
  begin_path_time  TIMESTAMP NOT NULL,
  end_path_time    TIMESTAMP,
  max_speed_kmh    INTEGER,
  duration_seconds BIGINT,
  distance_km      DOUBLE PRECISION,
  vehicle_limit    INTEGER,
  speed_over_limit INTEGER,
  severity         TEXT,                          -- low | medium | high | extreme
  _etl_run_id      BIGINT,
  _loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, device_id, begin_path_time)
);

CREATE INDEX IF NOT EXISTS idx_fact_overspeed_event_time
  ON warehouse.fact_overspeed (begin_path_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_overspeed_device
  ON warehouse.fact_overspeed (tenant_id, device_id, begin_path_time);

INSERT INTO warehouse.fact_overspeed (
  tenant_id, device_id, begin_path_time, end_path_time,
  max_speed_kmh, duration_seconds, distance_km,
  vehicle_limit, speed_over_limit, severity, _etl_run_id
)
SELECT
  o.tenant_id,
  o.device_id,
  o.begin_path_time,
  o.end_path_time,
  LEAST(o.max_speed, 200)                AS max_speed_kmh,    -- C5
  o.path_duration                         AS duration_seconds,
  o.distance_driven                       AS distance_km,
  dv.max_speed_setting                    AS vehicle_limit,
  CASE WHEN dv.max_speed_setting IS NOT NULL
       THEN GREATEST(0, LEAST(o.max_speed, 200) - dv.max_speed_setting)
       ELSE NULL END                      AS speed_over_limit,
  CASE
    WHEN LEAST(o.max_speed, 200) < 100 THEN 'low'
    WHEN LEAST(o.max_speed, 200) < 130 THEN 'medium'
    WHEN LEAST(o.max_speed, 200) < 160 THEN 'high'
    ELSE                                     'extreme'
  END                                     AS severity,
  :etl_run_id
FROM staging.rep_overspeed o
LEFT JOIN warehouse.dim_device dd
  ON dd.tenant_id = o.tenant_id AND dd.device_id = o.device_id
LEFT JOIN warehouse.dim_vehicle dv
  ON dv.vehicle_sk = dd.vehicle_sk
WHERE o.begin_path_time >= :window_start
  AND o.begin_path_time <  :window_end
  AND o.begin_path_time >= '2019-10-01'::timestamp
  AND dd.device_id IS NOT NULL                              -- C7
ON CONFLICT (tenant_id, device_id, begin_path_time) DO UPDATE
  SET end_path_time    = EXCLUDED.end_path_time,
      max_speed_kmh    = EXCLUDED.max_speed_kmh,
      duration_seconds = EXCLUDED.duration_seconds,
      distance_km      = EXCLUDED.distance_km,
      vehicle_limit    = EXCLUDED.vehicle_limit,
      speed_over_limit = EXCLUDED.speed_over_limit,
      severity         = EXCLUDED.severity,
      _etl_run_id      = EXCLUDED._etl_run_id,
      _loaded_at       = NOW();
