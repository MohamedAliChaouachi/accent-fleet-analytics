-- =============================================================================
-- 14_fact_daily_activity_incr.sql
-- =============================================================================
-- Daily activity aggregate. working_time converted ms -> sec, distance
-- derived from odometer delta.
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_daily_activity (
  activity_sk         BIGSERIAL PRIMARY KEY,
  tenant_id           INTEGER NOT NULL,
  device_id           BIGINT NOT NULL,
  activity_date       DATE NOT NULL,
  activity_start_time TIMESTAMP NOT NULL,
  activity_end_time   TIMESTAMP,
  start_odo           DOUBLE PRECISION,
  end_odo             DOUBLE PRECISION,
  distance_km         DOUBLE PRECISION,            -- derived from odo delta
  working_time_sec    DOUBLE PRECISION,            -- converted from ms
  working_hours       DOUBLE PRECISION,            -- seconds / 3600
  _etl_run_id         BIGINT,
  _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, device_id, activity_start_time)
);

CREATE INDEX IF NOT EXISTS idx_fact_daily_activity_event_time
  ON warehouse.fact_daily_activity (activity_start_time DESC);

INSERT INTO warehouse.fact_daily_activity (
  tenant_id, device_id, activity_date, activity_start_time, activity_end_time,
  start_odo, end_odo, distance_km, working_time_sec, working_hours, _etl_run_id
)
SELECT
  a.tenant_id,
  a.device_id::BIGINT,
  a.activity_start_time::DATE                       AS activity_date,
  a.activity_start_time,
  a.activity_end_time,
  a.activity_start_odo,
  a.activity_end_odo,
  CASE WHEN a.activity_end_odo >= a.activity_start_odo
       THEN a.activity_end_odo - a.activity_start_odo
       ELSE NULL END                                 AS distance_km,
  a.working_time / 1000.0                            AS working_time_sec,
  a.working_time / 1000.0 / 3600.0                   AS working_hours,
  :etl_run_id
FROM staging.rep_activity_daily a
WHERE a.activity_start_time >= :window_start
  AND a.activity_start_time <  :window_end
  AND a.activity_start_time >= '2019-10-01'::timestamp
  AND EXISTS (
    SELECT 1 FROM warehouse.dim_device dd
    WHERE dd.tenant_id = a.tenant_id AND dd.device_id = a.device_id::BIGINT
  )
ON CONFLICT (tenant_id, device_id, activity_start_time) DO UPDATE
  SET activity_end_time = EXCLUDED.activity_end_time,
      start_odo         = EXCLUDED.start_odo,
      end_odo           = EXCLUDED.end_odo,
      distance_km       = EXCLUDED.distance_km,
      working_time_sec  = EXCLUDED.working_time_sec,
      working_hours     = EXCLUDED.working_hours,
      _etl_run_id       = EXCLUDED._etl_run_id,
      _loaded_at        = NOW();
