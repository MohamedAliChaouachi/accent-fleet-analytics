-- =============================================================================
-- 27_fact_trip_reconstruct_telemetry_last_month.sql
-- =============================================================================
-- Reconstruct trip-like records from staging.archive telemetry for one tenant
-- and one month. Intended for tenant 7486, whose telemetry exists while
-- staging.path/fact_trip is empty.
--
-- Session rule:
--   - source pings are ignition-on rows from staging.archive
--   - a new trip starts after a gap larger than :max_gap_minutes
--   - GPS distance is the sum of valid haversine segments, filtered by
--     :max_segment_speed_kmh to reject coordinate jumps
--   - only sessions with enough duration, movement, and distance are inserted
--
-- Parameters:
--   :tenant_id
--   :month_start                 nullable timestamp; default = latest archive month
--   :month_end                   nullable timestamp; default = month_start + 1 month
--   :etl_run_id
--   :max_gap_minutes
--   :min_duration_seconds
--   :min_distance_km
--   :max_segment_speed_kmh
--   :ping_seconds
-- =============================================================================

ALTER TABLE warehouse.fact_trip
  ADD COLUMN IF NOT EXISTS trip_source TEXT NOT NULL DEFAULT 'path';

ALTER TABLE warehouse.fact_trip
  ADD COLUMN IF NOT EXISTS reconstruction_method TEXT;

ALTER TABLE warehouse.fact_trip
  ADD COLUMN IF NOT EXISTS source_ping_count BIGINT;

ALTER TABLE warehouse.fact_trip
  ADD COLUMN IF NOT EXISTS moving_ping_count BIGINT;

ALTER TABLE warehouse.fact_trip
  ADD COLUMN IF NOT EXISTS valid_gps_ping_count BIGINT;

ALTER TABLE warehouse.fact_trip
  ADD COLUMN IF NOT EXISTS source_month CHAR(7);

CREATE TABLE IF NOT EXISTS warehouse.fact_trip_reconstruction_audit (
  audit_id                 BIGSERIAL PRIMARY KEY,
  tenant_id                INTEGER NOT NULL,
  source_month             CHAR(7) NOT NULL,
  month_start              TIMESTAMP NOT NULL,
  month_end                TIMESTAMP NOT NULL,
  reconstruction_method    TEXT NOT NULL,
  reconstructed_trips      INTEGER NOT NULL,
  reconstructed_devices    INTEGER NOT NULL,
  total_distance_km        DOUBLE PRECISION NOT NULL,
  total_duration_seconds   BIGINT NOT NULL,
  _etl_run_id              BIGINT,
  _loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, source_month, reconstruction_method)
);

WITH
latest_month AS (
  SELECT date_trunc('month', MAX(a.date))::timestamp AS month_start
  FROM staging.archive a
  WHERE a.tenant_id = CAST(:tenant_id AS integer)
),
cfg AS (
  SELECT
    CAST(:tenant_id AS integer)                                     AS tenant_id,
    COALESCE(CAST(:month_start AS timestamp), lm.month_start)       AS month_start,
    COALESCE(CAST(:month_end AS timestamp),
             COALESCE(CAST(:month_start AS timestamp), lm.month_start)
               + INTERVAL '1 month')                                AS month_end,
    CAST(:etl_run_id AS bigint)                                     AS etl_run_id,
    CAST(:max_gap_minutes AS integer)                               AS max_gap_minutes,
    CAST(:min_duration_seconds AS integer)                          AS min_duration_seconds,
    CAST(:min_distance_km AS double precision)                      AS min_distance_km,
    CAST(:max_segment_speed_kmh AS double precision)                AS max_segment_speed_kmh,
    CAST(:ping_seconds AS integer)                                  AS ping_seconds
  FROM latest_month lm
),
raw AS (
  SELECT
    a.tenant_id,
    a.id_device::BIGINT                                             AS device_id,
    a.date::timestamp                                               AS ts,
    LEAST(GREATEST(COALESCE(a.speed, 0), 0), 250)::INTEGER          AS speed_kmh,
    COALESCE(a.fuel_rate, 0)::DOUBLE PRECISION                      AS fuel_rate,
    CASE
      WHEN a.latitude BETWEEN -90 AND 90
       AND a.longitude BETWEEN -180 AND 180
       AND NOT (a.latitude = 0 AND a.longitude = 0)
      THEN a.latitude::DOUBLE PRECISION
      ELSE NULL
    END                                                             AS latitude,
    CASE
      WHEN a.latitude BETWEEN -90 AND 90
       AND a.longitude BETWEEN -180 AND 180
       AND NOT (a.latitude = 0 AND a.longitude = 0)
      THEN a.longitude::DOUBLE PRECISION
      ELSE NULL
    END                                                             AS longitude,
    c.month_start,
    c.month_end,
    c.etl_run_id,
    c.max_gap_minutes,
    c.min_duration_seconds,
    c.min_distance_km,
    c.max_segment_speed_kmh,
    c.ping_seconds
  FROM staging.archive a
  CROSS JOIN cfg c
  WHERE a.tenant_id = c.tenant_id
    AND a.date >= c.month_start
    AND a.date <  c.month_end
    AND a.ignition = 1
    AND EXISTS (
      SELECT 1
      FROM warehouse.dim_device dd
      WHERE dd.tenant_id = a.tenant_id
        AND dd.device_id = a.id_device::BIGINT
    )
),
ordered AS (
  SELECT
    r.*,
    LAG(r.ts)        OVER w AS prev_ts,
    LAG(r.latitude)  OVER w AS prev_latitude,
    LAG(r.longitude) OVER w AS prev_longitude
  FROM raw r
  WINDOW w AS (PARTITION BY r.tenant_id, r.device_id ORDER BY r.ts)
),
flagged AS (
  SELECT
    o.*,
    CASE
      WHEN o.prev_ts IS NULL THEN 1
      WHEN EXTRACT(EPOCH FROM (o.ts - o.prev_ts)) > o.max_gap_minutes * 60 THEN 1
      ELSE 0
    END AS new_session
  FROM ordered o
),
sessionized AS (
  SELECT
    f.*,
    SUM(f.new_session) OVER (
      PARTITION BY f.tenant_id, f.device_id
      ORDER BY f.ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_id
  FROM flagged f
),
segment_calc AS (
  SELECT
    s.*,
    CASE
      WHEN s.new_session = 0
      THEN EXTRACT(EPOCH FROM (s.ts - s.prev_ts))
      ELSE NULL
    END AS segment_seconds,
    CASE
      WHEN s.new_session = 0
       AND s.latitude IS NOT NULL
       AND s.longitude IS NOT NULL
       AND s.prev_latitude IS NOT NULL
       AND s.prev_longitude IS NOT NULL
      THEN 2 * 6371.0088 * ASIN(SQRT(LEAST(1.0,
        POWER(SIN(RADIANS(s.latitude - s.prev_latitude) / 2), 2)
        + COS(RADIANS(s.prev_latitude)) * COS(RADIANS(s.latitude))
          * POWER(SIN(RADIANS(s.longitude - s.prev_longitude) / 2), 2)
      )))
      ELSE NULL
    END AS raw_segment_km
  FROM sessionized s
),
segments AS (
  SELECT
    sc.*,
    CASE
      WHEN sc.segment_seconds > 0
       AND sc.segment_seconds <= sc.max_gap_minutes * 60
       AND sc.raw_segment_km <= sc.max_segment_speed_kmh * sc.segment_seconds / 3600.0
      THEN sc.raw_segment_km
      ELSE 0
    END AS segment_km
  FROM segment_calc sc
),
trip_rollup AS (
  SELECT
    s.tenant_id,
    s.device_id,
    s.session_id,
    MIN(s.ts)                                                     AS begin_path_time,
    MAX(s.ts)                                                     AS end_path_time,
    COUNT(*)::BIGINT                                              AS source_ping_count,
    COUNT(*) FILTER (WHERE s.speed_kmh > 0)::BIGINT               AS moving_ping_count,
    COUNT(*) FILTER (WHERE s.latitude IS NOT NULL
                      AND s.longitude IS NOT NULL)::BIGINT        AS valid_gps_ping_count,
    SUM(s.segment_km)                                              AS distance_km,
    LEAST(MAX(s.speed_kmh), 200)::INTEGER                         AS max_speed_kmh,
    SUM(COALESCE(s.fuel_rate, 0)) * MAX(s.ping_seconds) / 3600.0  AS fuel_used,
    MAX(s.etl_run_id)                                             AS etl_run_id,
    MAX(s.month_start)                                            AS month_start,
    MAX(s.month_end)                                              AS month_end
  FROM segments s
  GROUP BY s.tenant_id, s.device_id, s.session_id
),
start_points AS (
  SELECT DISTINCT ON (tenant_id, device_id, session_id)
    tenant_id, device_id, session_id,
    latitude AS start_lat,
    longitude AS start_lon
  FROM segments
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL
  ORDER BY tenant_id, device_id, session_id, ts ASC
),
end_points AS (
  SELECT DISTINCT ON (tenant_id, device_id, session_id)
    tenant_id, device_id, session_id,
    latitude AS end_lat,
    longitude AS end_lon
  FROM segments
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL
  ORDER BY tenant_id, device_id, session_id, ts DESC
),
trips AS (
  SELECT
    r.tenant_id,
    r.device_id,
    r.begin_path_time::DATE                                      AS trip_date,
    r.begin_path_time,
    r.end_path_time,
    EXTRACT(EPOCH FROM (r.end_path_time - r.begin_path_time))::BIGINT
                                                                  AS duration_seconds,
    r.distance_km,
    r.max_speed_kmh,
    CASE
      WHEN EXTRACT(EPOCH FROM (r.end_path_time - r.begin_path_time)) > 0
      THEN r.distance_km / (EXTRACT(EPOCH FROM (r.end_path_time - r.begin_path_time)) / 3600.0)
      ELSE NULL
    END                                                           AS avg_speed_kmh,
    r.fuel_used,
    sp.start_lat,
    sp.start_lon,
    ep.end_lat,
    ep.end_lon,
    r.source_ping_count,
    r.moving_ping_count,
    r.valid_gps_ping_count,
    TO_CHAR(r.month_start, 'YYYY-MM')                             AS source_month,
    r.etl_run_id,
    r.month_start,
    r.month_end
  FROM trip_rollup r
  JOIN start_points sp USING (tenant_id, device_id, session_id)
  JOIN end_points ep USING (tenant_id, device_id, session_id)
  CROSS JOIN cfg c
  WHERE EXTRACT(EPOCH FROM (r.end_path_time - r.begin_path_time)) >= c.min_duration_seconds
    AND r.distance_km >= c.min_distance_km
    AND r.moving_ping_count >= 2
    AND r.valid_gps_ping_count >= 2
)
INSERT INTO warehouse.fact_trip (
  tenant_id, device_id, trip_date, begin_path_time, end_path_time,
  duration_seconds, distance_km, max_speed_kmh, avg_speed_kmh, speed_ratio,
  fuel_used, start_odo, end_odo, start_lat, start_lon, end_lat, end_lon,
  is_night_trip, is_weekend_trip, is_rush_hour_trip, is_short_trip, is_long_trip,
  trip_source, reconstruction_method, source_ping_count, moving_ping_count,
  valid_gps_ping_count, source_month, _etl_run_id
)
SELECT
  t.tenant_id,
  t.device_id,
  t.trip_date,
  t.begin_path_time,
  t.end_path_time,
  t.duration_seconds,
  t.distance_km,
  t.max_speed_kmh,
  t.avg_speed_kmh,
  CASE WHEN t.avg_speed_kmh > 0 THEN t.max_speed_kmh / t.avg_speed_kmh ELSE NULL END,
  CASE WHEN t.fuel_used BETWEEN 0 AND 500 THEN t.fuel_used ELSE NULL END,
  NULL::DOUBLE PRECISION,
  NULL::DOUBLE PRECISION,
  t.start_lat,
  t.start_lon,
  t.end_lat,
  t.end_lon,
  (EXTRACT(HOUR FROM t.begin_path_time) >= 21
    OR EXTRACT(HOUR FROM t.begin_path_time) <= 5),
  EXTRACT(DOW FROM t.begin_path_time) IN (0, 6),
  EXTRACT(HOUR FROM t.begin_path_time) IN (7, 8, 17, 18),
  t.distance_km < 1.0,
  t.distance_km > 100.0,
  'telemetry_reconstructed',
  'ignition_gap_haversine_v1',
  t.source_ping_count,
  t.moving_ping_count,
  t.valid_gps_ping_count,
  t.source_month,
  t.etl_run_id
FROM trips t
ON CONFLICT (tenant_id, device_id, begin_path_time) DO UPDATE
  SET end_path_time          = EXCLUDED.end_path_time,
      trip_date              = EXCLUDED.trip_date,
      duration_seconds       = EXCLUDED.duration_seconds,
      distance_km            = EXCLUDED.distance_km,
      max_speed_kmh          = EXCLUDED.max_speed_kmh,
      avg_speed_kmh          = EXCLUDED.avg_speed_kmh,
      speed_ratio            = EXCLUDED.speed_ratio,
      fuel_used              = EXCLUDED.fuel_used,
      start_lat              = EXCLUDED.start_lat,
      start_lon              = EXCLUDED.start_lon,
      end_lat                = EXCLUDED.end_lat,
      end_lon                = EXCLUDED.end_lon,
      is_night_trip          = EXCLUDED.is_night_trip,
      is_weekend_trip        = EXCLUDED.is_weekend_trip,
      is_rush_hour_trip      = EXCLUDED.is_rush_hour_trip,
      is_short_trip          = EXCLUDED.is_short_trip,
      is_long_trip           = EXCLUDED.is_long_trip,
      reconstruction_method  = EXCLUDED.reconstruction_method,
      source_ping_count      = EXCLUDED.source_ping_count,
      moving_ping_count      = EXCLUDED.moving_ping_count,
      valid_gps_ping_count   = EXCLUDED.valid_gps_ping_count,
      source_month           = EXCLUDED.source_month,
      _etl_run_id            = EXCLUDED._etl_run_id,
      _loaded_at             = NOW()
  WHERE warehouse.fact_trip.trip_source = 'telemetry_reconstructed';

WITH
latest_month AS (
  SELECT date_trunc('month', MAX(a.date))::timestamp AS month_start
  FROM staging.archive a
  WHERE a.tenant_id = CAST(:tenant_id AS integer)
),
cfg AS (
  SELECT
    CAST(:tenant_id AS integer)                               AS tenant_id,
    COALESCE(CAST(:month_start AS timestamp), lm.month_start) AS month_start,
    COALESCE(CAST(:month_end AS timestamp),
             COALESCE(CAST(:month_start AS timestamp), lm.month_start)
               + INTERVAL '1 month')                          AS month_end,
    CAST(:etl_run_id AS bigint)                               AS etl_run_id
  FROM latest_month lm
),
summary AS (
  SELECT
    c.tenant_id,
    TO_CHAR(c.month_start, 'YYYY-MM')                         AS source_month,
    c.month_start,
    c.month_end,
    'ignition_gap_haversine_v1'                               AS reconstruction_method,
    COUNT(ft.*)::INTEGER                                      AS reconstructed_trips,
    COUNT(DISTINCT ft.device_id)::INTEGER                     AS reconstructed_devices,
    COALESCE(SUM(ft.distance_km), 0)::DOUBLE PRECISION        AS total_distance_km,
    COALESCE(SUM(ft.duration_seconds), 0)::BIGINT             AS total_duration_seconds,
    c.etl_run_id
  FROM cfg c
  LEFT JOIN warehouse.fact_trip ft
    ON ft.tenant_id = c.tenant_id
   AND ft.trip_source = 'telemetry_reconstructed'
   AND ft.reconstruction_method = 'ignition_gap_haversine_v1'
   AND ft.begin_path_time >= c.month_start
   AND ft.begin_path_time <  c.month_end
  GROUP BY c.tenant_id, c.month_start, c.month_end, c.etl_run_id
)
INSERT INTO warehouse.fact_trip_reconstruction_audit (
  tenant_id, source_month, month_start, month_end, reconstruction_method,
  reconstructed_trips, reconstructed_devices, total_distance_km,
  total_duration_seconds, _etl_run_id
)
SELECT
  tenant_id, source_month, month_start, month_end, reconstruction_method,
  reconstructed_trips, reconstructed_devices, total_distance_km,
  total_duration_seconds, etl_run_id
FROM summary
ON CONFLICT (tenant_id, source_month, reconstruction_method) DO UPDATE
  SET month_start             = EXCLUDED.month_start,
      month_end               = EXCLUDED.month_end,
      reconstructed_trips     = EXCLUDED.reconstructed_trips,
      reconstructed_devices   = EXCLUDED.reconstructed_devices,
      total_distance_km       = EXCLUDED.total_distance_km,
      total_duration_seconds  = EXCLUDED.total_duration_seconds,
      _etl_run_id             = EXCLUDED._etl_run_id,
      _loaded_at              = NOW();

WITH
latest_month AS (
  SELECT date_trunc('month', MAX(a.date))::timestamp AS month_start
  FROM staging.archive a
  WHERE a.tenant_id = CAST(:tenant_id AS integer)
),
cfg AS (
  SELECT
    CAST(:tenant_id AS integer)                               AS tenant_id,
    COALESCE(CAST(:month_start AS timestamp), lm.month_start) AS month_start,
    COALESCE(CAST(:month_end AS timestamp),
             COALESCE(CAST(:month_start AS timestamp), lm.month_start)
               + INTERVAL '1 month')                          AS month_end
  FROM latest_month lm
)
SELECT
  TO_CHAR(c.month_start, 'YYYY-MM')                         AS source_month,
  c.month_start,
  c.month_end,
  COUNT(ft.*)::INTEGER                                      AS reconstructed_trips,
  COUNT(DISTINCT ft.device_id)::INTEGER                     AS reconstructed_devices,
  COALESCE(ROUND(SUM(ft.distance_km)::numeric, 2), 0)       AS total_distance_km,
  COALESCE(ROUND((SUM(ft.duration_seconds) / 3600.0)::numeric, 2), 0)
                                                               AS total_duration_hours
FROM cfg c
LEFT JOIN warehouse.fact_trip ft
  ON ft.tenant_id = c.tenant_id
 AND ft.trip_source = 'telemetry_reconstructed'
 AND ft.reconstruction_method = 'ignition_gap_haversine_v1'
 AND ft.begin_path_time >= c.month_start
 AND ft.begin_path_time <  c.month_end
GROUP BY c.month_start, c.month_end;
