-- =====================================================================
--  02_data_profiling.sql
--  Per-column descriptive statistics on the principal source tables
-- =====================================================================

-- staging.path
SELECT
    COUNT(*)                             AS row_count,
    COUNT(DISTINCT tenant_id)            AS distinct_tenants,
    COUNT(DISTINCT device_id)            AS distinct_devices,
    MIN(begin_path_time)                 AS min_event_time,
    MAX(begin_path_time)                 AS max_event_time,
    AVG(distance_driven)::numeric(10,2)  AS avg_distance,
    PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY distance_driven) AS p05_distance,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY distance_driven) AS p50_distance,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY distance_driven) AS p95_distance,
    AVG(max_speed)::numeric(10,2)        AS avg_max_speed,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY max_speed) AS p95_max_speed,
    AVG(path_duration)::numeric(10,2)    AS avg_duration,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY path_duration) AS p95_duration
FROM staging.path;

-- staging.archive (telemetry)
SELECT
    COUNT(*)                                   AS row_count,
    COUNT(DISTINCT device_id)                  AS distinct_devices,
    MIN(date)                                  AS min_date,
    MAX(date)                                  AS max_date,
    AVG(speed)::numeric(10,2)                  AS avg_speed,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY speed) AS p95_speed,
    AVG(rpm)::numeric(10,2)                    AS avg_rpm,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY rpm) AS p95_rpm,
    SUM(CASE WHEN ignition = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0) AS ignition_on_ratio
FROM staging.archive;

-- staging.rep_overspeed
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT device_id) AS distinct_devices,
    MIN(begin_path_time) AS min_event,
    MAX(begin_path_time) AS max_event,
    SUM(CASE WHEN severity = 'low' THEN 1 ELSE 0 END) AS sev_low,
    SUM(CASE WHEN severity = 'medium' THEN 1 ELSE 0 END) AS sev_medium,
    SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) AS sev_high,
    SUM(CASE WHEN severity = 'extreme' THEN 1 ELSE 0 END) AS sev_extreme
FROM staging.rep_overspeed;

-- staging.notification (filtered to SPEED%)
SELECT
    COUNT(*) AS row_count,
    COUNT(*) FILTER (WHERE category LIKE 'SPEED%') AS speed_rows,
    MIN(created_at) AS min_event,
    MAX(created_at) AS max_event
FROM staging.notification;
