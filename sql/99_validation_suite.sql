-- =============================================================================
-- 99_validation_suite.sql
-- =============================================================================
-- Post-run validation. Each query returns a single row with a pass/fail flag
-- and a numeric value. The Python monitoring module runs these and records
-- results to etl_run_log.
-- =============================================================================

-- V1: dim row counts are non-zero
SELECT 'V1_dims_populated' AS check_name,
       (SELECT COUNT(*) FROM warehouse.dim_tenant)   > 0 AND
       (SELECT COUNT(*) FROM warehouse.dim_vehicle)  > 0 AND
       (SELECT COUNT(*) FROM warehouse.dim_device)   > 0 AND
       (SELECT COUNT(*) FROM warehouse.dim_date)     > 0 AS passed,
       (SELECT COUNT(*) FROM warehouse.dim_device)   AS devices_count;

-- V2: every fact_trip row has a matching dim_device (referential integrity)
SELECT 'V2_fact_trip_ref_integrity' AS check_name,
       COUNT(*) = 0 AS passed,
       COUNT(*)   AS orphan_count
FROM warehouse.fact_trip ft
LEFT JOIN warehouse.dim_device dd
  ON dd.tenant_id = ft.tenant_id AND dd.device_id = ft.device_id
WHERE dd.device_id IS NULL;

-- V3: no fact_trip dates before 2019-10-01 (C1 enforcement)
SELECT 'V3_fact_trip_C1_enforced' AS check_name,
       COUNT(*) = 0 AS passed,
       COUNT(*)   AS bad_rows
FROM warehouse.fact_trip
WHERE begin_path_time < '2019-10-01'::timestamp;

-- V4: no negative or zero durations / distances (C2, C3)
SELECT 'V4_fact_trip_C2C3_enforced' AS check_name,
       COUNT(*) = 0 AS passed,
       COUNT(*)   AS bad_rows
FROM warehouse.fact_trip
WHERE duration_seconds <= 0 OR distance_km <= 0;

-- V5: no speed > 200 (C5)
SELECT 'V5_fact_trip_C5_enforced' AS check_name,
       COUNT(*) = 0 AS passed,
       COUNT(*)   AS bad_rows
FROM warehouse.fact_trip
WHERE max_speed_kmh > 200;

-- V6: null rate on critical columns within threshold (< 1 %)
SELECT 'V6_fact_trip_null_rates' AS check_name,
       (AVG(CASE WHEN distance_km  IS NULL THEN 1.0 ELSE 0.0 END) < 0.01)
   AND (AVG(CASE WHEN max_speed_kmh IS NULL THEN 1.0 ELSE 0.0 END) < 0.01)
       AS passed,
       AVG(CASE WHEN distance_km   IS NULL THEN 1.0 ELSE 0.0 END) AS null_rate_distance,
       AVG(CASE WHEN max_speed_kmh IS NULL THEN 1.0 ELSE 0.0 END) AS null_rate_speed
FROM warehouse.fact_trip;

-- V7: mart row count matches expected grain (device, month) coverage
SELECT 'V7_mart_grain' AS check_name,
       COUNT(*) > 0 AS passed,
       COUNT(*)     AS mart_rows,
       COUNT(DISTINCT tenant_id) AS tenants,
       COUNT(DISTINCT (tenant_id, device_id)) AS devices
FROM marts.mart_device_monthly_behavior;

-- V8: risk-score distribution sanity
SELECT 'V8_risk_distribution' AS check_name,
       (SELECT COUNT(*) FROM marts.v_device_risk_profile) > 0 AS passed,
       COUNT(*) FILTER (WHERE risk_category = 'critical') AS critical_cnt,
       COUNT(*) FILTER (WHERE risk_category = 'high')     AS high_cnt,
       COUNT(*) FILTER (WHERE risk_category = 'moderate') AS moderate_cnt,
       COUNT(*) FILTER (WHERE risk_category = 'low')      AS low_cnt
FROM marts.v_device_risk_profile;
