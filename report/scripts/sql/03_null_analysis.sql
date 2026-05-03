-- =====================================================================
--  03_null_analysis.sql
--  Null-rate report on the principal source tables
--  Output is intended to be plotted as a missing-value heatmap.
-- =====================================================================

-- staging.path null rates
SELECT 'staging.path' AS table_name, 'distance_driven' AS column_name,
       100.0 * SUM(CASE WHEN distance_driven IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS null_pct
FROM staging.path
UNION ALL
SELECT 'staging.path', 'max_speed',
       100.0 * SUM(CASE WHEN max_speed IS NULL THEN 1 ELSE 0 END) / COUNT(*) FROM staging.path
UNION ALL
SELECT 'staging.path', 'path_duration',
       100.0 * SUM(CASE WHEN path_duration IS NULL THEN 1 ELSE 0 END) / COUNT(*) FROM staging.path
UNION ALL
SELECT 'staging.path', 'fuel_used',
       100.0 * SUM(CASE WHEN fuel_used IS NULL THEN 1 ELSE 0 END) / COUNT(*) FROM staging.path

-- staging.archive null rates
UNION ALL
SELECT 'staging.archive', 'speed',
       100.0 * SUM(CASE WHEN speed IS NULL THEN 1 ELSE 0 END) / COUNT(*) FROM staging.archive
UNION ALL
SELECT 'staging.archive', 'rpm',
       100.0 * SUM(CASE WHEN rpm IS NULL THEN 1 ELSE 0 END) / COUNT(*) FROM staging.archive
UNION ALL
SELECT 'staging.archive', 'accel_x',
       100.0 * SUM(CASE WHEN accel_x IS NULL THEN 1 ELSE 0 END) / COUNT(*) FROM staging.archive
UNION ALL
SELECT 'staging.archive', 'ignition',
       100.0 * SUM(CASE WHEN ignition IS NULL THEN 1 ELSE 0 END) / COUNT(*) FROM staging.archive

-- staging.notification null rates
UNION ALL
SELECT 'staging.notification', 'category',
       100.0 * SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) / COUNT(*) FROM staging.notification
UNION ALL
SELECT 'staging.notification', 'created_at',
       100.0 * SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) / COUNT(*) FROM staging.notification

ORDER BY table_name, column_name;
