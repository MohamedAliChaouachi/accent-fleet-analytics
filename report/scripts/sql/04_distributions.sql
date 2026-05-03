-- =====================================================================
--  04_distributions.sql
--  Histograms (binned counts) for the principal numerical variables
-- =====================================================================

-- Distribution of trip distance (km) over 20 bins, range [0, 1000]
SELECT
    width_bucket(distance_driven, 0, 1000, 20) AS bin,
    COUNT(*)                                   AS n,
    MIN(distance_driven)                       AS bin_min,
    MAX(distance_driven)                       AS bin_max
FROM staging.path
WHERE distance_driven > 0
GROUP BY bin
ORDER BY bin;

-- Distribution of max speed (km/h) over 20 bins, range [0, 200]
SELECT
    width_bucket(max_speed, 0, 200, 20) AS bin,
    COUNT(*) AS n
FROM staging.path
WHERE max_speed > 0
GROUP BY bin
ORDER BY bin;

-- Distribution of trip duration (minutes) over 30 bins, range [0, 1800]
SELECT
    width_bucket(path_duration / 60.0, 0, 1800, 30) AS bin,
    COUNT(*) AS n
FROM staging.path
WHERE path_duration > 0
GROUP BY bin
ORDER BY bin;

-- Distribution of overspeed events per device per month
WITH per_device_month AS (
    SELECT tenant_id, device_id, date_trunc('month', begin_path_time) AS month, COUNT(*) AS n_events
    FROM staging.rep_overspeed
    GROUP BY tenant_id, device_id, date_trunc('month', begin_path_time)
)
SELECT
    width_bucket(n_events, 0, 200, 20) AS bin,
    COUNT(*) AS device_months
FROM per_device_month
GROUP BY bin
ORDER BY bin;

-- Distribution of risk score in marts.v_device_risk_profile
SELECT
    width_bucket(risk_score, 0, 1, 20) AS bin,
    COUNT(*) AS device_months
FROM marts.v_device_risk_profile
GROUP BY bin
ORDER BY bin;
