-- =============================================================================
-- 42_v_real_time_alerts_stream.sql
-- =============================================================================
-- Live operations alert feed (v2.0). Unifies the last 24 hours of
-- notifications, overspeed events, and harsh events into a single
-- chronologically-sorted stream the ops console polls every ~30s.
--
-- Sources:
--   fact_notification    → speed/geofence/maintenance/fuel-theft alerts
--   fact_overspeed       → recent speed-band events (last 24h)
--   fact_harsh_event     → recent brake/accel/corner events (last 24h)
--   dim_device + last-trip → "device went silent in last 24h" synthetic alert
--
-- Priority score: severity × recency × risk_score weight. Higher = more
-- urgent. Calibrated so a critical+recent+high-risk-device tops the list.
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_real_time_alerts_stream AS
WITH
-- ---- Per-device latest risk + cluster context (for priority weighting) ----
device_ctx AS (
  SELECT DISTINCT ON (dr.tenant_id, dr.device_id)
    dr.tenant_id,
    dr.device_id,
    dr.risk_score,
    dr.risk_category
  FROM marts.fact_device_risk_score dr
  ORDER BY dr.tenant_id, dr.device_id, dr.year_month DESC
),
device_cluster_ctx AS (
  SELECT DISTINCT ON (c.tenant_id, c.device_id)
    c.tenant_id,
    c.device_id,
    c.cluster_id
  FROM marts.fact_device_cluster_assignment c
  ORDER BY c.tenant_id, c.device_id, c.year_month DESC
),
-- ---- Latest trip per device (for context column & inactive detection) ----
latest_trip AS (
  SELECT
    tenant_id,
    device_id,
    MAX(begin_path_time)                                        AS latest_trip_time,
    SUM(distance_km) FILTER (WHERE begin_path_time > NOW() - INTERVAL '24 hours')
                                                                AS distance_km_24h
  FROM warehouse.fact_trip
  WHERE begin_path_time > NOW() - INTERVAL '7 days'
  GROUP BY tenant_id, device_id
),
-- ---- 1. Notification-based alerts ----
notif AS (
  SELECT
    fn.notif_id::TEXT                                           AS source_id,
    fn.tenant_id,
    fn.device_id,
    fn.created_at                                               AS detected_at,
    CASE
      WHEN fn.alert_category = 'speed_alert'       THEN 'speed'
      WHEN fn.alert_category = 'geofence_alert'    THEN 'geofence'
      WHEN fn.alert_category = 'maintenance_alert' THEN 'maintenance'
      WHEN fn.alert_category = 'fuel_theft_alert'  THEN 'fuel_theft'
      WHEN fn.alert_category = 'ignition_alert'    THEN 'ignition'
      ELSE 'other'
    END                                                         AS alert_category,
    fn.description                                              AS alert_message,
    fn.alert_value                                              AS alert_value,
    -- Severity heuristic: panic/SIPHONAGE/extreme speed get critical, the
    -- rest land at medium. (Source data has no severity column directly.)
    CASE
      WHEN fn.alert_category = 'fuel_theft_alert'                           THEN 'critical'
      WHEN fn.description ILIKE '%PANIC%' OR fn.description ILIKE '%SOS%'    THEN 'critical'
      WHEN fn.alert_category = 'speed_alert'
        AND COALESCE(NULLIF(fn.alert_value, '')::NUMERIC, 0) > 140          THEN 'high'
      WHEN fn.alert_category = 'speed_alert'                                THEN 'medium'
      ELSE 'medium'
    END                                                         AS severity
  FROM warehouse.fact_notification fn
  WHERE fn.created_at > NOW() - INTERVAL '24 hours'
),
-- ---- 2. Recent overspeed events ----
over AS (
  SELECT
    fo.overspeed_sk::TEXT                                       AS source_id,
    fo.tenant_id,
    fo.device_id,
    fo.begin_path_time                                          AS detected_at,
    'speed'::TEXT                                               AS alert_category,
    'Overspeed: '
      || fo.max_speed_kmh::TEXT
      || ' km/h (limit '
      || fo.vehicle_limit::TEXT
      || ', '
      || COALESCE(fo.duration_seconds, 0)::TEXT
      || 's)'                                                    AS alert_message,
    fo.max_speed_kmh::TEXT                                      AS alert_value,
    CASE
      WHEN fo.severity = 'extreme' THEN 'critical'
      WHEN fo.severity = 'high'    THEN 'high'
      WHEN fo.severity = 'medium'  THEN 'medium'
      ELSE 'low'
    END                                                         AS severity
  FROM warehouse.fact_overspeed fo
  WHERE fo.begin_path_time > NOW() - INTERVAL '24 hours'
),
-- ---- 3. Recent harsh events ----
harsh AS (
  SELECT
    fh.harsh_event_sk::TEXT                                     AS source_id,
    fh.tenant_id,
    fh.device_id,
    fh.event_time                                               AS detected_at,
    'harsh'::TEXT                                               AS alert_category,
    'Harsh '
      || REPLACE(fh.event_type, 'harsh_', '')
      || ' @ '
      || fh.speed_kmh::TEXT
      || ' km/h ('
      || fh.severity
      || ')'                                                    AS alert_message,
    fh.event_type                                               AS alert_value,
    CASE
      WHEN fh.severity = 'extreme'  THEN 'critical'
      WHEN fh.severity = 'high'     THEN 'high'
      ELSE 'medium'
    END                                                         AS severity
  FROM warehouse.fact_harsh_event fh
  WHERE fh.event_time > NOW() - INTERVAL '24 hours'
),
-- ---- 4. Inactive devices (silent > 24h, were active in last 30d) ----
inactive AS (
  SELECT
    ('inactive-' || dd.tenant_id::TEXT || '-' || dd.device_id::TEXT)
                                                                 AS source_id,
    dd.tenant_id,
    dd.device_id,
    NOW()                                                        AS detected_at,
    'inactive'::TEXT                                             AS alert_category,
    'No trip in '
      || EXTRACT(HOUR FROM NOW() - lt.latest_trip_time)::INTEGER::TEXT
      || ' hours'                                                AS alert_message,
    EXTRACT(EPOCH FROM NOW() - lt.latest_trip_time)::INTEGER::TEXT
                                                                 AS alert_value,
    'medium'::TEXT                                               AS severity
  FROM warehouse.dim_device dd
  JOIN latest_trip lt USING (tenant_id, device_id)
  WHERE lt.latest_trip_time BETWEEN NOW() - INTERVAL '30 days'
                                AND NOW() - INTERVAL '24 hours'
),
-- ---- Union all sources ----
unioned AS (
  SELECT * FROM notif
  UNION ALL SELECT * FROM over
  UNION ALL SELECT * FROM harsh
  UNION ALL SELECT * FROM inactive
)
SELECT
  -- Stable alert ID = MD5 of source+id. Survives re-execution; safe to use
  -- as a primary-key for ack/dismiss flows added on top.
  MD5(u.alert_category || ':' || u.source_id)                   AS alert_id,
  u.tenant_id,
  u.device_id,
  u.alert_category,
  u.severity,
  u.detected_at,
  u.alert_message,
  u.alert_value,
  -- ---- Context (so the ops console doesn't need a second query) ----
  lt.latest_trip_time,
  COALESCE(lt.distance_km_24h, 0)                               AS distance_km_24h,
  dctx.risk_score                                               AS device_risk_score,
  dctx.risk_category                                            AS device_risk_category,
  dclu.cluster_id                                               AS device_cluster,
  -- ---- Priority Score (0-100) ----
  -- severity_weight × recency_weight × risk_weight, then capped at 100.
  LEAST(100,
    -- severity weight (critical=40, high=25, medium=15, low=5)
    CASE u.severity
      WHEN 'critical' THEN 40
      WHEN 'high'     THEN 25
      WHEN 'medium'   THEN 15
      ELSE                  5
    END
    -- recency weight (1.5× if < 1h, 1.0× if < 6h, 0.7× if < 24h)
    * CASE
        WHEN u.detected_at > NOW() - INTERVAL '1 hour' THEN 1.5
        WHEN u.detected_at > NOW() - INTERVAL '6 hours' THEN 1.0
        ELSE 0.7
      END
    -- risk-tier multiplier for the device
    * CASE COALESCE(dctx.risk_category, 'low')
        WHEN 'critical' THEN 1.5
        WHEN 'high'     THEN 1.2
        WHEN 'moderate' THEN 1.0
        ELSE 0.8
      END
  )::NUMERIC(5,1)                                               AS priority_score
FROM unioned u
LEFT JOIN latest_trip          lt   USING (tenant_id, device_id)
LEFT JOIN device_ctx           dctx USING (tenant_id, device_id)
LEFT JOIN device_cluster_ctx   dclu USING (tenant_id, device_id)
ORDER BY priority_score DESC, u.detected_at DESC;
