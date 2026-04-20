-- =============================================================================
-- 23_v_fleet_risk_dashboard.sql
-- =============================================================================
-- Tenant-level fleet dashboard. Consumed by BI layer.
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_fleet_risk_dashboard AS
SELECT
  r.tenant_id,
  dt.tenant_label,
  COUNT(*)                                                       AS devices_scored,
  COUNT(*) FILTER (WHERE r.risk_category = 'critical')           AS critical_devices,
  COUNT(*) FILTER (WHERE r.risk_category = 'high')               AS high_devices,
  COUNT(*) FILTER (WHERE r.risk_category = 'moderate')           AS moderate_devices,
  COUNT(*) FILTER (WHERE r.risk_category = 'low')                AS low_devices,
  ROUND(AVG(r.risk_score), 1)                                    AS avg_risk_score,
  MAX(r.risk_score)                                              AS max_risk_score,
  SUM(r.overspeed_3m)                                            AS fleet_overspeed_3m,
  SUM(r.distance_3m)                                             AS fleet_distance_3m,
  CASE WHEN SUM(r.distance_3m) > 0
       THEN SUM(r.overspeed_3m) / SUM(r.distance_3m) * 100
       ELSE 0 END                                                AS fleet_overspeed_per_100km,
  MAX(m.avg_max_speed_kmh)                                       AS fleet_highest_avg_max_speed,
  AVG(m.night_trip_ratio)                                        AS fleet_avg_night_ratio
FROM marts.v_device_risk_profile r
JOIN warehouse.dim_tenant dt ON dt.tenant_id = r.tenant_id
JOIN marts.mart_device_monthly_behavior m
  ON m.tenant_id = r.tenant_id
 AND m.device_id = r.device_id
 AND m.year_month = r.latest_month
GROUP BY r.tenant_id, dt.tenant_label;
