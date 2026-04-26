-- =============================================================================
-- 34_v_operational_dashboard.sql
-- =============================================================================
-- Day-grain operational view for fleet managers. Shows the last 90 days
-- of fleet activity with derived ratios used for traffic-light KPIs:
--   - alerts_per_100km
--   - overspeed_per_100km
--   - harsh_events_per_100km
--   - panic_share_pct (panic alerts as % of total alerts)
--
-- Dashboard usage:
--   SELECT * FROM marts.v_operational_dashboard
--   WHERE tenant_id = :t AND fleet_date >= CURRENT_DATE - 90;
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_operational_dashboard AS
SELECT
  d.tenant_id,
  d.fleet_date,
  d.active_devices,
  d.total_trips,
  d.total_distance_km,
  d.total_driving_hours,
  d.total_stops,
  d.total_alerts,
  d.speed_alerts,
  d.geofence_alerts,
  d.panic_alerts,
  d.maintenance_alerts,
  d.other_alerts,
  d.overspeed_events,
  d.harsh_brake_events,
  d.harsh_accel_events,
  d.harsh_corner_events,
  (d.harsh_brake_events + d.harsh_accel_events + d.harsh_corner_events)
                                                                  AS total_harsh_events,
  -- Defensive ratios
  CASE WHEN d.total_distance_km > 0
       THEN d.total_alerts::DOUBLE PRECISION / d.total_distance_km * 100
       ELSE NULL END                                              AS alerts_per_100km,
  CASE WHEN d.total_distance_km > 0
       THEN d.overspeed_events::DOUBLE PRECISION / d.total_distance_km * 100
       ELSE NULL END                                              AS overspeed_per_100km,
  CASE WHEN d.total_distance_km > 0
       THEN (d.harsh_brake_events + d.harsh_accel_events + d.harsh_corner_events)::DOUBLE PRECISION
            / d.total_distance_km * 100
       ELSE NULL END                                              AS harsh_events_per_100km,
  CASE WHEN d.total_alerts > 0
       THEN d.panic_alerts::DOUBLE PRECISION / d.total_alerts * 100
       ELSE NULL END                                              AS panic_share_pct,
  -- 7-day rolling for trendlines
  AVG(d.total_distance_km) OVER w7                                AS distance_km_7d_avg,
  AVG(d.total_alerts) OVER w7                                     AS alerts_7d_avg
FROM marts.mart_fleet_daily d
WINDOW w7 AS (PARTITION BY d.tenant_id ORDER BY d.fleet_date
              ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);
