-- =============================================================================
-- 30_mart_fleet_daily.sql
-- =============================================================================
-- mart_fleet_daily: per-(tenant, date) executive KPIs for the BI dashboard.
-- Aggregates every operational fact in the warehouse to a single daily row
-- per tenant — the grain BI tools want for line/area charts and KPI cards.
--
-- Recompute scope: :touched_dates (DATE[]). Only those dates are recomputed
-- per run. The Python loader builds the list from each fact's window.
--
-- One row per (tenant_id, fleet_date).
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.mart_fleet_daily (
  tenant_id                 INTEGER NOT NULL,
  fleet_date                DATE NOT NULL,
  -- Activity volumes
  active_devices            INTEGER NOT NULL DEFAULT 0,
  total_trips               INTEGER NOT NULL DEFAULT 0,
  total_distance_km         DOUBLE PRECISION NOT NULL DEFAULT 0,
  total_driving_hours       DOUBLE PRECISION,
  -- Speed & risk
  avg_max_speed_kmh         DOUBLE PRECISION,
  overspeed_events          INTEGER NOT NULL DEFAULT 0,
  harsh_brake_events        INTEGER NOT NULL DEFAULT 0,
  harsh_accel_events        INTEGER NOT NULL DEFAULT 0,
  harsh_corner_events       INTEGER NOT NULL DEFAULT 0,
  -- Alerts (full notification)
  total_alerts              INTEGER NOT NULL DEFAULT 0,
  speed_alerts              INTEGER NOT NULL DEFAULT 0,
  geofence_alerts           INTEGER NOT NULL DEFAULT 0,
  panic_alerts              INTEGER NOT NULL DEFAULT 0,
  maintenance_alerts        INTEGER NOT NULL DEFAULT 0,
  other_alerts              INTEGER NOT NULL DEFAULT 0,
  -- Stops
  total_stops               INTEGER NOT NULL DEFAULT 0,
  -- Maintenance
  maintenance_events        INTEGER NOT NULL DEFAULT 0,
  maintenance_cost_total    DOUBLE PRECISION NOT NULL DEFAULT 0,
  -- Fueling
  fueling_events            INTEGER NOT NULL DEFAULT 0,
  fuel_litres               DOUBLE PRECISION NOT NULL DEFAULT 0,
  fuel_cost_total           DOUBLE PRECISION NOT NULL DEFAULT 0,
  -- Lineage
  _etl_run_id               BIGINT,
  _computed_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, fleet_date)
);

CREATE INDEX IF NOT EXISTS idx_mart_fleet_daily_date
  ON marts.mart_fleet_daily (fleet_date DESC);
CREATE INDEX IF NOT EXISTS idx_mart_fleet_daily_tenant_date
  ON marts.mart_fleet_daily (tenant_id, fleet_date DESC);

-- -----------------------------------------------------------------------------
-- The recompute. CTE chain — one CTE per source fact, then OUTER JOIN-merge
-- into the target. Pattern intentionally mirrors mart_device_monthly_behavior.
-- -----------------------------------------------------------------------------
WITH
target_dates AS (
  SELECT DISTINCT d::DATE AS fleet_date
  FROM UNNEST(CAST(:touched_dates AS DATE[])) AS d
),
trip_agg AS (
  SELECT
    ft.tenant_id,
    ft.begin_path_time::DATE                            AS fleet_date,
    COUNT(DISTINCT ft.device_id)::INTEGER               AS active_devices,
    COUNT(*)::INTEGER                                    AS total_trips,
    SUM(ft.distance_km)                                  AS total_distance_km,
    SUM(ft.duration_seconds) / 3600.0                    AS total_driving_hours,
    AVG(ft.max_speed_kmh)                                AS avg_max_speed_kmh
  FROM warehouse.fact_trip ft
  JOIN target_dates td ON ft.begin_path_time::DATE = td.fleet_date
  GROUP BY ft.tenant_id, ft.begin_path_time::DATE
),
overspeed_agg AS (
  SELECT
    fo.tenant_id,
    fo.begin_path_time::DATE                             AS fleet_date,
    COUNT(*)::INTEGER                                     AS overspeed_events
  FROM warehouse.fact_overspeed fo
  JOIN target_dates td ON fo.begin_path_time::DATE = td.fleet_date
  GROUP BY fo.tenant_id, fo.begin_path_time::DATE
),
harsh_agg AS (
  SELECT
    fh.tenant_id,
    fh.event_time::DATE                                  AS fleet_date,
    COUNT(*) FILTER (WHERE fh.event_type = 'harsh_brake')::INTEGER  AS harsh_brake_events,
    COUNT(*) FILTER (WHERE fh.event_type = 'harsh_accel')::INTEGER  AS harsh_accel_events,
    COUNT(*) FILTER (WHERE fh.event_type = 'harsh_corner')::INTEGER AS harsh_corner_events
  FROM warehouse.fact_harsh_event fh
  JOIN target_dates td ON fh.event_time::DATE = td.fleet_date
  GROUP BY fh.tenant_id, fh.event_time::DATE
),
notif_agg AS (
  SELECT
    fn.tenant_id,
    fn.notification_date                                  AS fleet_date,
    COUNT(*)::INTEGER                                      AS total_alerts,
    COUNT(*) FILTER (WHERE fn.alert_category = 'speed_alert')::INTEGER       AS speed_alerts,
    COUNT(*) FILTER (WHERE fn.alert_category = 'geofence_alert')::INTEGER    AS geofence_alerts,
    COUNT(*) FILTER (WHERE fn.alert_category = 'panic_alert')::INTEGER       AS panic_alerts,
    COUNT(*) FILTER (WHERE fn.alert_category = 'maintenance_alert')::INTEGER AS maintenance_alerts,
    COUNT(*) FILTER (WHERE fn.alert_category NOT IN
      ('speed_alert','geofence_alert','panic_alert','maintenance_alert'))::INTEGER AS other_alerts
  FROM warehouse.fact_notification fn
  JOIN target_dates td ON fn.notification_date = td.fleet_date
  GROUP BY fn.tenant_id, fn.notification_date
),
stop_agg AS (
  SELECT
    fs.tenant_id,
    fs.stop_start::DATE                                   AS fleet_date,
    COUNT(*)::INTEGER                                      AS total_stops
  FROM warehouse.fact_stop fs
  JOIN target_dates td ON fs.stop_start::DATE = td.fleet_date
  GROUP BY fs.tenant_id, fs.stop_start::DATE
),
maint_agg AS (
  SELECT
    fm.tenant_id,
    fm.maintenance_date                                   AS fleet_date,
    COUNT(*)::INTEGER                                      AS maintenance_events,
    SUM(COALESCE(fm.total_cost, 0))                       AS maintenance_cost_total
  FROM warehouse.fact_maintenance fm
  JOIN target_dates td ON fm.maintenance_date = td.fleet_date
  GROUP BY fm.tenant_id, fm.maintenance_date
),
fuel_agg AS (
  SELECT
    ff.tenant_id,
    ff.fueling_date                                       AS fleet_date,
    COUNT(*)::INTEGER                                      AS fueling_events,
    SUM(COALESCE(ff.quantity_l, 0))                       AS fuel_litres,
    SUM(COALESCE(ff.cost_total, 0))                       AS fuel_cost_total
  FROM warehouse.fact_fueling ff
  JOIN target_dates td ON ff.fueling_date = td.fleet_date
  GROUP BY ff.tenant_id, ff.fueling_date
),
-- Outer-merge all keys (tenant, date) so a tenant with only maintenance on a
-- day still gets a row.
all_keys AS (
  SELECT tenant_id, fleet_date FROM trip_agg
  UNION SELECT tenant_id, fleet_date FROM overspeed_agg
  UNION SELECT tenant_id, fleet_date FROM harsh_agg
  UNION SELECT tenant_id, fleet_date FROM notif_agg
  UNION SELECT tenant_id, fleet_date FROM stop_agg
  UNION SELECT tenant_id, fleet_date FROM maint_agg
  UNION SELECT tenant_id, fleet_date FROM fuel_agg
)
INSERT INTO marts.mart_fleet_daily (
  tenant_id, fleet_date,
  active_devices, total_trips, total_distance_km, total_driving_hours,
  avg_max_speed_kmh, overspeed_events,
  harsh_brake_events, harsh_accel_events, harsh_corner_events,
  total_alerts, speed_alerts, geofence_alerts, panic_alerts,
  maintenance_alerts, other_alerts,
  total_stops,
  maintenance_events, maintenance_cost_total,
  fueling_events, fuel_litres, fuel_cost_total,
  _etl_run_id
)
SELECT
  k.tenant_id, k.fleet_date,
  COALESCE(t.active_devices, 0),
  COALESCE(t.total_trips, 0),
  COALESCE(t.total_distance_km, 0),
  COALESCE(t.total_driving_hours, 0),
  COALESCE(t.avg_max_speed_kmh, 0),
  COALESCE(o.overspeed_events, 0),
  COALESCE(h.harsh_brake_events, 0),
  COALESCE(h.harsh_accel_events, 0),
  COALESCE(h.harsh_corner_events, 0),
  COALESCE(n.total_alerts, 0),
  COALESCE(n.speed_alerts, 0),
  COALESCE(n.geofence_alerts, 0),
  COALESCE(n.panic_alerts, 0),
  COALESCE(n.maintenance_alerts, 0),
  COALESCE(n.other_alerts, 0),
  COALESCE(s.total_stops, 0),
  COALESCE(m.maintenance_events, 0),
  COALESCE(m.maintenance_cost_total, 0),
  COALESCE(f.fueling_events, 0),
  COALESCE(f.fuel_litres, 0),
  COALESCE(f.fuel_cost_total, 0),
  :etl_run_id
FROM all_keys k
LEFT JOIN trip_agg      t USING (tenant_id, fleet_date)
LEFT JOIN overspeed_agg o USING (tenant_id, fleet_date)
LEFT JOIN harsh_agg     h USING (tenant_id, fleet_date)
LEFT JOIN notif_agg     n USING (tenant_id, fleet_date)
LEFT JOIN stop_agg      s USING (tenant_id, fleet_date)
LEFT JOIN maint_agg     m USING (tenant_id, fleet_date)
LEFT JOIN fuel_agg      f USING (tenant_id, fleet_date)
ON CONFLICT (tenant_id, fleet_date) DO UPDATE SET
  active_devices         = EXCLUDED.active_devices,
  total_trips            = EXCLUDED.total_trips,
  total_distance_km      = EXCLUDED.total_distance_km,
  total_driving_hours    = EXCLUDED.total_driving_hours,
  avg_max_speed_kmh      = EXCLUDED.avg_max_speed_kmh,
  overspeed_events       = EXCLUDED.overspeed_events,
  harsh_brake_events     = EXCLUDED.harsh_brake_events,
  harsh_accel_events     = EXCLUDED.harsh_accel_events,
  harsh_corner_events    = EXCLUDED.harsh_corner_events,
  total_alerts           = EXCLUDED.total_alerts,
  speed_alerts           = EXCLUDED.speed_alerts,
  geofence_alerts        = EXCLUDED.geofence_alerts,
  panic_alerts           = EXCLUDED.panic_alerts,
  maintenance_alerts     = EXCLUDED.maintenance_alerts,
  other_alerts           = EXCLUDED.other_alerts,
  total_stops            = EXCLUDED.total_stops,
  maintenance_events     = EXCLUDED.maintenance_events,
  maintenance_cost_total = EXCLUDED.maintenance_cost_total,
  fueling_events         = EXCLUDED.fueling_events,
  fuel_litres            = EXCLUDED.fuel_litres,
  fuel_cost_total        = EXCLUDED.fuel_cost_total,
  _etl_run_id            = EXCLUDED._etl_run_id,
  _computed_at           = NOW();
