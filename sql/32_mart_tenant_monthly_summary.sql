-- =============================================================================
-- 32_mart_tenant_monthly_summary.sql
-- =============================================================================
-- mart_tenant_monthly_summary: top-level monthly KPI per tenant. Built by
-- rolling up mart_vehicle_monthly + mart_fleet_daily so the BI layer doesn't
-- have to scan facts. One row per (tenant_id, year_month).
--
-- Recompute scope: :touched_months (TEXT[]).
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.mart_tenant_monthly_summary (
  tenant_id                INTEGER NOT NULL,
  year_month               CHAR(7) NOT NULL,
  -- Fleet size (distinct vehicles seen in the month)
  active_vehicles          INTEGER NOT NULL DEFAULT 0,
  active_devices           INTEGER NOT NULL DEFAULT 0,
  -- Driving
  total_trips              INTEGER NOT NULL DEFAULT 0,
  total_distance_km        DOUBLE PRECISION NOT NULL DEFAULT 0,
  total_driving_hours      DOUBLE PRECISION,
  avg_distance_per_vehicle DOUBLE PRECISION,
  -- Risk
  total_overspeed          INTEGER NOT NULL DEFAULT 0,
  total_harsh_events       INTEGER NOT NULL DEFAULT 0,
  total_alerts             INTEGER NOT NULL DEFAULT 0,
  panic_alerts             INTEGER NOT NULL DEFAULT 0,
  -- Cost
  total_maintenance_cost   DOUBLE PRECISION NOT NULL DEFAULT 0,
  total_fuel_cost          DOUBLE PRECISION NOT NULL DEFAULT 0,
  total_operating_cost     DOUBLE PRECISION NOT NULL DEFAULT 0,
  cost_per_km              DOUBLE PRECISION,
  -- Lineage
  _etl_run_id              BIGINT,
  _computed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, year_month)
);

CREATE INDEX IF NOT EXISTS idx_mart_tenant_monthly_summary_ym
  ON marts.mart_tenant_monthly_summary (year_month);

WITH
target_months AS (
  SELECT DISTINCT m AS year_month
  FROM UNNEST(CAST(:touched_months AS text[])) AS m
),
-- Roll up the vehicle-grain mart
veh AS (
  SELECT
    mv.tenant_id, mv.year_month,
    COUNT(*) FILTER (WHERE mv.total_distance_km > 0)::INTEGER     AS active_vehicles,
    SUM(mv.total_trips)::INTEGER                                   AS total_trips,
    SUM(mv.total_distance_km)                                     AS total_distance_km,
    SUM(mv.total_driving_hours)                                   AS total_driving_hours,
    SUM(mv.maintenance_cost_total)                                AS total_maintenance_cost,
    SUM(mv.fuel_cost_total)                                       AS total_fuel_cost
  FROM marts.mart_vehicle_monthly mv
  JOIN target_months tm USING (year_month)
  GROUP BY mv.tenant_id, mv.year_month
),
-- Day-grain mart aggregated to month for risk + alert + active_devices
day AS (
  SELECT
    mfd.tenant_id,
    TO_CHAR(mfd.fleet_date, 'YYYY-MM')                            AS year_month,
    MAX(mfd.active_devices)::INTEGER                              AS active_devices,
    SUM(mfd.overspeed_events)::INTEGER                            AS total_overspeed,
    SUM(mfd.harsh_brake_events + mfd.harsh_accel_events
        + mfd.harsh_corner_events)::INTEGER                       AS total_harsh_events,
    SUM(mfd.total_alerts)::INTEGER                                AS total_alerts,
    SUM(mfd.panic_alerts)::INTEGER                                AS panic_alerts
  FROM marts.mart_fleet_daily mfd
  JOIN target_months tm ON TO_CHAR(mfd.fleet_date, 'YYYY-MM') = tm.year_month
  GROUP BY mfd.tenant_id, TO_CHAR(mfd.fleet_date, 'YYYY-MM')
),
all_keys AS (
  SELECT tenant_id, year_month FROM veh
  UNION SELECT tenant_id, year_month FROM day
)
INSERT INTO marts.mart_tenant_monthly_summary (
  tenant_id, year_month,
  active_vehicles, active_devices,
  total_trips, total_distance_km, total_driving_hours, avg_distance_per_vehicle,
  total_overspeed, total_harsh_events, total_alerts, panic_alerts,
  total_maintenance_cost, total_fuel_cost, total_operating_cost, cost_per_km,
  _etl_run_id
)
SELECT
  k.tenant_id, k.year_month,
  COALESCE(v.active_vehicles, 0),
  COALESCE(d.active_devices, 0),
  COALESCE(v.total_trips, 0),
  COALESCE(v.total_distance_km, 0),
  COALESCE(v.total_driving_hours, 0),
  -- Defaults to 0 (not NULL) so BI cards/charts render cleanly.
  CASE WHEN COALESCE(v.active_vehicles, 0) > 0
       THEN v.total_distance_km / v.active_vehicles
       ELSE 0 END                                                 AS avg_distance_per_vehicle,
  COALESCE(d.total_overspeed, 0),
  COALESCE(d.total_harsh_events, 0),
  COALESCE(d.total_alerts, 0),
  COALESCE(d.panic_alerts, 0),
  COALESCE(v.total_maintenance_cost, 0),
  COALESCE(v.total_fuel_cost, 0),
  COALESCE(v.total_maintenance_cost, 0) + COALESCE(v.total_fuel_cost, 0)
                                                                  AS total_operating_cost,
  CASE WHEN COALESCE(v.total_distance_km, 0) > 0
       THEN (COALESCE(v.total_maintenance_cost, 0) + COALESCE(v.total_fuel_cost, 0))
            / v.total_distance_km
       ELSE 0 END                                                 AS cost_per_km,
  :etl_run_id
FROM all_keys k
LEFT JOIN veh v USING (tenant_id, year_month)
LEFT JOIN day d USING (tenant_id, year_month)
ON CONFLICT (tenant_id, year_month) DO UPDATE SET
  active_vehicles          = EXCLUDED.active_vehicles,
  active_devices           = EXCLUDED.active_devices,
  total_trips              = EXCLUDED.total_trips,
  total_distance_km        = EXCLUDED.total_distance_km,
  total_driving_hours      = EXCLUDED.total_driving_hours,
  avg_distance_per_vehicle = EXCLUDED.avg_distance_per_vehicle,
  total_overspeed          = EXCLUDED.total_overspeed,
  total_harsh_events       = EXCLUDED.total_harsh_events,
  total_alerts             = EXCLUDED.total_alerts,
  panic_alerts             = EXCLUDED.panic_alerts,
  total_maintenance_cost   = EXCLUDED.total_maintenance_cost,
  total_fuel_cost          = EXCLUDED.total_fuel_cost,
  total_operating_cost     = EXCLUDED.total_operating_cost,
  cost_per_km              = EXCLUDED.cost_per_km,
  _etl_run_id              = EXCLUDED._etl_run_id,
  _computed_at             = NOW();
