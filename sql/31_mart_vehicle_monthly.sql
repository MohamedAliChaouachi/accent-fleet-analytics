-- =============================================================================
-- 31_mart_vehicle_monthly.sql
-- =============================================================================
-- mart_vehicle_monthly: per-(tenant, vehicle, year_month) rollup combining
-- driving (from device-grain facts) with maintenance/fueling (vehicle-grain
-- facts). The grain switch is resolved through dim_device.vehicle_sk →
-- dim_vehicle.vehicle_sk → dim_vehicle.vehicule_id.
--
-- Recompute scope: :touched_months (TEXT[]). Same contract as
-- mart_device_monthly_behavior.
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.mart_vehicle_monthly (
  tenant_id                INTEGER NOT NULL,
  vehicle_id               BIGINT NOT NULL,         -- staging.vehicule.vehicule_id
  year_month               CHAR(7) NOT NULL,
  -- Driving (from trips on devices linked to this vehicle)
  active_days              INTEGER NOT NULL DEFAULT 0,
  total_trips              INTEGER NOT NULL DEFAULT 0,
  total_distance_km        DOUBLE PRECISION NOT NULL DEFAULT 0,
  total_driving_hours      DOUBLE PRECISION,
  trip_fuel_used_l         DOUBLE PRECISION,
  -- Maintenance
  maintenance_events       INTEGER NOT NULL DEFAULT 0,
  maintenance_cost_total   DOUBLE PRECISION NOT NULL DEFAULT 0,
  maintenance_labor_total  DOUBLE PRECISION NOT NULL DEFAULT 0,
  offense_events           INTEGER NOT NULL DEFAULT 0,
  sinistre_events          INTEGER NOT NULL DEFAULT 0,
  reparation_events        INTEGER NOT NULL DEFAULT 0,
  reparation_amount_total  DOUBLE PRECISION NOT NULL DEFAULT 0,
  -- Fueling
  fueling_events           INTEGER NOT NULL DEFAULT 0,
  fuel_litres              DOUBLE PRECISION NOT NULL DEFAULT 0,
  fuel_cost_total          DOUBLE PRECISION NOT NULL DEFAULT 0,
  avg_cost_per_litre       DOUBLE PRECISION,
  -- Derived efficiency (defensive division)
  cost_per_km              DOUBLE PRECISION,
  fuel_l_per_100km         DOUBLE PRECISION,
  -- Lineage
  _etl_run_id              BIGINT,
  _computed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, vehicle_id, year_month)
);

CREATE INDEX IF NOT EXISTS idx_mart_vehicle_monthly_ym
  ON marts.mart_vehicle_monthly (year_month);
CREATE INDEX IF NOT EXISTS idx_mart_vehicle_monthly_tenant
  ON marts.mart_vehicle_monthly (tenant_id, year_month);

WITH
target_months AS (
  SELECT DISTINCT m AS year_month
  FROM UNNEST(CAST(:touched_months AS text[])) AS m
),
-- Driving aggregates (device-grain → vehicle-grain via dim_device)
trip_agg AS (
  SELECT
    ft.tenant_id,
    dv.vehicule_id                                       AS vehicle_id,
    TO_CHAR(ft.begin_path_time, 'YYYY-MM')               AS year_month,
    COUNT(DISTINCT ft.trip_date)::INTEGER                AS active_days,
    COUNT(*)::INTEGER                                     AS total_trips,
    SUM(ft.distance_km)                                   AS total_distance_km,
    SUM(ft.duration_seconds) / 3600.0                     AS total_driving_hours,
    SUM(ft.fuel_used)                                     AS trip_fuel_used_l
  FROM warehouse.fact_trip ft
  JOIN warehouse.dim_device dd
    ON dd.tenant_id = ft.tenant_id AND dd.device_id = ft.device_id
  JOIN warehouse.dim_vehicle dv
    ON dv.vehicle_sk = dd.vehicle_sk
  JOIN target_months tm ON TO_CHAR(ft.begin_path_time, 'YYYY-MM') = tm.year_month
  GROUP BY ft.tenant_id, dv.vehicule_id, TO_CHAR(ft.begin_path_time, 'YYYY-MM')
),
maint_agg AS (
  SELECT
    fm.tenant_id,
    fm.vehicle_id,
    TO_CHAR(fm.date_operation, 'YYYY-MM')                AS year_month,
    COUNT(*)::INTEGER                                     AS maintenance_events,
    SUM(COALESCE(fm.total_cost, 0))                      AS maintenance_cost_total,
    SUM(COALESCE(fm.labor_cost, 0))                      AS maintenance_labor_total
  FROM warehouse.fact_maintenance fm
  JOIN target_months tm ON TO_CHAR(fm.date_operation, 'YYYY-MM') = tm.year_month
  GROUP BY fm.tenant_id, fm.vehicle_id, TO_CHAR(fm.date_operation, 'YYYY-MM')
),
line_agg AS (
  SELECT
    fl.tenant_id,
    fl.vehicle_id,
    TO_CHAR(fl.date_operation, 'YYYY-MM')                AS year_month,
    COUNT(*) FILTER (WHERE fl.line_type = 'offense')::INTEGER     AS offense_events,
    COUNT(*) FILTER (WHERE fl.line_type = 'sinistre')::INTEGER    AS sinistre_events,
    COUNT(*) FILTER (WHERE fl.line_type = 'reparation')::INTEGER  AS reparation_events,
    SUM(CASE WHEN fl.line_type = 'reparation'
             THEN COALESCE(fl.amount, 0) ELSE 0 END)              AS reparation_amount_total
  FROM warehouse.fact_maintenance_line fl
  JOIN target_months tm ON TO_CHAR(fl.date_operation, 'YYYY-MM') = tm.year_month
  GROUP BY fl.tenant_id, fl.vehicle_id, TO_CHAR(fl.date_operation, 'YYYY-MM')
),
fuel_agg AS (
  SELECT
    ff.tenant_id,
    ff.vehicle_id,
    TO_CHAR(ff.fueling_at, 'YYYY-MM')                    AS year_month,
    COUNT(*)::INTEGER                                     AS fueling_events,
    SUM(COALESCE(ff.quantity_l, 0))                      AS fuel_litres,
    SUM(COALESCE(ff.cost_total, 0))                      AS fuel_cost_total,
    AVG(ff.cost_per_litre)                               AS avg_cost_per_litre
  FROM warehouse.fact_fueling ff
  JOIN target_months tm ON TO_CHAR(ff.fueling_at, 'YYYY-MM') = tm.year_month
  GROUP BY ff.tenant_id, ff.vehicle_id, TO_CHAR(ff.fueling_at, 'YYYY-MM')
),
all_keys AS (
  SELECT tenant_id, vehicle_id, year_month FROM trip_agg
  UNION SELECT tenant_id, vehicle_id, year_month FROM maint_agg
  UNION SELECT tenant_id, vehicle_id, year_month FROM line_agg
  UNION SELECT tenant_id, vehicle_id, year_month FROM fuel_agg
)
INSERT INTO marts.mart_vehicle_monthly (
  tenant_id, vehicle_id, year_month,
  active_days, total_trips, total_distance_km, total_driving_hours, trip_fuel_used_l,
  maintenance_events, maintenance_cost_total, maintenance_labor_total,
  offense_events, sinistre_events, reparation_events, reparation_amount_total,
  fueling_events, fuel_litres, fuel_cost_total, avg_cost_per_litre,
  cost_per_km, fuel_l_per_100km, _etl_run_id
)
SELECT
  k.tenant_id, k.vehicle_id, k.year_month,
  COALESCE(t.active_days, 0),
  COALESCE(t.total_trips, 0),
  COALESCE(t.total_distance_km, 0),
  t.total_driving_hours,
  t.trip_fuel_used_l,
  COALESCE(m.maintenance_events, 0),
  COALESCE(m.maintenance_cost_total, 0),
  COALESCE(m.maintenance_labor_total, 0),
  COALESCE(l.offense_events, 0),
  COALESCE(l.sinistre_events, 0),
  COALESCE(l.reparation_events, 0),
  COALESCE(l.reparation_amount_total, 0),
  COALESCE(f.fueling_events, 0),
  COALESCE(f.fuel_litres, 0),
  COALESCE(f.fuel_cost_total, 0),
  f.avg_cost_per_litre,
  -- cost_per_km = (maintenance_total + fuel_total) / total_distance_km
  CASE WHEN COALESCE(t.total_distance_km, 0) > 0
       THEN (COALESCE(m.maintenance_cost_total, 0) + COALESCE(f.fuel_cost_total, 0))
            / t.total_distance_km
       ELSE NULL END                                            AS cost_per_km,
  CASE WHEN COALESCE(t.total_distance_km, 0) > 0 AND f.fuel_litres IS NOT NULL
       THEN f.fuel_litres / t.total_distance_km * 100.0
       ELSE NULL END                                            AS fuel_l_per_100km,
  :etl_run_id
FROM all_keys k
LEFT JOIN trip_agg  t USING (tenant_id, vehicle_id, year_month)
LEFT JOIN maint_agg m USING (tenant_id, vehicle_id, year_month)
LEFT JOIN line_agg  l USING (tenant_id, vehicle_id, year_month)
LEFT JOIN fuel_agg  f USING (tenant_id, vehicle_id, year_month)
ON CONFLICT (tenant_id, vehicle_id, year_month) DO UPDATE SET
  active_days              = EXCLUDED.active_days,
  total_trips              = EXCLUDED.total_trips,
  total_distance_km        = EXCLUDED.total_distance_km,
  total_driving_hours      = EXCLUDED.total_driving_hours,
  trip_fuel_used_l         = EXCLUDED.trip_fuel_used_l,
  maintenance_events       = EXCLUDED.maintenance_events,
  maintenance_cost_total   = EXCLUDED.maintenance_cost_total,
  maintenance_labor_total  = EXCLUDED.maintenance_labor_total,
  offense_events           = EXCLUDED.offense_events,
  sinistre_events          = EXCLUDED.sinistre_events,
  reparation_events        = EXCLUDED.reparation_events,
  reparation_amount_total  = EXCLUDED.reparation_amount_total,
  fueling_events           = EXCLUDED.fueling_events,
  fuel_litres              = EXCLUDED.fuel_litres,
  fuel_cost_total          = EXCLUDED.fuel_cost_total,
  avg_cost_per_litre       = EXCLUDED.avg_cost_per_litre,
  cost_per_km              = EXCLUDED.cost_per_km,
  fuel_l_per_100km         = EXCLUDED.fuel_l_per_100km,
  _etl_run_id              = EXCLUDED._etl_run_id,
  _computed_at             = NOW();
