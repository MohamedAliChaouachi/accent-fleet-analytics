-- =============================================================================
-- 36_v_fleet_efficiency_dashboard.sql
-- =============================================================================
-- Fleet efficiency BI view (v2.0). One row per (tenant_id, year_month) with
-- cost-per-km, fuel efficiency, utilization, and MoM trend columns the
-- operations team needs to monitor Total Cost of Ownership at a glance.
--
-- Why a view (not a mart):
--   Every column here is derivable from existing marts. Materialising would
--   be redundant churn — and the view re-evaluates on each query so MoM
--   ratios always reflect the freshest mart_tenant_monthly_summary refresh.
--
-- Dashboard usage:
--   SELECT * FROM marts.v_fleet_efficiency_dashboard
--    WHERE tenant_id = :t AND year_month BETWEEN :start_month AND :end_month
--    ORDER BY year_month;
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_fleet_efficiency_dashboard AS
WITH
device_agg AS (
  -- Roll mart_device_monthly_behavior (device × month) up to tenant × month
  -- for "active device-days" (utilization denominator) and the typical trip
  -- duration. Weighted by trip count so a device that ran 1 trip doesn't
  -- skew the fleet average.
  SELECT
    tenant_id,
    year_month,
    SUM(active_days)                                          AS device_active_days_sum,
    SUM(total_trips * avg_trip_duration_minutes)
      / NULLIF(SUM(total_trips), 0)                           AS avg_trip_duration_min,
    -- Idle proxy: long stops (> medium threshold) divided by total stops.
    -- We don't have raw idle_minutes/ignition_minutes in the marts yet, so
    -- this is the best available signal. Documented in the column comment
    -- on consumers.
    SUM(long_stop_count)::FLOAT / NULLIF(SUM(total_stops), 0) * 100
                                                              AS idle_time_pct
  FROM marts.mart_device_monthly_behavior
  GROUP BY tenant_id, year_month
),
vehicle_agg AS (
  -- Roll mart_vehicle_monthly (vehicle × month) to tenant × month for fuel
  -- litres. mart_tenant_monthly_summary already has fuel _cost_, but not
  -- _litres_ — so a quantity-weighted avg cost-per-litre needs this CTE.
  SELECT
    tenant_id,
    year_month,
    SUM(fuel_litres)                                          AS total_fuel_litres,
    SUM(fuel_cost_total) / NULLIF(SUM(fuel_litres), 0)        AS avg_cost_per_litre
  FROM marts.mart_vehicle_monthly
  GROUP BY tenant_id, year_month
),
days_in_month AS (
  -- Days in each calendar month — the denominator for "trips per device per
  -- day" so a 31-day month doesn't look more productive than a 28-day one.
  SELECT
    year_month,
    EXTRACT(DAY FROM
      (TO_DATE(year_month || '-01', 'YYYY-MM-DD')
       + INTERVAL '1 month' - INTERVAL '1 day')
    )::INTEGER                                                AS days
  FROM (SELECT DISTINCT year_month FROM marts.mart_tenant_monthly_summary) ym
)
SELECT
  s.tenant_id,
  s.year_month,
  -- ---- Activity context (so dashboards don't need a second query) ----
  s.active_devices,
  s.active_vehicles,
  s.total_trips,
  s.total_distance_km,
  s.total_driving_hours,
  s.total_operating_cost,
  s.total_fuel_cost,
  s.total_maintenance_cost,
  -- ---- Cost Efficiency (DA per km / trip) ----
  CASE WHEN s.total_distance_km > 0
       THEN s.total_operating_cost / s.total_distance_km
       ELSE 0 END                                              AS cost_per_km,
  CASE WHEN s.total_trips > 0
       THEN s.total_operating_cost::DOUBLE PRECISION / s.total_trips
       ELSE 0 END                                              AS cost_per_trip,
  CASE WHEN s.total_distance_km > 0
       THEN s.total_fuel_cost / s.total_distance_km * 100
       ELSE 0 END                                              AS fuel_cost_per_100km,
  CASE WHEN s.total_distance_km > 0
       THEN s.total_maintenance_cost / s.total_distance_km
       ELSE 0 END                                              AS maintenance_cost_per_km,
  -- ---- Fuel Efficiency ----
  COALESCE(va.total_fuel_litres, 0)                            AS total_fuel_litres,
  CASE WHEN s.total_distance_km > 0
       THEN COALESCE(va.total_fuel_litres, 0) / s.total_distance_km * 100
       ELSE 0 END                                              AS fuel_litres_per_100km,
  COALESCE(va.avg_cost_per_litre, 0)                           AS avg_cost_per_litre,
  -- ---- Utilization ----
  -- "What share of device-days were the devices actually driving?"
  -- numerator: sum of per-device active_days; denominator: active_devices ×
  -- calendar days. >100% would indicate a data bug.
  CASE WHEN dm.days > 0 AND s.active_devices > 0
       THEN LEAST(
         da.device_active_days_sum::FLOAT
           / (s.active_devices * dm.days) * 100,
         100.0
       )
       ELSE 0 END                                              AS utilization_rate_pct,
  COALESCE(da.idle_time_pct, 0)                                AS idle_time_pct,
  CASE WHEN s.active_devices > 0 AND dm.days > 0
       THEN s.total_trips::FLOAT / (s.active_devices * dm.days)
       ELSE 0 END                                              AS trips_per_device_per_day,
  -- ---- Activity Averages ----
  COALESCE(da.avg_trip_duration_min, 0)                        AS avg_trip_duration_min,
  CASE WHEN s.active_devices > 0
       THEN s.total_distance_km / s.active_devices
       ELSE 0 END                                              AS avg_distance_per_device,
  -- ---- MoM Trend (% change vs prior month) ----
  -- COALESCE because the first month per tenant has no prior to compare.
  -- NULLIF on the denominator avoids divide-by-zero blow-ups.
  COALESCE(
    (s.total_distance_km - LAG(s.total_distance_km) OVER w)
      / NULLIF(LAG(s.total_distance_km) OVER w, 0) * 100,
    0
  )                                                            AS distance_trend_mom_pct,
  COALESCE(
    (s.total_operating_cost - LAG(s.total_operating_cost) OVER w)
      / NULLIF(LAG(s.total_operating_cost) OVER w, 0) * 100,
    0
  )                                                            AS cost_trend_mom_pct
FROM marts.mart_tenant_monthly_summary s
LEFT JOIN device_agg    da USING (tenant_id, year_month)
LEFT JOIN vehicle_agg   va USING (tenant_id, year_month)
LEFT JOIN days_in_month dm USING (year_month)
WINDOW w AS (PARTITION BY s.tenant_id ORDER BY s.year_month);
