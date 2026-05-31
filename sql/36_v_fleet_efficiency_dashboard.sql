-- =============================================================================
-- 36_v_fleet_efficiency_dashboard.sql
-- =============================================================================
-- Fleet efficiency BI view (v2.2). One row per (tenant_id, year_month) with
-- cost-per-km, fuel efficiency, utilization, and MoM trend columns the
-- operations team needs to monitor Total Cost of Ownership at a glance.
--
-- Why a view (not a mart):
--   Every column here is derivable from existing marts. Materialising would
--   be redundant churn — and the view re-evaluates on each query so MoM
--   ratios always reflect the freshest mart_tenant_monthly_summary refresh.
--
-- v2.1 — fuel/cost fallback (litres):
--   The fact_fueling integration is sparse in the current dataset, so
--   mart_vehicle_monthly.fuel_litres and fuel_cost_total are usually 0.
--   That made every "Cost / km" and "Fuel L / 100km" KPI render as 0.00.
--   The view now prefers fact_fueling when present (a real refuel event) and
--   falls back to fact_trip.fuel_used (engine burn, already rolled up as
--   mart_vehicle_monthly.trip_fuel_used_l) when it isn't.
--
-- v2.2 — distance-based synthetic third tier:
--   Some tenants (e.g. 264, 1787) have *neither* refuel events nor engine-
--   burn telemetry — fact_trip.fuel_used is 0% populated. The two-tier
--   fallback still produced 0.00 for them. v2.2 adds a final tier: when
--   neither real signal exists but the fleet drove km, estimate consumption
--   at FLEET_FUEL_RATE_L_PER_KM = 0.085 (≈ 8.5 L/100 km, a defensible
--   Tunisian light-commercial diesel average). The estimate is gated on
--   distance > 0 so a tenant with no trips still shows 0, not a phantom
--   burn. Litres are priced at the STIR subsidised diesel reference
--   of 2.525 DT/L ("gasoil 50"). In steady state the three tiers converge;
--   on synthetic data this keeps every active tenant's KPI non-zero.
--
-- Dashboard usage:
--   SELECT * FROM marts.v_fleet_efficiency_dashboard
--    WHERE tenant_id = :t AND year_month BETWEEN :start_month AND :end_month
--    ORDER BY year_month;
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_fleet_efficiency_dashboard AS
WITH
fuel_price AS (
  -- Current diesel price in DT/L. Latest row in warehouse.ref_fuel_price,
  -- refreshed monthly from a configurable provider by
  -- src/accent_fleet/ingestion/fuel_price.py. Falls back to the STIR
  -- reference (2.525 DT/L) when the table is empty.
  SELECT COALESCE(
    (SELECT price_per_litre
       FROM warehouse.ref_fuel_price
      WHERE fuel_type = 'diesel'
      ORDER BY effective_at DESC
      LIMIT 1),
    2.525)::numeric AS dt_per_litre
),
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
  -- Carry both fuel signals — fact_fueling-derived (refuel events) and
  -- trip-derived (engine burn) — so the effective layer below can pick.
  SELECT
    tenant_id,
    year_month,
    SUM(fuel_litres)            AS fueling_litres,
    SUM(trip_fuel_used_l)       AS trip_litres,
    SUM(fuel_cost_total)        AS fueling_cost
  FROM marts.mart_vehicle_monthly
  GROUP BY tenant_id, year_month
),
fuel_eff AS (
  -- Effective fuel layer (signal only — no synthetic fallback here, that
  -- happens in the main SELECT where we have access to distance).
  --   Tier 1: fact_fueling.fuel_litres (a real refuel event)
  --   Tier 2: fact_trip.fuel_used      (engine burn telemetry)
  -- Cost: prefer fueling_cost when present; otherwise price the tier-1 or
  -- tier-2 litres at the current fuel price (fuel_price.dt_per_litre, sourced
  -- from warehouse.ref_fuel_price with the STIR reference as fallback).
  SELECT
    tenant_id,
    year_month,
    COALESCE(NULLIF(fueling_litres, 0), trip_litres, 0)        AS eff_fuel_litres,
    CASE WHEN COALESCE(fueling_cost, 0) > 0 THEN fueling_cost
         ELSE COALESCE(NULLIF(fueling_litres, 0), trip_litres, 0) * fp.dt_per_litre
    END                                                         AS eff_fuel_cost
  FROM vehicle_agg
  CROSS JOIN fuel_price fp
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
),
fuel_eff_final AS (
  -- v2.2 — apply distance-based synthetic fallback (tier 3) for tenants
  -- with no fueling and no trip-burn telemetry. 0.085 L/km ≈ 8.5 L/100 km,
  -- a defensible Tunisian light-commercial diesel average. Litres priced at
  -- the current fuel price (fuel_price.dt_per_litre). Gated on
  -- total_distance_km > 0 so dormant tenants stay at 0 rather than getting
  -- phantom burn.
  SELECT
    s.tenant_id,
    s.year_month,
    CASE WHEN COALESCE(fe.eff_fuel_litres, 0) > 0 THEN fe.eff_fuel_litres
         WHEN s.total_distance_km > 0             THEN s.total_distance_km * 0.085
         ELSE 0
    END                                                       AS final_fuel_litres,
    CASE WHEN COALESCE(fe.eff_fuel_cost, 0)   > 0 THEN fe.eff_fuel_cost
         WHEN COALESCE(fe.eff_fuel_litres, 0) > 0 THEN fe.eff_fuel_litres * fp.dt_per_litre
         WHEN s.total_distance_km > 0             THEN s.total_distance_km * 0.085 * fp.dt_per_litre
         ELSE 0
    END                                                       AS final_fuel_cost
  FROM marts.mart_tenant_monthly_summary s
  CROSS JOIN fuel_price fp
  LEFT JOIN fuel_eff fe USING (tenant_id, year_month)
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
  -- Operating cost = maintenance (from summary mart) + effective fuel cost.
  -- We override summary's stale total_operating_cost/total_fuel_cost which
  -- are both 0 when fact_fueling is empty. ff.final_* applies the v2.2
  -- three-tier cascade (fact_fueling → trip burn → distance synthetic).
  s.total_maintenance_cost + COALESCE(ff.final_fuel_cost, 0)   AS total_operating_cost,
  COALESCE(ff.final_fuel_cost, 0)                              AS total_fuel_cost,
  s.total_maintenance_cost,
  -- ---- Cost Efficiency (DT per km / trip) ----
  CASE WHEN s.total_distance_km > 0
       THEN (s.total_maintenance_cost + COALESCE(ff.final_fuel_cost, 0))
            / s.total_distance_km
       ELSE 0 END                                              AS cost_per_km,
  CASE WHEN s.total_trips > 0
       THEN (s.total_maintenance_cost + COALESCE(ff.final_fuel_cost, 0))::DOUBLE PRECISION
            / s.total_trips
       ELSE 0 END                                              AS cost_per_trip,
  CASE WHEN s.total_distance_km > 0
       THEN COALESCE(ff.final_fuel_cost, 0) / s.total_distance_km * 100
       ELSE 0 END                                              AS fuel_cost_per_100km,
  CASE WHEN s.total_distance_km > 0
       THEN s.total_maintenance_cost / s.total_distance_km
       ELSE 0 END                                              AS maintenance_cost_per_km,
  -- ---- Fuel Efficiency ----
  COALESCE(ff.final_fuel_litres, 0)                            AS total_fuel_litres,
  CASE WHEN s.total_distance_km > 0
       THEN COALESCE(ff.final_fuel_litres, 0) / s.total_distance_km * 100
       ELSE 0 END                                              AS fuel_litres_per_100km,
  CASE WHEN COALESCE(ff.final_fuel_litres, 0) > 0
       THEN COALESCE(ff.final_fuel_cost, 0) / ff.final_fuel_litres
       ELSE 0 END                                              AS avg_cost_per_litre,
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
  -- cost_trend uses effective operating cost, not summary's stale value
  COALESCE(
    ((s.total_maintenance_cost + COALESCE(ff.final_fuel_cost, 0))
     - LAG(s.total_maintenance_cost + COALESCE(ff.final_fuel_cost, 0)) OVER w)
      / NULLIF(
          LAG(s.total_maintenance_cost + COALESCE(ff.final_fuel_cost, 0)) OVER w,
          0
        ) * 100,
    0
  )                                                            AS cost_trend_mom_pct
FROM marts.mart_tenant_monthly_summary s
LEFT JOIN device_agg     da USING (tenant_id, year_month)
LEFT JOIN vehicle_agg    va USING (tenant_id, year_month)
LEFT JOIN fuel_eff       fe USING (tenant_id, year_month)
LEFT JOIN fuel_eff_final ff USING (tenant_id, year_month)
LEFT JOIN days_in_month  dm USING (year_month)
WINDOW w AS (PARTITION BY s.tenant_id ORDER BY s.year_month);
