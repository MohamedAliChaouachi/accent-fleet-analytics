-- =============================================================================
-- 33_v_executive_dashboard.sql
-- =============================================================================
-- Top-level BI view for the executive dashboard (v1.1). Picks the most
-- recent 12 months of mart_tenant_monthly_summary and adds month-over-month
-- delta columns so a dashboard can render trend cards directly.
--
-- v1.1 — fuel/cost cascade (mirrors v_fleet_efficiency_dashboard v2.2):
--   mart_tenant_monthly_summary.total_fuel_cost / total_operating_cost /
--   cost_per_km are 0 when fact_fueling is empty (which it is on the
--   current synthetic dataset). The Executive overview's COST / KM card
--   showed 0.00 because of that.
--
--   This view now derives a corrected fuel cost using a three-tier
--   cascade — fact_fueling → fact_trip.fuel_used → distance synthetic
--   (0.085 L/km, priced at the current fuel price) — and republishes
--   total_fuel_cost, total_operating_cost and cost_per_km from it. Activity
--   columns (devices, trips, km, alerts) pass through unchanged.
--
-- Fuel price (DT/L) is read live from warehouse.ref_fuel_price (latest row
-- per fuel_type), refreshed monthly from a configurable provider by
-- src/accent_fleet/ingestion/fuel_price.py. If that table is empty the
-- COALESCE falls back to the 2.525 DT/L STIR reference, so the cascade
-- always has a price.
--
-- Dashboard usage:
--   SELECT * FROM marts.v_executive_dashboard WHERE tenant_id = :t;
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_executive_dashboard AS
WITH
fuel_price AS (
  -- Current diesel price in DT/L. Latest row in warehouse.ref_fuel_price,
  -- with the STIR reference (2.525) as the empty-table fallback.
  SELECT COALESCE(
    (SELECT price_per_litre
       FROM warehouse.ref_fuel_price
      WHERE fuel_type = 'diesel'
      ORDER BY effective_at DESC
      LIMIT 1),
    2.525)::numeric AS dt_per_litre
),
vehicle_agg AS (
  -- Roll mart_vehicle_monthly up to tenant × month for the two real fuel
  -- signals (fueling-fact litres/cost; trip-burn telemetry litres).
  SELECT
    tenant_id,
    year_month,
    SUM(fuel_litres)        AS fueling_litres,
    SUM(trip_fuel_used_l)   AS trip_litres,
    SUM(fuel_cost_total)    AS fueling_cost
  FROM marts.mart_vehicle_monthly
  GROUP BY tenant_id, year_month
),
fuel_eff_final AS (
  -- Three-tier cascade. Mirrors v_fleet_efficiency_dashboard so KPIs on
  -- the two pages agree. 0.085 L/km ≈ 8.5 L/100 km synthetic estimate
  -- for tenants with no fueling fact AND no trip burn. 2.525 DT/L is the
  -- Tunisian STIR subsidised diesel ("gasoil 50") reference price.
  SELECT
    s.tenant_id,
    s.year_month,
    CASE WHEN COALESCE(va.fueling_cost, 0) > 0 THEN va.fueling_cost
         WHEN COALESCE(NULLIF(va.fueling_litres, 0), va.trip_litres, 0) > 0
              THEN COALESCE(NULLIF(va.fueling_litres, 0), va.trip_litres, 0) * fp.dt_per_litre
         WHEN s.total_distance_km > 0
              THEN s.total_distance_km * 0.085 * fp.dt_per_litre
         ELSE 0
    END                                                       AS final_fuel_cost
  FROM marts.mart_tenant_monthly_summary s
  CROSS JOIN fuel_price fp
  LEFT JOIN vehicle_agg va USING (tenant_id, year_month)
)
SELECT
  s.tenant_id,
  s.year_month,
  s.active_vehicles,
  s.active_devices,
  s.total_trips,
  s.total_distance_km,
  s.total_maintenance_cost,
  -- v1.1: republish corrected fuel + operating cost from the cascade.
  COALESCE(ff.final_fuel_cost, 0)                                          AS total_fuel_cost,
  s.total_maintenance_cost + COALESCE(ff.final_fuel_cost, 0)               AS total_operating_cost,
  CASE WHEN s.total_distance_km > 0
       THEN (s.total_maintenance_cost + COALESCE(ff.final_fuel_cost, 0))
            / s.total_distance_km
       ELSE 0 END                                                          AS cost_per_km,
  s.total_alerts,
  s.panic_alerts,
  s.total_overspeed,
  s.total_harsh_events,
  -- MoM deltas (0 for the earliest row per tenant — no prior month to compare).
  -- Cost delta uses the corrected operating cost so the trend stays consistent.
  COALESCE(s.total_distance_km - LAG(s.total_distance_km) OVER w, 0)       AS distance_km_mom_delta,
  COALESCE(
    (s.total_maintenance_cost + COALESCE(ff.final_fuel_cost, 0))
    - LAG(s.total_maintenance_cost + COALESCE(ff.final_fuel_cost, 0)) OVER w,
    0
  )                                                                        AS operating_cost_mom_delta,
  COALESCE(s.total_alerts - LAG(s.total_alerts) OVER w, 0)                 AS alerts_mom_delta,
  -- 3-month rolling averages (smooth out monthly noise).
  COALESCE(AVG(s.total_distance_km) OVER w_roll, 0)                        AS distance_km_3mo_avg,
  COALESCE(
    AVG(
      CASE WHEN s.total_distance_km > 0
           THEN (s.total_maintenance_cost + COALESCE(ff.final_fuel_cost, 0))
                / s.total_distance_km
           ELSE 0 END
    ) OVER w_roll,
    0
  )                                                                        AS cost_per_km_3mo_avg
FROM marts.mart_tenant_monthly_summary s
LEFT JOIN fuel_eff_final ff USING (tenant_id, year_month)
WINDOW
  w      AS (PARTITION BY s.tenant_id ORDER BY s.year_month),
  w_roll AS (PARTITION BY s.tenant_id ORDER BY s.year_month
             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
