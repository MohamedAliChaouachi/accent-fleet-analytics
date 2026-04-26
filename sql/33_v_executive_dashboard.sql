-- =============================================================================
-- 33_v_executive_dashboard.sql
-- =============================================================================
-- Top-level BI view for the executive dashboard. Picks the most recent 12
-- months of mart_tenant_monthly_summary and adds month-over-month delta
-- columns so a dashboard can render trend cards directly.
--
-- Dashboard usage:
--   SELECT * FROM marts.v_executive_dashboard WHERE tenant_id = :t;
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_executive_dashboard AS
SELECT
  s.tenant_id,
  s.year_month,
  s.active_vehicles,
  s.active_devices,
  s.total_trips,
  s.total_distance_km,
  s.total_maintenance_cost,
  s.total_fuel_cost,
  s.total_operating_cost,
  s.cost_per_km,
  s.total_alerts,
  s.panic_alerts,
  s.total_overspeed,
  s.total_harsh_events,
  -- MoM deltas (NULL for the earliest row per tenant)
  s.total_distance_km - LAG(s.total_distance_km) OVER w        AS distance_km_mom_delta,
  s.total_operating_cost - LAG(s.total_operating_cost) OVER w  AS operating_cost_mom_delta,
  s.total_alerts - LAG(s.total_alerts) OVER w                  AS alerts_mom_delta,
  -- 3-month rolling averages (smooth out monthly noise)
  AVG(s.total_distance_km) OVER w_roll                          AS distance_km_3mo_avg,
  AVG(s.cost_per_km) OVER w_roll                                AS cost_per_km_3mo_avg
FROM marts.mart_tenant_monthly_summary s
WINDOW
  w      AS (PARTITION BY s.tenant_id ORDER BY s.year_month),
  w_roll AS (PARTITION BY s.tenant_id ORDER BY s.year_month
             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
