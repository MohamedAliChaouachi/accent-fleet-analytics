-- =============================================================================
-- 35_v_maintenance_dashboard.sql
-- =============================================================================
-- Maintenance-focused BI view. One row per (tenant_id, vehicle_id, year_month)
-- with cost mix, repair turnaround, and "is this vehicle a money pit?" metrics
-- the workshop manager needs at a glance.
--
-- Dashboard usage:
--   - Vehicle leaderboard:
--       SELECT * FROM marts.v_maintenance_dashboard
--       WHERE tenant_id = :t AND year_month = :ym
--       ORDER BY total_cost DESC LIMIT 20;
--   - Trend per vehicle:
--       SELECT * FROM marts.v_maintenance_dashboard
--       WHERE tenant_id = :t AND vehicle_id = :v ORDER BY year_month;
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_maintenance_dashboard AS
SELECT
  mv.tenant_id,
  mv.vehicle_id,
  dv.matricule,
  dv.mark_clean                                              AS vehicle_mark,
  dv.vehicle_class,
  mv.year_month,
  -- Activity
  mv.total_distance_km,
  mv.active_days,
  -- Maintenance counts
  mv.maintenance_events,
  mv.offense_events,
  mv.sinistre_events,
  mv.reparation_events,
  -- Costs
  mv.maintenance_cost_total,
  mv.maintenance_labor_total,
  mv.reparation_amount_total,
  mv.fuel_cost_total,
  mv.maintenance_cost_total + mv.fuel_cost_total             AS total_cost,
  -- Mix percentages
  CASE WHEN (mv.maintenance_cost_total + mv.fuel_cost_total) > 0
       THEN mv.maintenance_cost_total
            / (mv.maintenance_cost_total + mv.fuel_cost_total) * 100
       ELSE 0 END                                             AS maintenance_share_pct,
  -- Per-km efficiency (0 when no driving — BI-friendly default)
  mv.cost_per_km,
  mv.fuel_l_per_100km,
  -- Repair turnaround for the month (avg hours; 0 when no repairs)
  COALESCE(rep.avg_repair_hours, 0)                            AS avg_repair_hours,
  COALESCE(rep.max_repair_hours, 0)                            AS max_repair_hours,
  -- "Money pit" rank within tenant for the month
  RANK() OVER (
    PARTITION BY mv.tenant_id, mv.year_month
    ORDER BY (mv.maintenance_cost_total + mv.fuel_cost_total) DESC
  )                                                           AS cost_rank_in_tenant
FROM marts.mart_vehicle_monthly mv
LEFT JOIN warehouse.dim_vehicle dv
  ON dv.tenant_id  = mv.tenant_id
 AND dv.vehicule_id = mv.vehicle_id
LEFT JOIN LATERAL (
  SELECT
    AVG(fm.repair_duration_hours) AS avg_repair_hours,
    MAX(fm.repair_duration_hours) AS max_repair_hours
  FROM warehouse.fact_maintenance fm
  WHERE fm.tenant_id  = mv.tenant_id
    AND fm.vehicle_id = mv.vehicle_id
    AND TO_CHAR(fm.date_operation, 'YYYY-MM') = mv.year_month
) rep ON TRUE;
