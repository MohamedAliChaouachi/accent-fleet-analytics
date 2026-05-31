-- =============================================================================
-- 39_v_tenant_billing_dashboard.sql
-- =============================================================================
-- Tenant billing BI view (v2.0). One row per (tenant_id, year_month) with
-- the usage metrics Accent's internal billing/finance team needs to:
--   - tier tenants by device count
--   - track usage MoM (for QoQ planning)
--   - estimate revenue and infrastructure cost allocation
--
-- Pricing model (v2.0 placeholder — wire to a real pricing table later):
--   Tier 1 (1-50 devices):    100 DT / device / month
--   Tier 2 (51-200 devices):  80 DT / device / month
--   Tier 3 (201+ devices):    60 DT / device / month
--
-- Storage is approximated from row counts × avg per-row footprint per fact
-- table. A true pg_total_relation_size split per-tenant requires partition-
-- aware schemas; until then this estimate is good enough for trending.
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_tenant_billing_dashboard AS
WITH
-- ---- Per-tenant device registry counts ----
-- Active = registered AND seen in the last 90 days.
device_counts AS (
  SELECT
    tenant_id,
    COUNT(*)                                                   AS total_devices_registered,
    COUNT(*) FILTER (WHERE _loaded_at > NOW() - INTERVAL '90 days')
                                                               AS active_devices_registered
  FROM warehouse.dim_device
  GROUP BY tenant_id
),
-- ---- Per-tenant × month usage ----
-- Reuses the already-aggregated tenant summary; cheap and consistent with
-- the rest of the BI layer.
usage AS (
  SELECT
    s.tenant_id,
    s.year_month,
    s.active_devices,
    s.active_vehicles,
    s.total_trips,
    s.total_distance_km,
    s.total_alerts,
    s.total_harsh_events,
    s.total_overspeed,
    s.total_operating_cost
  FROM marts.mart_tenant_monthly_summary s
),
-- ---- Storage estimate (MB) per tenant ----
-- ~120 bytes/row across our fact tables, conservatively averaged. The exact
-- per-row size varies by table (fact_trip is wide; fact_notification narrow),
-- but for trending purposes a single coefficient is good enough.
storage_est AS (
  SELECT
    t.tenant_id,
    (
      COALESCE((SELECT COUNT(*) FROM warehouse.fact_trip          WHERE tenant_id = t.tenant_id), 0) * 200
      + COALESCE((SELECT COUNT(*) FROM warehouse.fact_overspeed   WHERE tenant_id = t.tenant_id), 0) * 100
      + COALESCE((SELECT COUNT(*) FROM warehouse.fact_harsh_event WHERE tenant_id = t.tenant_id), 0) * 120
      + COALESCE((SELECT COUNT(*) FROM warehouse.fact_notification WHERE tenant_id = t.tenant_id), 0) * 150
      + COALESCE((SELECT COUNT(*) FROM warehouse.fact_maintenance WHERE tenant_id = t.tenant_id), 0) * 180
      + COALESCE((SELECT COUNT(*) FROM warehouse.fact_fueling     WHERE tenant_id = t.tenant_id), 0) * 160
    )::FLOAT / 1024 / 1024                                     AS storage_mb_total
  FROM warehouse.dim_tenant t
),
-- ---- Days in each month — for "avg daily events" ----
days_in_month AS (
  SELECT
    year_month,
    EXTRACT(DAY FROM
      (TO_DATE(year_month || '-01', 'YYYY-MM-DD')
       + INTERVAL '1 month' - INTERVAL '1 day')
    )::INTEGER                                                AS days
  FROM (SELECT DISTINCT year_month FROM marts.mart_tenant_monthly_summary) ym
)
SELECT
  u.tenant_id,
  dt.tenant_label                                              AS tenant_name,
  u.year_month,
  -- ---- Resource Counts ----
  u.active_devices,
  u.active_vehicles,
  COALESCE(dc.total_devices_registered, 0)                     AS total_devices_registered,
  COALESCE(dc.active_devices_registered, 0)                    AS active_devices_registered,
  -- ---- Usage Metrics ----
  u.total_trips                                                AS total_trips_processed,
  u.total_distance_km,
  u.total_alerts                                               AS total_alerts_generated,
  u.total_harsh_events,
  u.total_overspeed                                            AS total_overspeed_events,
  (u.total_trips + u.total_alerts + u.total_harsh_events + u.total_overspeed)
                                                               AS total_events,
  -- ---- Data Volume ----
  COALESCE(se.storage_mb_total, 0) / 1024                      AS data_volume_gb,
  COALESCE(se.storage_mb_total, 0)                             AS db_storage_mb,
  CASE WHEN dm.days > 0
       THEN (u.total_trips + u.total_alerts + u.total_harsh_events + u.total_overspeed)::FLOAT
            / dm.days
       ELSE 0 END                                              AS avg_daily_events,
  -- ---- Efficiency ----
  CASE WHEN u.active_devices > 0
       THEN (u.total_trips + u.total_alerts + u.total_harsh_events + u.total_overspeed)::FLOAT
            / u.active_devices
       ELSE 0 END                                              AS events_per_device,
  CASE WHEN u.total_trips > 0
       THEN u.total_alerts::FLOAT / u.total_trips
       ELSE 0 END                                              AS alerts_per_trip,
  CASE WHEN u.active_devices > 0
       THEN COALESCE(se.storage_mb_total, 0) / u.active_devices
       ELSE 0 END                                              AS data_per_device_mb,
  -- ---- Pricing Tier ----
  CASE
    WHEN u.active_devices <=  50 THEN 1
    WHEN u.active_devices <= 200 THEN 2
    ELSE 3
  END                                                          AS pricing_tier,
  CASE
    WHEN u.active_devices <=  50 THEN 100.0
    WHEN u.active_devices <= 200 THEN  80.0
    ELSE                                60.0
  END                                                          AS price_per_device,
  -- ---- Revenue Estimate ----
  u.active_devices *
    CASE
      WHEN u.active_devices <=  50 THEN 100.0
      WHEN u.active_devices <= 200 THEN  80.0
      ELSE                                60.0
    END                                                        AS estimated_revenue,
  -- ---- Infrastructure Cost Allocation ----
  -- Rule of thumb: 25% of revenue covers infrastructure. Plug in real
  -- numbers from finance once available.
  u.active_devices *
    CASE
      WHEN u.active_devices <=  50 THEN 100.0
      WHEN u.active_devices <= 200 THEN  80.0
      ELSE                                60.0
    END * 0.25                                                 AS estimated_infra_cost,
  -- ---- MoM Growth ----
  COALESCE(
    (u.active_devices - LAG(u.active_devices) OVER w)
      / NULLIF(LAG(u.active_devices) OVER w, 0)::FLOAT * 100,
    0
  )                                                            AS devices_mom_growth_pct,
  COALESCE(
    (u.total_trips - LAG(u.total_trips) OVER w)
      / NULLIF(LAG(u.total_trips) OVER w, 0)::FLOAT * 100,
    0
  )                                                            AS trips_mom_growth_pct,
  COALESCE(
    (u.total_alerts - LAG(u.total_alerts) OVER w)
      / NULLIF(LAG(u.total_alerts) OVER w, 0)::FLOAT * 100,
    0
  )                                                            AS alerts_mom_growth_pct
FROM usage u
LEFT JOIN warehouse.dim_tenant dt USING (tenant_id)
LEFT JOIN device_counts dc        USING (tenant_id)
LEFT JOIN storage_est   se        USING (tenant_id)
LEFT JOIN days_in_month dm        USING (year_month)
WINDOW w AS (PARTITION BY u.tenant_id ORDER BY u.year_month);
