-- Quick diagnostic: is marts.mart_tenant_monthly_summary populated?
-- Run as postgres so RLS is bypassed and we see ground truth.

\echo === row counts on the canary mart ===
SELECT COUNT(*)                AS rows,
       COUNT(DISTINCT tenant_id) AS tenants,
       MIN(year_month)         AS earliest,
       MAX(year_month)         AS latest
  FROM marts.mart_tenant_monthly_summary;

\echo
\echo === per-tenant per-month counts (truncated to last 12) ===
SELECT tenant_id, year_month, 1 AS rows_per_tenant_month
  FROM marts.mart_tenant_monthly_summary
 ORDER BY year_month DESC, tenant_id
 LIMIT 30;

\echo
\echo === also check the upstream fact (so we know whether the rebuild has data to chew on) ===
SELECT COUNT(*) AS fact_trip_rows,
       MIN(trip_start_ts) AS earliest_trip,
       MAX(trip_start_ts) AS latest_trip
  FROM warehouse.fact_trip;
