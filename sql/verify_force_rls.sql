-- =============================================================================
-- verify_force_rls.sql  --  NOT a migration.  Read-only smoke test.
--
-- Walks every role's view of warehouse.dim_vehicle after sql/55_force_rls.sql.
-- Run as the cluster `postgres` superuser:
--     sudo -u postgres psql -d accent_data -f /tmp/verify_force_rls.sql
--
-- session_user stays `postgres` throughout, but each test uses
-- `SET LOCAL ROLE` so PG evaluates RLS against the *current* role's
-- BYPASSRLS attribute. `SET LOCAL` reverts at COMMIT, so each test is
-- hermetic.
--
-- Expected outcomes (printed in the COUNT column):
--   baseline                  : N rows total, M distinct tenants
--   accent_etl_bypassrls      : N   (BYPASSRLS beats FORCE)
--   accent_superadmin_bypass  : N   (BYPASSRLS beats FORCE)
--   accent_app_no_guc         : 0   (NOBYPASSRLS, missing GUC ⇒ policy NULL)
--   accent_app_with_guc       : rows for the one tenant we set
--   medamine_dev_force_clamp  : 0   (THE proof that FORCE actually works)
--   medamine_dev_elevated     : N   (elevation path via accent_superadmin)
-- =============================================================================

\echo
\echo === baseline as postgres (superuser, bypasses RLS) ===
SELECT 'baseline' AS step,
       COUNT(*) AS rows,
       COUNT(DISTINCT tenant_id) AS tenants
  FROM warehouse.dim_vehicle;

-- Pick a tenant_id that actually has rows so the "with GUC" test is meaningful.
SELECT MIN(tenant_id) AS pick_tenant
  FROM warehouse.dim_vehicle
\gset

\echo
\echo Using tenant_id = :pick_tenant for the per-tenant test
\echo

-- 1. accent_etl --------------------------------------------------------------
BEGIN;
SET LOCAL ROLE accent_etl;
SELECT 'accent_etl_bypassrls' AS step, COUNT(*) AS rows FROM warehouse.dim_vehicle;
COMMIT;

-- 2. accent_superadmin -------------------------------------------------------
BEGIN;
SET LOCAL ROLE accent_superadmin;
SELECT 'accent_superadmin_bypass' AS step, COUNT(*) AS rows FROM warehouse.dim_vehicle;
COMMIT;

-- 3. accent_app, no tenant GUC ----------------------------------------------
BEGIN;
SET LOCAL ROLE accent_app;
SELECT 'accent_app_no_guc' AS step, COUNT(*) AS rows FROM warehouse.dim_vehicle;
COMMIT;

-- 4. accent_app, tenant GUC set ---------------------------------------------
BEGIN;
SET LOCAL ROLE accent_app;
SELECT set_config('app.current_tenant', :'pick_tenant'::text, true);
SELECT 'accent_app_with_guc' AS step,
       COUNT(*) AS rows,
       COUNT(DISTINCT tenant_id) AS tenants
  FROM warehouse.dim_vehicle;
COMMIT;

-- 5. medamine_dev — the FORCE-RLS canary -------------------------------------
-- Without elevation, the table OWNER must now see 0 rows because no
-- app.current_tenant is set. If this row shows > 0, FORCE didn't take.
BEGIN;
SET LOCAL ROLE medamine_dev;
SELECT 'medamine_dev_force_clamp' AS step, COUNT(*) AS rows FROM warehouse.dim_vehicle;
COMMIT;

-- 6. medamine_dev elevation path ---------------------------------------------
-- After SET LOCAL ROLE accent_superadmin the operator regains visibility
-- (this is the escape hatch sql/57 + sql/54 wire up).
BEGIN;
SET LOCAL ROLE medamine_dev;
SET LOCAL ROLE accent_superadmin;
SELECT 'medamine_dev_elevated' AS step, COUNT(*) AS rows FROM warehouse.dim_vehicle;
COMMIT;

\echo
\echo === verification complete ===
