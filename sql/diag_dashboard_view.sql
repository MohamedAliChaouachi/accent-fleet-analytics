-- Sanity: prove that marts.v_executive_dashboard returns rows for accent_app
-- when the same SET LOCAL ROLE / GUC paths the engine listener uses are in
-- place. If these all return rows, the DB side is healthy and the bug lives
-- in the API auth path.

\echo === as postgres (RLS bypassed) -- baseline ===
SELECT COUNT(*) AS rows FROM marts.v_executive_dashboard;

\echo
\echo === as accent_app with NO GUC (simulates unauthenticated request) ===
BEGIN;
SET LOCAL ROLE accent_app;
SELECT COUNT(*) AS rows FROM marts.v_executive_dashboard;
COMMIT;

\echo
\echo === as accent_app with tenant 235 (simulates tenant_user login) ===
BEGIN;
SET LOCAL ROLE accent_app;
SELECT set_config('app.current_tenant', '235', true);
SELECT COUNT(*) AS rows FROM marts.v_executive_dashboard;
COMMIT;

\echo
\echo === as accent_app, escalated via accent_superadmin (simulates superadmin login) ===
BEGIN;
SET LOCAL ROLE accent_app;
SET LOCAL ROLE accent_superadmin;
SELECT COUNT(*) AS rows FROM marts.v_executive_dashboard;
COMMIT;
