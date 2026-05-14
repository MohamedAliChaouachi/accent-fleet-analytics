-- =============================================================================
-- 53_views_security_invoker.sql
--
-- Bug fix for the RLS-through-views bypass discovered during M6 B6b verification
-- (2026-05-14). The comment in sql/51_rls_policies.sql at lines 160-164 claimed
-- "Postgres rewrites views into the underlying tables at plan time, so the
-- policies on the base tables also apply to anyone querying the view." That
-- claim is wrong for the default view mode.
--
-- =============================================================================
-- What actually happens (Postgres default: security_definer-style views)
-- =============================================================================
-- A view's underlying RLS is evaluated as the VIEW OWNER, not the querier.
-- Combined with the Postgres rule "table owners are exempt from their own
-- RLS unless FORCE ROW LEVEL SECURITY is set", a view owned by the same
-- role that owns the base tables (here, `medamine_dev`) bypasses RLS for
-- every caller — including a NOBYPASSRLS role like `accent_app`.
--
-- Reproduction (as accent_app, NOBYPASSRLS):
--     BEGIN;
--     SET LOCAL app.current_tenant = '235';
--     SELECT COUNT(*), MIN(tenant_id), MAX(tenant_id)
--       FROM marts.v_device_risk_profile;
--     -- observed: 528 rows, min=235, max=7486   <-- LEAK
--     ROLLBACK;
--
-- =============================================================================
-- Fix
-- =============================================================================
-- PG 15 added `security_invoker` as a view option. When `true`, the view's
-- underlying tables are accessed (and their RLS evaluated) as the CURRENT_USER
-- of the query, not the view owner. Since `accent_app` does not own the marts
-- tables, it is subject to RLS. The `tenant_isolation` policy from
-- sql/51_rls_policies.sql then compares `tenant_id::text` to the GUC
-- `app.current_tenant` (set by the SQLAlchemy `begin` listener in
-- src/accent_fleet/db/engine.py) — cross-tenant rows are filtered out.
--
-- Why not FORCE ROW LEVEL SECURITY on the base tables instead?
-- -----------------------------------------------------------
-- That would also work, but it would immediately break Prefect ETL — which at
-- v0.9.0 still connects as `medamine_dev`, the table owner. Setting
-- security_invoker on the views fixes the API leak without affecting writes,
-- because ETL paths don't go through the v_* views. When B7 cuts Prefect over
-- to `accent_etl` (BYPASSRLS), we can add FORCE ROW LEVEL SECURITY as defense
-- in depth.
--
-- =============================================================================
-- Idempotency
-- =============================================================================
-- ALTER VIEW ... SET (security_invoker = true) is idempotent: setting an
-- already-set option is a no-op. Safe to re-run.
--
-- =============================================================================
-- Rollback (NOT RECOMMENDED — re-opens the cross-tenant leak)
-- =============================================================================
--   ALTER VIEW marts.v_device_risk_profile        RESET (security_invoker);
--   ALTER VIEW marts.v_ml_features_driver_behavior RESET (security_invoker);
--   ALTER VIEW marts.v_ml_features_full           RESET (security_invoker);
--   ALTER VIEW marts.v_fleet_risk_dashboard       RESET (security_invoker);
--   ALTER VIEW marts.v_executive_dashboard        RESET (security_invoker);
--   ALTER VIEW marts.v_operational_dashboard      RESET (security_invoker);
--   ALTER VIEW marts.v_maintenance_dashboard      RESET (security_invoker);
-- =============================================================================

\set ON_ERROR_STOP on

-- Sanity: refuse to run on PG < 15 (security_invoker doesn't exist).
DO $$
DECLARE
    v_major int := current_setting('server_version_num')::int / 10000;
BEGIN
    IF v_major < 15 THEN
        RAISE EXCEPTION
            'security_invoker requires PostgreSQL 15+. This server is %.',
            current_setting('server_version');
    END IF;
END $$;

-- Apply security_invoker to every marts.v_* view. Wrapped in a DO so that a
-- missing view (partial schema, e.g. dev DBs without dashboards) raises a
-- NOTICE instead of aborting the whole transaction. The verification block
-- below catches drift.
DO $$
DECLARE
    v_name text;
    v_views text[] := ARRAY[
        'v_device_risk_profile',
        'v_ml_features_driver_behavior',
        'v_ml_features_full',
        'v_fleet_risk_dashboard',
        'v_executive_dashboard',
        'v_operational_dashboard',
        'v_maintenance_dashboard'
    ];
BEGIN
    FOREACH v_name IN ARRAY v_views LOOP
        IF EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'marts'
              AND c.relname = v_name
              AND c.relkind = 'v'
        ) THEN
            EXECUTE format(
                'ALTER VIEW marts.%I SET (security_invoker = true)',
                v_name
            );
            RAISE NOTICE 'security_invoker enabled on marts.%', v_name;
        ELSE
            RAISE NOTICE 'skipping marts.% (view not present)', v_name;
        END IF;
    END LOOP;
END $$;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
-- Every marts.v_* view should now carry security_invoker=true in reloptions.
-- This query returns one row per view that is still missing the option — empty
-- output means we're clean.
DO $$
DECLARE
    leaky_count int;
BEGIN
    SELECT COUNT(*) INTO leaky_count
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'marts'
      AND c.relkind = 'v'
      AND c.relname LIKE 'v\_%' ESCAPE '\'
      AND NOT (
        c.reloptions IS NOT NULL
        AND 'security_invoker=true' = ANY (c.reloptions)
      );

    IF leaky_count > 0 THEN
        RAISE WARNING
            '53_views_security_invoker: % marts.v_* view(s) still lack security_invoker=true',
            leaky_count;
    ELSE
        RAISE NOTICE
            '53_views_security_invoker: all marts.v_* views are security_invoker=true';
    END IF;
END $$;
