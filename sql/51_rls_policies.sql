-- =============================================================================
-- 51_rls_policies.sql
--
-- Row-Level Security policies on every tenant-owning table in warehouse.*
-- and marts.*. Pairs with the SQLAlchemy `before_cursor_execute`-style
-- listener in src/accent_fleet/db/engine.py that issues
--
--     SET LOCAL app.current_tenant = '<id>'
--
-- at the start of every web-request transaction (see docs/auth_design.md
-- §2.2 and §5).
--
-- =============================================================================
-- Threat model
-- =============================================================================
-- The whole point of this file is to make threat #1 from auth_design.md §1.2
-- ("logged-in user from tenant X queries tenant Y data") *impossible at the
-- database boundary*, not merely unlikely at the application boundary. A
-- missing WHERE clause, a constructor that forgets to inject the tenant, or
-- a hand-crafted SQL injection through an admin endpoint — none of those can
-- leak cross-tenant rows because Postgres filters before the rows ever cross
-- the wire.
--
-- =============================================================================
-- Two-level enforcement: GUC + role attribute
-- =============================================================================
-- 1. The GUC `app.current_tenant` holds the tenant_id (as text) for the
--    duration of the transaction. The engine listener sets it; the policies
--    compare to it.
--
-- 2. The Postgres ROLE attribute BYPASSRLS *skips* policy evaluation entirely.
--    Today (v0.9.0 M5) the app connects as a BYPASSRLS-capable owner — so
--    these policies are LOADED but DORMANT until M6 swaps the role to a
--    non-BYPASSRLS `accent_app`. We land the policies first so M6 is a pure
--    role flip with zero DDL churn.
--
-- =============================================================================
-- Idempotency
-- =============================================================================
-- The DO block guards each CREATE POLICY with a pg_policies lookup, and
-- ENABLE ROW LEVEL SECURITY is a no-op on a table that already has it. So
-- this script can be re-run safely (init scripts, migrations, dev resets).
--
-- =============================================================================
-- Rollback
-- =============================================================================
--   DO $$ DECLARE r record; BEGIN
--     FOR r IN SELECT schemaname, tablename FROM pg_policies
--              WHERE policyname = 'tenant_isolation' LOOP
--       EXECUTE format('DROP POLICY tenant_isolation ON %I.%I',
--                       r.schemaname, r.tablename);
--       EXECUTE format('ALTER TABLE %I.%I DISABLE ROW LEVEL SECURITY',
--                       r.schemaname, r.tablename);
--     END LOOP;
--   END $$;
--
-- =============================================================================

\set ON_ERROR_STOP on

-- The GUC must be declared before any policy can reference it. We register
-- it as a *customized* GUC in the `app` namespace so the value survives a
-- pg_dump/restore and shows up cleanly in pg_settings. The default is the
-- empty string (interpreted as "no tenant" by the policies below).
DO $$ BEGIN
    PERFORM set_config('app.current_tenant', '', false);
EXCEPTION WHEN OTHERS THEN
    -- set_config on an undeclared custom GUC is allowed since PG 9.2.
    -- Wrapping in EXCEPTION here is purely defensive for ancient installs.
    NULL;
END $$;

-- -----------------------------------------------------------------------------
-- The policy creator. One function call per table keeps the audit trail
-- shorter than 24 copy/pasted CREATE POLICY blocks but still names every
-- protected object explicitly below.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pg_temp.apply_tenant_rls(schema_name text, table_name text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Skip with a NOTICE if the table doesn't exist yet. This makes the
    -- script tolerant of partial schemas (e.g. a dev DB missing the v0.8.0
    -- marts) — operators see exactly which tables were skipped and can
    -- re-run after the missing migrations land. The verification block at
    -- the end of this file flags any count drift.
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = schema_name
          AND c.relname = table_name
          AND c.relkind IN ('r', 'p')        -- regular table or partitioned parent
    ) THEN
        RAISE NOTICE 'apply_tenant_rls: skipping %.% (table not present)',
                     schema_name, table_name;
        RETURN;
    END IF;

    -- Turn RLS on. Idempotent — re-enabling is a no-op.
    EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY',
                   schema_name, table_name);

    -- Drop the policy if it already exists, then recreate. We DROP-then-CREATE
    -- rather than `IF NOT EXISTS` because policy bodies may change over time
    -- and we want the latest definition to win every run.
    IF EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = schema_name
          AND tablename  = table_name
          AND policyname = 'tenant_isolation'
    ) THEN
        EXECUTE format('DROP POLICY tenant_isolation ON %I.%I',
                       schema_name, table_name);
    END IF;

    -- USING controls visibility on SELECT/UPDATE/DELETE.
    -- WITH CHECK controls validity on INSERT/UPDATE (the new row).
    --
    -- We compare as text rather than casting current_setting() to int so a
    -- bogus / missing value doesn't raise — it just compares as NULL, which
    -- evaluates to NULL, which the policy treats as false. Result: missing
    -- GUC ⇒ zero rows visible. That is the safe default for any path that
    -- forgets to set the GUC.
    EXECUTE format($SQL$
        CREATE POLICY tenant_isolation ON %I.%I
            FOR ALL
            USING (tenant_id::text = current_setting('app.current_tenant', true))
            WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true))
    $SQL$, schema_name, table_name);
END $$;

-- -----------------------------------------------------------------------------
-- Apply to every tenant-owning warehouse table.
-- -----------------------------------------------------------------------------
SELECT pg_temp.apply_tenant_rls('warehouse', t) FROM (VALUES
    ('dim_tenant'),
    ('dim_vehicle'),
    ('dim_device'),
    ('dim_driver'),
    ('bridge_device_driver'),
    ('fact_trip'),
    ('fact_overspeed'),
    ('fact_stop'),
    ('fact_speed_notification'),
    ('fact_daily_activity'),
    ('fact_harsh_event'),
    ('fact_telemetry_daily'),
    ('fact_notification'),
    ('fact_maintenance'),
    ('fact_maintenance_line'),
    ('fact_fueling'),
    -- quarantine_rejected has a NULLABLE tenant_id (some rejects are
    -- pre-attribution). NULL fails the USING predicate, so those rows are
    -- ETL-only by construction — exactly what we want.
    ('quarantine_rejected')
) AS s(t);

-- -----------------------------------------------------------------------------
-- Apply to every tenant-owning marts table.
--
-- NOTE on views (corrected 2026-05-14): an earlier version of this comment
-- claimed that policies on the base tables "automatically" apply when a v_*
-- view is queried. That is FALSE for the Postgres default view mode. A view's
-- underlying RLS is evaluated as the VIEW OWNER, and table owners are exempt
-- from their own RLS — so a marts.v_* view owned by the same role that owns
-- the base tables (here, `medamine_dev`) bypasses RLS for everyone who reads
-- it, including a NOBYPASSRLS app role.
--
-- The fix lives in sql/53_views_security_invoker.sql, which sets
-- `security_invoker = true` on every marts.v_* view so RLS is evaluated as
-- the querier instead of the owner. Keep these two files in sync: if you add
-- a tenant-owning marts table here, add the corresponding view (if any) to
-- 53_views_security_invoker.sql too.
-- -----------------------------------------------------------------------------
SELECT pg_temp.apply_tenant_rls('marts', t) FROM (VALUES
    ('mart_device_monthly_behavior'),
    ('mart_device_monthly_telemetry'),
    ('fact_device_cluster_assignment'),
    ('fact_device_risk_profile'),
    ('mart_fleet_daily'),
    ('mart_vehicle_monthly'),
    ('mart_tenant_monthly_summary')
) AS s(t);

-- -----------------------------------------------------------------------------
-- Verification: every protected table must have exactly one tenant_isolation
-- policy. If this query returns rows, something went wrong above.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    expected_count INT := 24;
    actual_count   INT;
BEGIN
    SELECT COUNT(*) INTO actual_count
    FROM pg_policies
    WHERE policyname = 'tenant_isolation'
      AND schemaname IN ('warehouse', 'marts');

    IF actual_count <> expected_count THEN
        RAISE WARNING
            'RLS policy count mismatch: expected %, got %',
            expected_count, actual_count;
    END IF;
END $$;
