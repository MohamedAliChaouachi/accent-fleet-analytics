-- =============================================================================
-- 59_grant_marts_to_accent_app.sql
--
-- Backfill the grant that 52_role_separation.sql missed for objects in
-- marts that ended up owned by accent_etl rather than the role that ran 52.
--
-- Symptom (discovered 2026-05-26):
--
--   GET /v1/dashboards/executive → 500
--   sqlalchemy.exc.ProgrammingError:
--     permission denied for view v_predictive_alerts_dashboard
--
-- The API connects as accent_app (NOBYPASSRLS). 52_role_separation.sql at
-- line 137 grants `SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA
-- marts TO accent_app`, but that statement only applies to objects that
-- existed at that moment. After 52 ran, the dashboard views (sql/33-39)
-- and the device risk score table (sql/28, 41) were re-applied as
-- accent_etl (their owner), so the prior GRANT didn't cover them. Worse,
-- the ALTER DEFAULT PRIVILEGES in 52 didn't have `FOR ROLE accent_etl`,
-- so default-priv rules don't fire for objects accent_etl creates —
-- meaning every future view created by the pipeline would have the same
-- gap.
--
-- This script fixes both halves:
--   1. Backfill SELECT on every existing relation in marts to accent_app.
--   2. Install FOR ROLE accent_etl default privileges so anything the
--      pipeline creates from now on is readable by accent_app automatically.
--
-- =============================================================================
-- Why only marts?
-- =============================================================================
-- warehouse fact/dim tables were created earlier in the bootstrap and are
-- owned by accent_etl too, but the GRANTs in 52 caught them because the
-- relations existed before 52 ran. A diagnostic query at fix time confirmed
-- accent_app already had SELECT on every warehouse relation. If a future
-- pipeline migration adds a warehouse table, the analogous default-priv
-- block below will start covering it.
--
-- =============================================================================
-- Idempotency
-- =============================================================================
-- GRANT and ALTER DEFAULT PRIVILEGES are both idempotent. Safe to re-run.
--
-- =============================================================================

\set ON_ERROR_STOP on

-- 1. Backfill: cover every existing relation in marts.
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA marts TO accent_app;
GRANT USAGE, SELECT
    ON ALL SEQUENCES IN SCHEMA marts TO accent_app;

-- And, for symmetry, cover warehouse too — cheap insurance against the
-- same drift on the warehouse side after a future ETL refactor.
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA warehouse TO accent_app;
GRANT USAGE, SELECT
    ON ALL SEQUENCES IN SCHEMA warehouse TO accent_app;

-- 2. Future-proof: when accent_etl creates a new relation in marts/warehouse,
--    accent_app should get SELECT/INSERT/UPDATE/DELETE on it automatically.
--    Without FOR ROLE, ALTER DEFAULT PRIVILEGES only covers objects created
--    by the CURRENT_USER of the ALTER statement — useless when the pipeline
--    runs as a different role than the migration operator.
ALTER DEFAULT PRIVILEGES FOR ROLE accent_etl IN SCHEMA marts
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO accent_app;
ALTER DEFAULT PRIVILEGES FOR ROLE accent_etl IN SCHEMA marts
    GRANT USAGE, SELECT ON SEQUENCES TO accent_app;

ALTER DEFAULT PRIVILEGES FOR ROLE accent_etl IN SCHEMA warehouse
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO accent_app;
ALTER DEFAULT PRIVILEGES FOR ROLE accent_etl IN SCHEMA warehouse
    GRANT USAGE, SELECT ON SEQUENCES TO accent_app;

-- 3. Verification — should print "all clear" after this script runs.
DO $$
DECLARE
    leaky_count int;
BEGIN
    SELECT COUNT(*) INTO leaky_count
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname IN ('marts','warehouse')
      AND c.relkind IN ('r','v','m')
      AND NOT has_table_privilege('accent_app', c.oid, 'SELECT');

    IF leaky_count > 0 THEN
        RAISE WARNING
            '59_grant_marts_to_accent_app: % marts/warehouse relations still unreadable by accent_app',
            leaky_count;
    ELSE
        RAISE NOTICE
            '59_grant_marts_to_accent_app: all clear — every marts/warehouse relation is readable by accent_app';
    END IF;
END $$;
