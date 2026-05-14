-- =============================================================================
-- 56_grant_etl_staging.sql
--
-- Gap fix discovered during B7 smoke test. sql/52_role_separation.sql granted
-- USAGE on warehouse, marts, auth, public — but missed `staging`. The
-- dimension pipeline's first SQL file (sql/01_dim_tenant.sql) reads from
-- `staging.path` and `staging.device`, so the first `docker compose exec etl
-- python scripts/run_batch.py --mode incremental` after the B7 cutover failed
-- with:
--
--     psycopg.errors.InsufficientPrivilege:
--       permission denied for schema staging
--
-- This worked under the old setup because the pipeline connected as
-- `medamine_dev`, which owns staging.* and therefore has implicit USAGE/SELECT.
--
-- =============================================================================
-- What we grant
-- =============================================================================
-- accent_etl is read-only on staging — staging is loaded by a separate
-- bootstrap path (notebooks / one-off scripts), not by the Prefect flow.
-- A grep of the codebase confirms no INSERT/UPDATE/DELETE/TRUNCATE/COPY
-- targets staging.* from the pipeline, so SELECT is sufficient.
--
-- We also set DEFAULT PRIVILEGES so any staging table added later
-- (e.g. staging.harsh_event) is automatically readable by accent_etl
-- without a follow-up migration.
--
-- =============================================================================
-- Why not amend sql/52_role_separation.sql?
-- =============================================================================
-- 52 has already been applied in production (M6 cutover). Editing it in
-- place wouldn't change live cluster state — only re-runs in a fresh DB
-- would pick up the fix. A new, idempotent follow-up file is the safer
-- pattern: it documents the gap, it can be replayed safely, and the audit
-- trail in `sql/` remains an honest record of what was done in what order.
--
-- =============================================================================
-- Idempotency
-- =============================================================================
-- All statements below are idempotent. GRANT on an already-granted privilege
-- is a no-op; ALTER DEFAULT PRIVILEGES likewise.
--
-- =============================================================================
-- Rollback
-- =============================================================================
--   REVOKE SELECT ON ALL TABLES IN SCHEMA staging FROM accent_etl;
--   REVOKE USAGE  ON SCHEMA staging               FROM accent_etl;
--   ALTER DEFAULT PRIVILEGES IN SCHEMA staging
--       REVOKE SELECT ON TABLES FROM accent_etl;
-- =============================================================================

\set ON_ERROR_STOP on

-- Schema-level: lets accent_etl name objects in staging.*
GRANT USAGE ON SCHEMA staging TO accent_etl;

-- Existing tables / sequences.
GRANT SELECT ON ALL TABLES    IN SCHEMA staging TO accent_etl;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA staging TO accent_etl;

-- Future tables added to staging — bake the grant in so we don't trip
-- on this again next time staging gains a table.
--
-- Default privileges are scoped to the role that creates the object. The
-- staging tables are owned by medamine_dev today, so we set defaults FOR
-- that role explicitly. (If staging ownership ever moves, add another
-- FOR ROLE clause here.)
ALTER DEFAULT PRIVILEGES FOR ROLE medamine_dev IN SCHEMA staging
    GRANT SELECT ON TABLES    TO accent_etl;
ALTER DEFAULT PRIVILEGES FOR ROLE medamine_dev IN SCHEMA staging
    GRANT SELECT ON SEQUENCES TO accent_etl;

-- accent_app + accent_superadmin coverage: keep the pattern symmetric so a
-- future feature that wants to read raw staging from the API (e.g. a
-- data-quality endpoint) doesn't trip the same trap. accent_app stays
-- RLS-clamped at the table layer regardless of this grant.
GRANT USAGE  ON SCHEMA staging                   TO accent_app, accent_superadmin;
GRANT SELECT ON ALL TABLES IN SCHEMA staging     TO accent_app, accent_superadmin;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA staging  TO accent_app, accent_superadmin;
ALTER DEFAULT PRIVILEGES FOR ROLE medamine_dev IN SCHEMA staging
    GRANT SELECT ON TABLES TO accent_app, accent_superadmin;

-- -----------------------------------------------------------------------------
-- Verification: accent_etl should now have USAGE on staging and SELECT on
-- every existing staging table. If `missing_count > 0` something blocked
-- one of the grants above.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    has_usage     boolean;
    missing_count int;
BEGIN
    SELECT has_schema_privilege('accent_etl', 'staging', 'USAGE')
      INTO has_usage;
    IF NOT has_usage THEN
        RAISE EXCEPTION
            '56_grant_etl_staging: accent_etl still lacks USAGE on staging';
    END IF;

    SELECT COUNT(*) INTO missing_count
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'staging'
       AND c.relkind IN ('r', 'p', 'v', 'm')
       AND NOT has_table_privilege('accent_etl', c.oid, 'SELECT');

    IF missing_count > 0 THEN
        RAISE EXCEPTION
            '56_grant_etl_staging: accent_etl lacks SELECT on % staging table(s)',
            missing_count;
    END IF;

    RAISE NOTICE
        '56_grant_etl_staging: accent_etl has USAGE + SELECT on staging.*';
END $$;
