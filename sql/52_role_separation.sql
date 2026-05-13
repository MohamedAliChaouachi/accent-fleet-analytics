-- =============================================================================
-- 52_role_separation.sql
--
-- Split the single `accent_admin`-style superuser into three purpose-built
-- Postgres roles so the API actually feels the RLS policies from
-- 51_rls_policies.sql.
--
-- Implements docs/auth_design.md §2.2 milestone M6.
--
-- *** This script is NOT auto-applied. ***
-- The init-script runner skips it because it has dependencies the cluster
-- alone can't satisfy (the cutover requires a coordinated app restart and
-- credential rotation). The operator runs it by hand following
-- docs/runbooks/role_separation_cutover.md.
--
-- =============================================================================
-- Why three roles
-- =============================================================================
--                  | BYPASSRLS | Owns DDL | Used by                          |
--   accent_app     |     no    |    no    | FastAPI service                  |
--   accent_etl     |    YES    |    no    | Prefect pipeline + seed_auth.py  |
--   accent_superadmin| YES    | YES (opt)| schema migrations, op tooling    |
--
-- Only `accent_app` lacks BYPASSRLS. That's the role policies actually bite.
-- ETL and superadmin connections keep their cross-tenant reach — that's
-- deliberate: a tenant policy on a job that's *defined* to walk every
-- tenant's rows is just friction.
--
-- =============================================================================
-- Password handling
-- =============================================================================
-- This file uses literal placeholders `__APP_PASSWORD__`, `__ETL_PASSWORD__`,
-- and `__SUPERADMIN_PASSWORD__`. The cutover runbook tells the operator to
-- substitute strong randoms (e.g. `python -c "import secrets;
-- print(secrets.token_urlsafe(32))"`) before running. We do NOT bake real
-- secrets into a tracked file.
--
-- =============================================================================
-- Idempotency
-- =============================================================================
-- DO blocks guard every CREATE ROLE with a pg_roles lookup. GRANT statements
-- are inherently idempotent. So this file is re-runnable: e.g. after rotating
-- a password, the operator can re-run only the ALTER ROLE block.
--
-- =============================================================================
-- Rollback
-- =============================================================================
-- The cutover runbook has the rollback. The short version:
--   1. Flip the API back to PG_USER=accent_admin (the BYPASSRLS owner).
--   2. Restart the API.
--   3. Optionally REVOKE/DROP the three new roles once nothing depends on
--      them. Be sure no live Prefect flow is still holding an accent_etl
--      connection before DROP ROLE.
--
-- =============================================================================

\set ON_ERROR_STOP on

-- -----------------------------------------------------------------------------
-- 1. The three roles.
-- -----------------------------------------------------------------------------
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'accent_app') THEN
        CREATE ROLE accent_app
            LOGIN
            NOBYPASSRLS
            NOSUPERUSER
            NOCREATEDB
            NOCREATEROLE
            INHERIT
            PASSWORD '__APP_PASSWORD__';
        RAISE NOTICE 'created role accent_app (NOBYPASSRLS)';
    ELSE
        -- Force the desired attribute every run — guards against drift if
        -- someone manually toggled BYPASSRLS while debugging.
        ALTER ROLE accent_app NOBYPASSRLS NOSUPERUSER;
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'accent_etl') THEN
        CREATE ROLE accent_etl
            LOGIN
            BYPASSRLS
            NOSUPERUSER
            NOCREATEDB
            NOCREATEROLE
            INHERIT
            PASSWORD '__ETL_PASSWORD__';
        RAISE NOTICE 'created role accent_etl (BYPASSRLS)';
    ELSE
        ALTER ROLE accent_etl BYPASSRLS NOSUPERUSER;
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'accent_superadmin') THEN
        CREATE ROLE accent_superadmin
            LOGIN
            BYPASSRLS
            SUPERUSER
            CREATEDB
            CREATEROLE
            INHERIT
            PASSWORD '__SUPERADMIN_PASSWORD__';
        RAISE NOTICE 'created role accent_superadmin (SUPERUSER + BYPASSRLS)';
    ELSE
        ALTER ROLE accent_superadmin BYPASSRLS SUPERUSER;
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 2. Schema-level USAGE grants.
--
-- USAGE just lets the role *see* the schema's object names; SELECT/INSERT
-- below grants actual data access. Without USAGE a role can't even fully
-- qualify a table name in a query.
-- -----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA warehouse, marts, auth, public TO accent_app;
GRANT USAGE ON SCHEMA warehouse, marts, auth, public TO accent_etl;
GRANT USAGE ON SCHEMA warehouse, marts, auth, public TO accent_superadmin;

-- accent_etl additionally needs CREATE on warehouse/marts because the
-- pipeline runs DDL (CREATE TABLE IF NOT EXISTS, CREATE INDEX, ALTER TABLE).
GRANT CREATE ON SCHEMA warehouse, marts TO accent_etl;

-- -----------------------------------------------------------------------------
-- 3. Table-level grants.
-- -----------------------------------------------------------------------------

-- 3a. accent_app: full DML on auth.* (login writes audit log, refresh
-- rotates rows, password reset updates the user row). Read+write on
-- marts.* and warehouse.* dimension/fact tables because device routes can
-- score-on-demand and the admin routes provision tenants. *Without
-- BYPASSRLS,* the policies decide which rows actually come back.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA auth      TO accent_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA marts     TO accent_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA warehouse TO accent_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA auth      TO accent_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA marts     TO accent_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA warehouse TO accent_app;

-- 3b. accent_etl: everything in warehouse + marts, plus read-only on auth
-- for the seed script (it INSERTs users so it needs INSERT too).
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA warehouse TO accent_etl;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA marts     TO accent_etl;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA auth TO accent_etl;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA warehouse TO accent_etl;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA marts     TO accent_etl;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA auth      TO accent_etl;

-- 3c. accent_superadmin: SUPERUSER already grants everything; the explicit
-- statements below are belt + suspenders for the day a future DBA narrows
-- the SUPERUSER attribute.
GRANT ALL ON ALL TABLES    IN SCHEMA warehouse, marts, auth TO accent_superadmin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA warehouse, marts, auth TO accent_superadmin;

-- -----------------------------------------------------------------------------
-- 4. Default privileges — covers tables created AFTER this script runs.
--
-- Without DEFAULT PRIVILEGES, a pipeline that creates a new fact table
-- tomorrow would leave the API unable to read it until someone re-runs the
-- grants. ALTER DEFAULT PRIVILEGES makes the grant happen automatically
-- against whatever role owns future tables (here: accent_etl for warehouse/
-- marts and accent_superadmin for auth/migrations).
-- -----------------------------------------------------------------------------
ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO accent_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO accent_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA auth
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO accent_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO accent_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO accent_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA auth
    GRANT SELECT, INSERT, UPDATE ON TABLES TO accent_etl;

ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse, marts, auth
    GRANT ALL ON TABLES    TO accent_superadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse, marts, auth
    GRANT ALL ON SEQUENCES TO accent_superadmin;

-- -----------------------------------------------------------------------------
-- 5. Verification snapshot.
--
-- After this script runs the operator should manually verify:
--
--   SELECT rolname, rolbypassrls, rolsuper
--   FROM pg_roles
--   WHERE rolname LIKE 'accent\_%' ESCAPE '\';
--
-- Expected:
--   accent_app          | f | f
--   accent_etl          | t | f
--   accent_superadmin   | t | t
--
-- If accent_app shows rolbypassrls = t, ABORT THE CUTOVER. Tenant
-- isolation is silently disabled.
-- -----------------------------------------------------------------------------
