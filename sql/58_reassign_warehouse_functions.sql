-- =============================================================================
-- 58_reassign_warehouse_functions.sql
--
-- Third gap fix discovered during the B7 smoke test. sql/57 transferred
-- ownership of warehouse/marts *relations* (tables, views, matviews,
-- sequences) from medamine_dev to accent_etl, which unblocked
-- `CREATE INDEX IF NOT EXISTS` and `ALTER TABLE`. After that, the dim
-- pipeline got one step further and failed on:
--
--   ProgrammingError: must be owner of function smallint_to_bool
--   [SQL: CREATE OR REPLACE FUNCTION warehouse.smallint_to_bool(v SMALLINT)
--         RETURNS BOOLEAN AS $$ ... $$ LANGUAGE SQL IMMUTABLE]
--
-- `CREATE OR REPLACE FUNCTION` requires ownership of the existing function
-- — no GRANT covers it. The function was originally created under
-- medamine_dev (the bootstrap DBA), so accent_etl can't replace it.
--
-- sql/57 missed this because it enumerated pg_class (relkind in
-- 'r','p','v','m','S') — functions live in pg_proc, not pg_class.
--
-- =============================================================================
-- What we change
-- =============================================================================
-- For every function, procedure, aggregate, and window function in
-- warehouse.* and marts.*, transfer ownership from `medamine_dev` to
-- `accent_etl`. Same reasoning as sql/57: the pipeline (running as
-- accent_etl post-B7) re-issues CREATE OR REPLACE on rebuild, so the
-- writing role must own the object.
--
-- =============================================================================
-- Function identity nuance
-- =============================================================================
-- Function names are NOT unique within a schema — only (schema, name,
-- arg-types) is. We use `pg_get_function_identity_arguments(p.oid)` to
-- build the disambiguating signature for the ALTER statement. Without
-- it, `ALTER FUNCTION warehouse.foo OWNER TO …` would fail for any
-- overloaded function.
--
-- prokind values we care about (PG 11+):
--   'f' -> FUNCTION         (incl. window funcs in older PGs)
--   'p' -> PROCEDURE
--   'a' -> AGGREGATE        (must use ALTER AGGREGATE)
--   'w' -> WINDOW FUNCTION  (ALTER FUNCTION works)
--
-- =============================================================================
-- Idempotency / Rollback
-- =============================================================================
-- ALTER … OWNER TO is a no-op when the target role already owns the object.
-- Rollback (rare, only if we ever need medamine_dev to re-own):
--
--   DO $$ DECLARE r record; BEGIN
--     FOR r IN SELECT n.nspname AS s, p.proname AS name,
--                     pg_get_function_identity_arguments(p.oid) AS args,
--                     p.prokind AS k
--                FROM pg_proc p
--                JOIN pg_namespace n ON n.oid = p.pronamespace
--                JOIN pg_roles ro    ON ro.oid = p.proowner
--               WHERE n.nspname IN ('warehouse','marts')
--                 AND ro.rolname = 'accent_etl' LOOP
--       IF r.k = 'a' THEN
--         EXECUTE format('ALTER AGGREGATE %I.%I(%s) OWNER TO medamine_dev',
--                        r.s, r.name, r.args);
--       ELSIF r.k = 'p' THEN
--         EXECUTE format('ALTER PROCEDURE %I.%I(%s) OWNER TO medamine_dev',
--                        r.s, r.name, r.args);
--       ELSE
--         EXECUTE format('ALTER FUNCTION %I.%I(%s) OWNER TO medamine_dev',
--                        r.s, r.name, r.args);
--       END IF;
--     END LOOP;
--   END $$;
--
-- =============================================================================
-- Privileges required to run
-- =============================================================================
-- Must be executed as a Postgres SUPERUSER (e.g. the cluster `postgres`
-- role, or accent_superadmin). ALTER … OWNER TO requires the executor to
-- be a member of BOTH old and new owner roles, OR be a superuser. The
-- superuser path is simplest.
-- =============================================================================

\set ON_ERROR_STOP on

-- -----------------------------------------------------------------------------
-- 1. Reassign warehouse + marts callable objects from any non-accent_etl
--    owner to accent_etl.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    r record;
    n_funcs      int := 0;
    n_procs      int := 0;
    n_aggs       int := 0;
    n_skipped    int := 0;
BEGIN
    FOR r IN
        SELECT n.nspname AS schema_name,
               p.proname AS proc_name,
               pg_get_function_identity_arguments(p.oid) AS arg_sig,
               p.prokind AS kind,
               ro.rolname AS owner_name
          FROM pg_proc p
          JOIN pg_namespace n ON n.oid = p.pronamespace
          JOIN pg_roles ro    ON ro.oid = p.proowner
         WHERE n.nspname IN ('warehouse', 'marts')
         ORDER BY n.nspname, p.proname, arg_sig
    LOOP
        IF r.owner_name = 'accent_etl' THEN
            n_skipped := n_skipped + 1;
            CONTINUE;
        END IF;

        IF r.kind = 'a' THEN
            EXECUTE format('ALTER AGGREGATE %I.%I(%s) OWNER TO accent_etl',
                           r.schema_name, r.proc_name, r.arg_sig);
            n_aggs := n_aggs + 1;
            RAISE NOTICE 'reassigned AGGREGATE %.%(%) (was %)',
                r.schema_name, r.proc_name, r.arg_sig, r.owner_name;
        ELSIF r.kind = 'p' THEN
            EXECUTE format('ALTER PROCEDURE %I.%I(%s) OWNER TO accent_etl',
                           r.schema_name, r.proc_name, r.arg_sig);
            n_procs := n_procs + 1;
            RAISE NOTICE 'reassigned PROCEDURE %.%(%) (was %)',
                r.schema_name, r.proc_name, r.arg_sig, r.owner_name;
        ELSE
            -- 'f' (function) and 'w' (window function) both use ALTER FUNCTION.
            EXECUTE format('ALTER FUNCTION %I.%I(%s) OWNER TO accent_etl',
                           r.schema_name, r.proc_name, r.arg_sig);
            n_funcs := n_funcs + 1;
            RAISE NOTICE 'reassigned FUNCTION %.%(%) (was %)',
                r.schema_name, r.proc_name, r.arg_sig, r.owner_name;
        END IF;
    END LOOP;

    RAISE NOTICE
        'function-ownership summary: % functions, % procedures, % aggregates reassigned; % already owned by accent_etl',
        n_funcs, n_procs, n_aggs, n_skipped;
END $$;

-- -----------------------------------------------------------------------------
-- 2. Verification: zero callable objects in warehouse/marts should remain
--    owned by anyone other than accent_etl.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    n_wrong_owner int;
BEGIN
    SELECT COUNT(*) INTO n_wrong_owner
      FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
      JOIN pg_roles ro    ON ro.oid = p.proowner
     WHERE n.nspname IN ('warehouse', 'marts')
       AND ro.rolname <> 'accent_etl';

    IF n_wrong_owner > 0 THEN
        RAISE EXCEPTION
            '58_reassign_warehouse_functions: % callable object(s) in warehouse/marts still not owned by accent_etl',
            n_wrong_owner;
    END IF;

    RAISE NOTICE
        '58_reassign_warehouse_functions: all functions/procedures/aggregates in warehouse/marts owned by accent_etl';
END $$;
