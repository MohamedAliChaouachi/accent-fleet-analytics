-- =============================================================================
-- 57_reassign_warehouse_ownership.sql
--
-- Second gap fix discovered during B7 smoke test. After sql/56 unblocked
-- staging reads, the dimension pipeline failed on:
--
--   ProgrammingError: must be owner of table dim_vehicle
--   [SQL: CREATE INDEX IF NOT EXISTS idx_dim_vehicle_tenant
--         ON warehouse.dim_vehicle (tenant_id)]
--
-- Postgres requires the *owner* of a table to execute CREATE INDEX,
-- ALTER TABLE, COMMENT ON, etc. No GRANT bestows ownership — it's a
-- single-role attribute on the table itself. The pipeline's dimension
-- SQL files (02_dim_vehicle.sql, 03_dim_device.sql, 07_bridge_*.sql, …)
-- all issue idempotent CREATE INDEX IF NOT EXISTS on rebuild, so the
-- pipeline can't proceed until accent_etl owns those tables.
--
-- =============================================================================
-- What we change
-- =============================================================================
-- For every relation in warehouse.* and marts.* (table, view, materialized
-- view, sequence), transfer ownership from `medamine_dev` (the bootstrap
-- DBA account) to `accent_etl`. The pipeline runs as accent_etl post-B7,
-- so this aligns ownership with the role doing the writes.
--
-- Why accent_etl and not accent_superadmin or a dedicated owner?
--   * accent_etl is the role that runs Prefect day-to-day; the principle
--     of least surprise is "the role that writes the table owns it."
--   * Ownership-as-accent_superadmin would re-introduce the M6 problem
--     in reverse — the API user (accent_app) would gain implicit reach
--     via membership, undermining tenant isolation.
--   * Spinning up a fourth role just to own DDL is overkill for a
--     single-process pipeline.
--
-- =============================================================================
-- Human-DBA ergonomics
-- =============================================================================
-- After this migration `medamine_dev` no longer owns warehouse/marts, so:
--   * `psql -U medamine_dev` against those schemas is RLS-clamped immediately
--     (one migration earlier than sql/55_force_rls.sql would have caused it).
--   * Direct ALTER TABLE / CREATE INDEX as medamine_dev returns
--     "must be owner of table …" — exactly the error this file fixes for
--     accent_etl, in the opposite direction.
--
-- To preserve operator ergonomics we GRANT membership in accent_superadmin
-- to medamine_dev. medamine_dev can then `SET LOCAL ROLE accent_superadmin`
-- to do ad-hoc admin work — same elevation pattern accent_app already uses
-- (sql/54_grant_superadmin_membership.sql). The login role stays medamine_dev,
-- so any audit / connection-log entries still identify the human operator.
--
-- =============================================================================
-- Indexes, policies, defaults
-- =============================================================================
-- * Indexes follow their parent table automatically — no separate ALTER.
-- * RLS policies are not owned by anyone in particular; they stay attached
--   to the table and survive owner changes.
-- * marts.v_* views' `security_invoker = true` flag (sql/53) is unchanged
--   by an owner reassignment; the option is independent of ownership.
-- * Column DEFAULT expressions that reference sequences (SERIAL, IDENTITY)
--   keep working because we ALTER both tables and their sequences in
--   the same migration.
--
-- =============================================================================
-- Idempotency
-- =============================================================================
-- ALTER TABLE/VIEW/SEQUENCE OWNER TO is a no-op when the target role is
-- already the owner. Safe to re-run.
--
-- =============================================================================
-- Privileges required to run
-- =============================================================================
-- Must be executed as a Postgres SUPERUSER (e.g. accent_superadmin, or the
-- Azure server admin role). `ALTER TABLE ... OWNER TO` requires the executor
-- to be a member of BOTH old and new owner roles, OR be a superuser. The
-- simplest path is the superuser one.
--
-- =============================================================================
-- Rollback
-- =============================================================================
--   DO $$ DECLARE r record; BEGIN
--     FOR r IN SELECT n.nspname AS s, c.relname AS t, c.relkind AS k
--                FROM pg_class c
--                JOIN pg_namespace n ON n.oid = c.relnamespace
--                JOIN pg_roles ro    ON ro.oid = c.relowner
--               WHERE n.nspname IN ('warehouse','marts')
--                 AND ro.rolname = 'accent_etl' LOOP
--       IF r.k = 'r' OR r.k = 'p' THEN
--         EXECUTE format('ALTER TABLE %I.%I OWNER TO medamine_dev', r.s, r.t);
--       ELSIF r.k = 'v' THEN
--         EXECUTE format('ALTER VIEW %I.%I OWNER TO medamine_dev', r.s, r.t);
--       ELSIF r.k = 'm' THEN
--         EXECUTE format('ALTER MATERIALIZED VIEW %I.%I OWNER TO medamine_dev', r.s, r.t);
--       ELSIF r.k = 'S' THEN
--         EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO medamine_dev', r.s, r.t);
--       END IF;
--     END LOOP;
--   END $$;
--   REVOKE accent_superadmin FROM medamine_dev;
-- =============================================================================

\set ON_ERROR_STOP on

-- -----------------------------------------------------------------------------
-- 1. Reassign warehouse + marts objects from medamine_dev to accent_etl.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    r record;
    n_tables    int := 0;
    n_views     int := 0;
    n_matviews  int := 0;
    n_sequences int := 0;
    n_skipped   int := 0;
BEGIN
    FOR r IN
        SELECT n.nspname AS schema_name,
               c.relname AS rel_name,
               c.relkind AS kind,
               ro.rolname AS owner_name
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_roles ro    ON ro.oid = c.relowner
         WHERE n.nspname IN ('warehouse', 'marts')
           AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
         ORDER BY n.nspname, c.relname
    LOOP
        IF r.owner_name = 'accent_etl' THEN
            n_skipped := n_skipped + 1;
            CONTINUE;
        END IF;

        IF r.kind IN ('r', 'p') THEN
            EXECUTE format('ALTER TABLE %I.%I OWNER TO accent_etl',
                           r.schema_name, r.rel_name);
            n_tables := n_tables + 1;
            RAISE NOTICE 'reassigned TABLE %.% (was %)',
                r.schema_name, r.rel_name, r.owner_name;
        ELSIF r.kind = 'v' THEN
            EXECUTE format('ALTER VIEW %I.%I OWNER TO accent_etl',
                           r.schema_name, r.rel_name);
            n_views := n_views + 1;
            RAISE NOTICE 'reassigned VIEW %.% (was %)',
                r.schema_name, r.rel_name, r.owner_name;
        ELSIF r.kind = 'm' THEN
            EXECUTE format('ALTER MATERIALIZED VIEW %I.%I OWNER TO accent_etl',
                           r.schema_name, r.rel_name);
            n_matviews := n_matviews + 1;
            RAISE NOTICE 'reassigned MATERIALIZED VIEW %.% (was %)',
                r.schema_name, r.rel_name, r.owner_name;
        ELSIF r.kind = 'S' THEN
            EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO accent_etl',
                           r.schema_name, r.rel_name);
            n_sequences := n_sequences + 1;
            RAISE NOTICE 'reassigned SEQUENCE %.% (was %)',
                r.schema_name, r.rel_name, r.owner_name;
        END IF;
    END LOOP;

    RAISE NOTICE
        'ownership summary: % tables, % views, % matviews, % sequences reassigned; % already owned by accent_etl',
        n_tables, n_views, n_matviews, n_sequences, n_skipped;
END $$;

-- -----------------------------------------------------------------------------
-- 2. Re-establish operator ergonomics: medamine_dev keeps an escape hatch.
--
-- After reassignment medamine_dev is just another non-owner, NOBYPASSRLS-as-
-- far-as-the-table-is-concerned principal on warehouse/marts. Grant role
-- membership in accent_superadmin so the human DBA can `SET LOCAL ROLE
-- accent_superadmin` when needed — same pattern accent_app uses today.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_auth_members m
        JOIN pg_roles parent ON parent.oid = m.roleid
        JOIN pg_roles child  ON child.oid  = m.member
        WHERE parent.rolname = 'accent_superadmin'
          AND child.rolname  = 'medamine_dev'
    ) THEN
        GRANT accent_superadmin TO medamine_dev;
        RAISE NOTICE 'granted accent_superadmin TO medamine_dev';
    ELSE
        RAISE NOTICE 'medamine_dev already member of accent_superadmin (no-op)';
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 3. Verification.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    n_wrong_owner int;
    has_membership boolean;
BEGIN
    SELECT COUNT(*) INTO n_wrong_owner
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_roles ro    ON ro.oid = c.relowner
     WHERE n.nspname IN ('warehouse', 'marts')
       AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
       AND ro.rolname <> 'accent_etl';

    IF n_wrong_owner > 0 THEN
        RAISE EXCEPTION
            '57_reassign_warehouse_ownership: % relation(s) in warehouse/marts still not owned by accent_etl',
            n_wrong_owner;
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM pg_auth_members m
        JOIN pg_roles parent ON parent.oid = m.roleid
        JOIN pg_roles child  ON child.oid  = m.member
        WHERE parent.rolname = 'accent_superadmin'
          AND child.rolname  = 'medamine_dev'
    ) INTO has_membership;
    IF NOT has_membership THEN
        RAISE EXCEPTION
            '57_reassign_warehouse_ownership: medamine_dev lacks accent_superadmin membership';
    END IF;

    RAISE NOTICE
        '57_reassign_warehouse_ownership: warehouse/marts owned by accent_etl; medamine_dev can elevate via accent_superadmin';
END $$;
