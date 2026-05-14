-- =============================================================================
-- 55_force_rls.sql
--
-- Defense-in-depth followup to M6 + B7. Runs AFTER Prefect ETL has been cut
-- over from `medamine_dev` to `accent_etl` (BYPASSRLS) — applying this before
-- B7 would break ETL writes, because medamine_dev (the table owner) would
-- suddenly be subject to its own tenant_isolation policies and the ETL
-- transactions don't SET `app.current_tenant`.
--
-- =============================================================================
-- What FORCE ROW LEVEL SECURITY does
-- =============================================================================
-- Postgres default: a table's owner is exempt from RLS on that table (and so
-- is anyone the owner shares a role with). FORCE removes that exemption —
-- the owner now obeys the same policies as everyone else.
--
-- Why we want it after B7:
--   * Before B7, Prefect connected as `medamine_dev` (the table owner) and
--     relied on owner-exemption to write any tenant's row. Adding FORCE then
--     would have required Prefect to start setting `app.current_tenant` per
--     transaction — invasive.
--   * After B7, Prefect connects as `accent_etl` (BYPASSRLS). BYPASSRLS
--     beats FORCE — the role-attribute check happens before policy
--     evaluation. So Prefect writes still go through.
--   * The remaining caller of `medamine_dev` is now ad-hoc operator psql
--     (migrations, fixups). With FORCE on, those sessions are RLS-clamped
--     too, which closes a real attack surface: a stolen `medamine_dev`
--     password no longer grants cross-tenant read/write at the table layer.
--     The operator can still see everything by `SET LOCAL ROLE
--     accent_superadmin` (membership granted in
--     sql/54_grant_superadmin_membership.sql), so legitimate ops aren't
--     blocked — but the elevation is now explicit and auditable.
--
-- =============================================================================
-- Roles unaffected by FORCE
-- =============================================================================
--   accent_app          NOBYPASSRLS, non-owner -> already RLS-clamped (no change)
--   accent_etl          BYPASSRLS              -> bypasses RLS regardless
--   accent_superadmin   SUPERUSER + BYPASSRLS  -> bypasses RLS regardless
--
-- =============================================================================
-- Role affected by FORCE
-- =============================================================================
--   medamine_dev        owner of the tables   -> previously exempt;
--                                                will now be RLS-clamped
--                                                unless `SET LOCAL ROLE
--                                                accent_superadmin` is issued
--
-- =============================================================================
-- Idempotency
-- =============================================================================
-- `ALTER TABLE ... FORCE ROW LEVEL SECURITY` is idempotent: applying when
-- already FORCED is a no-op. Safe to re-run.
--
-- =============================================================================
-- Rollback (re-opens the owner-exemption escape hatch)
-- =============================================================================
--   DO $$ DECLARE r record; BEGIN
--     FOR r IN SELECT n.nspname AS s, c.relname AS t
--                FROM pg_class c
--                JOIN pg_namespace n ON n.oid = c.relnamespace
--               WHERE n.nspname IN ('warehouse','marts')
--                 AND c.relforcerowsecurity LOOP
--       EXECUTE format('ALTER TABLE %I.%I NO FORCE ROW LEVEL SECURITY',
--                       r.s, r.t);
--     END LOOP;
--   END $$;
--
-- =============================================================================

\set ON_ERROR_STOP on

-- Apply FORCE to every table that already has RLS enabled in
-- warehouse.* and marts.*. We don't enumerate by name on purpose:
-- 51_rls_policies.sql is the source of truth for which tables are
-- tenant-scoped, and this file just amplifies whatever it enabled.
-- That way, adding a new tenant-owning table only requires updating
-- 51_rls_policies.sql; the FORCE attribute follows automatically the
-- next time this migration is re-run.
DO $$
DECLARE
    r record;
    n_already int := 0;
    n_changed int := 0;
BEGIN
    FOR r IN
        SELECT n.nspname AS schema_name, c.relname AS table_name,
               c.relforcerowsecurity AS already_forced
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname IN ('warehouse', 'marts')
           AND c.relkind IN ('r', 'p')
           AND c.relrowsecurity = true       -- only tables with RLS on
         ORDER BY n.nspname, c.relname
    LOOP
        IF r.already_forced THEN
            n_already := n_already + 1;
            RAISE NOTICE 'already FORCED: %.%', r.schema_name, r.table_name;
        ELSE
            EXECUTE format('ALTER TABLE %I.%I FORCE ROW LEVEL SECURITY',
                           r.schema_name, r.table_name);
            n_changed := n_changed + 1;
            RAISE NOTICE 'FORCED: %.%', r.schema_name, r.table_name;
        END IF;
    END LOOP;

    RAISE NOTICE
        'FORCE ROW LEVEL SECURITY summary: % newly forced, % already forced',
        n_changed, n_already;
END $$;

-- -----------------------------------------------------------------------------
-- Verification: every RLS-enabled table in warehouse.* and marts.* should
-- now be FORCED. If `unforced_count > 0` the migration didn't take.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    unforced_count int;
BEGIN
    SELECT COUNT(*) INTO unforced_count
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname IN ('warehouse', 'marts')
       AND c.relkind IN ('r', 'p')
       AND c.relrowsecurity = true
       AND c.relforcerowsecurity = false;

    IF unforced_count > 0 THEN
        RAISE EXCEPTION
            '55_force_rls: % RLS-enabled table(s) still lack FORCE',
            unforced_count;
    END IF;
    RAISE NOTICE
        '55_force_rls: every RLS-enabled table in warehouse/marts is FORCED';
END $$;
