-- =============================================================================
-- 54_grant_superadmin_membership.sql
--
-- Followup to M6 (sql/52_role_separation.sql) discovered during v0.9.0 B6
-- verification. After cutting the API over to `accent_app` (NOBYPASSRLS), the
-- engine listener in src/accent_fleet/db/engine.py short-circuits with no
-- SET LOCAL for principals whose role is `superadmin`. The original reasoning
-- (see engine.py docstring) was "superadmin connects as a BYPASSRLS role" —
-- but post-M6 the connection role is `accent_app`, which has no BYPASSRLS.
-- Result: every superadmin request to a tenant-scoped table returns 0 rows
-- because the policy compares `tenant_id::text` to an empty GUC.
--
-- =============================================================================
-- Fix
-- =============================================================================
-- We grant `accent_app` membership in `accent_superadmin`. The listener can
-- then issue `SET LOCAL ROLE accent_superadmin` for superadmin principals,
-- temporarily elevating the transaction to a role with BYPASSRLS. The role
-- swap reverts at COMMIT/ROLLBACK, so other principals on the same pooled
-- connection are unaffected.
--
-- This is *not* the same as letting accent_app log in as accent_superadmin.
-- The login role is still accent_app (NOBYPASSRLS); SET LOCAL ROLE only
-- changes the effective role for the active transaction, and it only works
-- because accent_app is a member of accent_superadmin. No new external-facing
-- privilege is created.
--
-- =============================================================================
-- Threat model
-- =============================================================================
-- An attacker with a stolen superadmin JWT already has full cross-tenant
-- access via the application layer (admin endpoints, etc.) — granting role
-- membership doesn't expand that surface. An attacker without a superadmin
-- JWT cannot trigger the `SET LOCAL ROLE` path because the listener only
-- swaps role when `principal.role == 'superadmin'`, which requires a
-- signature-valid JWT carrying that claim.
--
-- =============================================================================
-- Idempotency
-- =============================================================================
-- GRANT on an existing membership is a no-op (Postgres returns NOTICE on
-- the duplicate). Wrapped in a guard for clean re-runs.
--
-- =============================================================================
-- Rollback
-- =============================================================================
--   REVOKE accent_superadmin FROM accent_app;
-- (after which superadmin requests to tenant-scoped views return 0 rows again)
-- =============================================================================

\set ON_ERROR_STOP on

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_auth_members m
        JOIN pg_roles parent ON parent.oid = m.roleid
        JOIN pg_roles child  ON child.oid  = m.member
        WHERE parent.rolname = 'accent_superadmin'
          AND child.rolname  = 'accent_app'
    ) THEN
        GRANT accent_superadmin TO accent_app;
        RAISE NOTICE 'granted accent_superadmin TO accent_app';
    ELSE
        RAISE NOTICE 'accent_app already has membership in accent_superadmin (no-op)';
    END IF;
END $$;

-- Verification
DO $$
DECLARE
    has_membership boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM pg_auth_members m
        JOIN pg_roles parent ON parent.oid = m.roleid
        JOIN pg_roles child  ON child.oid  = m.member
        WHERE parent.rolname = 'accent_superadmin'
          AND child.rolname  = 'accent_app'
    ) INTO has_membership;

    IF NOT has_membership THEN
        RAISE EXCEPTION 'verification failed: accent_app lacks accent_superadmin membership';
    END IF;
    RAISE NOTICE 'verified: accent_app -> accent_superadmin membership in place';
END $$;
