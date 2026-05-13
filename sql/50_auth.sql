-- =============================================================================
-- 50_auth.sql
-- =============================================================================
-- Authentication + multi-tenancy schema.
--
-- Implements §5 of docs/auth_design.md. Lands DDL only, with no RLS and no
-- middleware enforcement — that arrives in later milestones (M5/M6). After
-- this file runs, the application keeps working unchanged: nothing reads
-- from `auth.*` yet.
--
-- Idempotent: CREATE SCHEMA / CREATE TABLE / CREATE INDEX all use IF NOT
-- EXISTS, and the tenants backfill uses ON CONFLICT DO NOTHING. Safe to
-- re-run on a partially-initialised database without resetting state.
--
-- Rollback (M1 only): DROP SCHEMA auth CASCADE;
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS auth;

-- pgcrypto provides gen_random_uuid() for refresh-token primary keys.
-- Available in core Postgres 13+; CREATE EXTENSION IF NOT EXISTS makes
-- the script idempotent against a database that already has it.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- -----------------------------------------------------------------------------
-- auth.tenants
-- -----------------------------------------------------------------------------
-- One row per logical customer. tenant_id is the same integer used across
-- warehouse.* and marts.* facts so existing FKs need no migration: the
-- auth-side row simply names the tenant the data side already knows.
--
-- No RLS on this table itself; only the auth router reads it, and the
-- router uses Principal.role for any cross-tenant access decisions.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS auth.tenants (
    tenant_id     INTEGER PRIMARY KEY
                  REFERENCES warehouse.dim_tenant(tenant_id),
    display_name  TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active     BOOLEAN NOT NULL DEFAULT TRUE
);

-- -----------------------------------------------------------------------------
-- auth.users
-- -----------------------------------------------------------------------------
-- Users belong to exactly one tenant; superadmins use tenant_id = NULL and
-- the 'superadmin' role. The CHECK constraint pins the bijection
-- (role = 'superadmin') ⇔ (tenant_id IS NULL) so we cannot accidentally
-- create a tenant-bound superadmin or an untenanted regular user.
--
-- password_hash is a full PHC-string Argon2id encoding (algorithm, params,
-- salt, and digest in one column). Verification is self-contained — no
-- separate salt column to keep in sync.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS auth.users (
    user_id        BIGSERIAL PRIMARY KEY,
    tenant_id      INTEGER REFERENCES auth.tenants(tenant_id),
    email          TEXT NOT NULL UNIQUE,
    password_hash  TEXT NOT NULL,
    role           TEXT NOT NULL
                   CHECK (role IN ('tenant_user',
                                   'tenant_admin',
                                   'superadmin')),
    is_active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at  TIMESTAMPTZ,
    CHECK ((role = 'superadmin') = (tenant_id IS NULL))
);

CREATE INDEX IF NOT EXISTS ix_auth_users_tenant
    ON auth.users (tenant_id) WHERE tenant_id IS NOT NULL;

-- -----------------------------------------------------------------------------
-- auth.refresh_tokens
-- -----------------------------------------------------------------------------
-- Stateful refresh tokens so logout immediately revokes. Access tokens
-- stay stateless JWT (see §6 of the design doc).
--
-- Partial index on user_id WHERE revoked_at IS NULL keeps the
-- "active sessions for user X" lookup cheap as the table grows; revoked
-- rows accumulate but are not indexed.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS auth.refresh_tokens (
    token_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      BIGINT NOT NULL REFERENCES auth.users(user_id)
                 ON DELETE CASCADE,
    issued_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at   TIMESTAMPTZ NOT NULL,
    revoked_at   TIMESTAMPTZ,
    user_agent   TEXT,
    source_ip    INET
);

CREATE INDEX IF NOT EXISTS ix_auth_refresh_tokens_active_user
    ON auth.refresh_tokens (user_id) WHERE revoked_at IS NULL;

-- -----------------------------------------------------------------------------
-- auth.audit_log
-- -----------------------------------------------------------------------------
-- Append-only. The operator queries this to investigate "who did X when".
-- `action` is a free-text column (not an enum) so adding new event types
-- never requires a schema migration; the design doc §5 names the
-- canonical set:
--   login_success | login_failure | refresh | logout | token_revoked
--   admin_create_user | admin_create_tenant | admin_disable_user
--   password_reset
--
-- `detail` is JSONB so each action can carry its own context (failure
-- reason, target user id, etc.) without sprouting columns.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS auth.audit_log (
    event_id     BIGSERIAL PRIMARY KEY,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    user_id      BIGINT REFERENCES auth.users(user_id),
    tenant_id    INTEGER REFERENCES auth.tenants(tenant_id),
    action       TEXT NOT NULL,
    source_ip    INET,
    user_agent   TEXT,
    detail       JSONB
);

-- "What happened recently across the system?" — primary operator query.
CREATE INDEX IF NOT EXISTS ix_auth_audit_log_occurred_at
    ON auth.audit_log (occurred_at DESC);

-- "What did this user do recently?" — incident-response query.
CREATE INDEX IF NOT EXISTS ix_auth_audit_log_user_occurred
    ON auth.audit_log (user_id, occurred_at DESC);

-- -----------------------------------------------------------------------------
-- Backfill: one auth.tenants row per existing warehouse.dim_tenant.
-- -----------------------------------------------------------------------------
-- Idempotent via ON CONFLICT — re-running this file never overwrites a
-- display name the operator may have edited manually. Default display
-- name is "tenant_<n>" so the row exists; the operator renames it via
-- an admin endpoint or a direct UPDATE.
-- -----------------------------------------------------------------------------
INSERT INTO auth.tenants (tenant_id, display_name)
SELECT tenant_id, 'tenant_' || tenant_id::text
FROM warehouse.dim_tenant
ON CONFLICT (tenant_id) DO NOTHING;
