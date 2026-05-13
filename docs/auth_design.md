# Authentication + Multi-Tenancy — Design Document

> **Status:** Draft for review, no code yet.
> **Tracks:** `NEXT_STEPS.md` §2.2.
> **Author:** initial draft 2026-05-13.
> **Target version:** v0.9.0 (auth + tenant isolation) and v0.10.0 (admin UI).

This document describes how Accent Fleet Analytics will move from a
single shared `htpasswd` credential (today's stopgap from §1.5) to
genuine per-tenant authentication with database-enforced isolation.

It is written for two audiences: the implementer (who needs a buildable
plan) and the report examiner (who needs evidence that the architectural
choices were deliberate, not accidental). Where alternatives were
rejected, the rejection rationale is recorded inline so the trade-off
can be defended.

---

## 1. Goals and non-goals

### 1.1 Goals

1. **Per-user authentication.** Every request to `/v1/*` carries a
   verifiable credential identifying the user and their tenant.
2. **Per-tenant isolation enforced by the database, not by application
   code.** A bug in a query helper, a missing `WHERE tenant_id = ...`,
   or a hand-crafted SQL injection cannot cause cross-tenant data
   leakage. Defense in depth, with Postgres Row-Level Security (RLS)
   as the outermost enforcement layer.
3. **Auditable.** Every authentication event (success, failure,
   refresh, logout) and every privileged action is written to an
   append-only audit log with timestamp, user, action, and source IP.
4. **Operable.** Token rotation, password reset, and tenant
   provisioning are documented runbooks with admin endpoints, not
   ad-hoc SQL.
5. **Plug-in path to OIDC.** The architecture must allow Azure AD SSO
   to be added later without rewriting consumers. JWT-with-local-
   passwords ships first; OIDC is a future swap-in of the issuer.

### 1.2 Non-goals (for the v0.9.0 milestone)

- **OIDC / SSO integration.** Designed for, not shipped. The threat
  model and token shape are OIDC-compatible so a later Azure AD
  integration is a swap-in, not a rewrite.
- **End-user self-service signup.** Tenants and initial users are
  created by a superadmin through admin endpoints. No public
  registration flow.
- **Per-resource fine-grained authorization (ACLs).** Roles are coarse:
  `tenant_admin`, `tenant_user`, `superadmin`. RBAC beyond that is
  future work.
- **Dashboard auth.** Streamlit is on the deprecate-list (§2.3). It
  keeps the existing `--profile auth` nginx Basic Auth as a stopgap
  until the new frontend lands. Building a Streamlit login flow now
  would be wasted work.

---

## 2. Threat model

What we are defending against, ranked by severity. Each row identifies
**the defense** in this design.

| # | Threat | Realistic? | Defense |
|---|---|---|---|
| 1 | A logged-in user from tenant X queries the API or crafts an HTTP request to read tenant Y data | High — the system has 5 real tenants today | RLS policies on every `marts.*` and `warehouse.*` table. Application code physically cannot escape the policy because Postgres enforces it before returning rows. |
| 2 | An attacker steals a JWT from the wire | Medium — TLS terminates at nginx today | Short-lived access tokens (15 min). Stolen tokens expire quickly. Refresh tokens are stateful and revocable. |
| 3 | An attacker brute-forces `/v1/auth/login` | High — any internet-exposed login is | Argon2id password hashing (slow by design). Per-IP rate limit (5 attempts / 15 min). Failed attempts written to `auth.audit_log` for after-the-fact correlation. |
| 4 | Stolen JWT signing key allows forging arbitrary tokens | Low but catastrophic | Key stored in env var (or Key Vault in §2.4), never in the repo. Documented rotation procedure: dual-key window for ~24h to handle in-flight tokens. |
| 5 | A compromised application bug (SQL injection, deserialization) returns cross-tenant rows | Medium | Same defense as #1 — RLS makes this physically impossible regardless of application behavior. |
| 6 | An insider with DB access reads the `auth.users` table to harvest password hashes | Medium | Hashes are Argon2id with site-wide pepper from env. Even with the table dumped, plaintext recovery is computationally infeasible without the pepper. |
| 7 | Replay of a refresh token after logout | Medium | Refresh tokens are stateful: stored in `auth.refresh_tokens` with `revoked_at`. Logout sets `revoked_at = NOW()`; subsequent refresh attempts fail. |

Out-of-threat-model (explicit non-defenses): DDoS at the application
layer (delegate to nginx / CDN / cloud-provider WAF in §2.4), supply-
chain compromise of a Python dependency (dependabot + manual review
is the answer, separate concern).

---

## 3. Architecture overview

```
                    ┌───────────────────┐
   user / curl ───▶ │  nginx (TLS)      │  unchanged from --profile auth
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────────┐
                    │  FastAPI app          │
                    │  ┌─────────────────┐  │
                    │  │ auth router     │  │  /v1/auth/{login,refresh,
                    │  │                 │  │  logout,me}
                    │  └─────────────────┘  │
                    │  ┌─────────────────┐  │
                    │  │ JWT middleware  │──┼──▶ ContextVar[Principal]
                    │  └─────────────────┘  │       (request-scoped)
                    │           │           │
                    │           ▼           │
                    │  ┌─────────────────┐  │
                    │  │ SQLAlchemy      │  │
                    │  │ session.execute │──┼──▶ on connect: SET LOCAL
                    │  │ event listener  │  │       app.current_tenant
                    │  └─────────────────┘  │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  Postgres             │
                    │  ┌─────────────────┐  │
                    │  │ RLS policies on │  │  WHERE tenant_id =
                    │  │ marts.*,        │  │   current_setting(
                    │  │ warehouse.*     │  │   'app.current_tenant'
                    │  │                 │  │   )::int
                    │  └─────────────────┘  │
                    │  ┌─────────────────┐  │
                    │  │ auth.tenants    │  │  new schema, no RLS
                    │  │ auth.users      │  │  (only auth router reads)
                    │  │ auth.refresh_   │  │
                    │  │   tokens        │  │
                    │  │ auth.audit_log  │  │
                    │  └─────────────────┘  │
                    └───────────────────────┘
```

Key invariants:

- A request without a valid JWT cannot reach any DB query. JWT
  validation happens in middleware, before route handlers run.
- A request with a valid JWT cannot read rows for a tenant other than
  the one in its `tnt` claim. The `SET LOCAL app.current_tenant`
  before every query is what RLS reads, and it is sourced from the
  JWT claim, not from query params or headers.
- The `superadmin` role bypasses RLS via Postgres `BYPASSRLS` role
  attribute (not via an application-level flag). The bypass is
  visible in `pg_roles`, auditable, and revocable with one
  `ALTER ROLE`.

---

## 4. Design choices and rejected alternatives

### 4.1 JWT-with-local-passwords first, OIDC later

**Decision.** Ship JWT-based authentication with local password
storage in `auth.users` for v0.9.0. Design the token-issuer code so
that an OIDC provider can replace local-password issuance without
touching any consumer.

**Rejected: OIDC-only from day one (Azure AD).**
The graduation timeline does not align with the Azure AD tenant
provisioning paperwork the host organization would require. More
importantly, an OIDC-only design hides one of the things this report
needs to demonstrate: that we understand the underlying mechanics of
JWT verification, password hashing, refresh-token lifecycle, and
session management. Treating those as a black-box external concern
weakens the academic story.

**Rejected: session cookies with server-side state.**
Cookie-based auth is the alternative to JWT. Reasons to prefer JWT
here:
- The API may eventually be consumed by a non-browser client (the
  new frontend in §2.3, a mobile companion app, etc.). Bearer tokens
  are universal; cookies are browser-shaped.
- JWT keeps the API stateless for access checks — no Redis-style
  session store needed for the common case.
- Refresh tokens give us the stateful revocation property we want
  without making access tokens stateful.

### 4.2 Postgres RLS, not schema-per-tenant, not application-only filtering

**Decision.** Apply RLS policies on every `warehouse.*` and `marts.*`
table whose rows belong to a tenant. The current SQLAlchemy queries
do not change — they continue to lack `WHERE tenant_id = ...` because
RLS now adds it transparently.

**Rejected: application-layer tenant filtering only.**
The current state. Security depends entirely on every developer
remembering to add `WHERE tenant_id = :tenant` to every query. One
forgotten filter is a cross-tenant breach. For a graduation project
this is the indefensible option — the examiner will rightly ask "what
happens when someone forgets?"

**Rejected: schema-per-tenant (`tenant_5.marts.fact_device_risk_profile`).**
- 5x the DDL maintenance for 5 tenants today; N× for N tenants.
- Migration nightmares — every `ALTER TABLE` has to be applied N times.
- Cross-tenant superadmin queries (`SELECT COUNT(*) FROM ALL.users`)
  become awkward.
- The data volume (single-VM Postgres, sub-TB) does not warrant the
  isolation cost.

**Rejected: a separate database per tenant.**
Same problems as schema-per-tenant, plus connection-pool fan-out.
Reasonable only at much larger scale or with strict regulatory
isolation requirements that we do not have.

### 4.3 Tenant context: ContextVar + SQLAlchemy event, not per-query parameter

**Decision.** A single FastAPI dependency reads the JWT, validates it,
and stores a `Principal(user_id, tenant_id, role)` in a request-scoped
`contextvars.ContextVar`. A SQLAlchemy `before_cursor_execute` event
listener reads the contextvar and issues `SET LOCAL app.current_tenant
= :n` before each statement. RLS policies read
`current_setting('app.current_tenant')::int`.

**Rejected: pass tenant_id explicitly through every query.**
Same failure mode as application-only filtering — one missed call
and you have a leak. The whole point of moving enforcement to RLS is
to remove this footgun.

**Rejected: connection-pool-per-tenant.**
Would require a custom engine factory and lose pgbouncer compatibility.
The `SET LOCAL` approach is the textbook RLS pattern and works with
any pool topology.

---

## 5. Data model

New schema `auth`. None of these tables themselves have RLS — only
the `auth` router reads them, and routes inside it explicitly check
`Principal.role`.

```sql
CREATE SCHEMA IF NOT EXISTS auth;

-- One row per logical customer. tenant_id matches warehouse.dim_tenant
-- on the data side so existing facts/marts need no FK changes.
CREATE TABLE auth.tenants (
    tenant_id     INTEGER PRIMARY KEY
                  REFERENCES warehouse.dim_tenant(tenant_id),
    display_name  TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active     BOOLEAN NOT NULL DEFAULT TRUE
);

-- Users belong to exactly one tenant. Superadmins use tenant_id = NULL
-- and the 'superadmin' role.
CREATE TABLE auth.users (
    user_id        BIGSERIAL PRIMARY KEY,
    tenant_id      INTEGER REFERENCES auth.tenants(tenant_id),
    email          TEXT NOT NULL UNIQUE,
    -- argon2id hash; format includes salt+params so verification is
    -- self-contained.
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

-- Stateful refresh tokens so logout actually revokes. Access tokens
-- stay stateless (JWT-only).
CREATE TABLE auth.refresh_tokens (
    token_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      BIGINT NOT NULL REFERENCES auth.users(user_id)
                 ON DELETE CASCADE,
    issued_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at   TIMESTAMPTZ NOT NULL,
    revoked_at   TIMESTAMPTZ,
    user_agent   TEXT,
    source_ip    INET
);
CREATE INDEX ON auth.refresh_tokens (user_id) WHERE revoked_at IS NULL;

-- Append-only. Used by the operator to investigate "who did X when".
CREATE TABLE auth.audit_log (
    event_id     BIGSERIAL PRIMARY KEY,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    user_id      BIGINT REFERENCES auth.users(user_id),
    tenant_id    INTEGER REFERENCES auth.tenants(tenant_id),
    action       TEXT NOT NULL,
    -- One of: login_success, login_failure, refresh, logout,
    -- token_revoked, admin_create_user, admin_create_tenant,
    -- admin_disable_user, password_reset.
    source_ip    INET,
    user_agent   TEXT,
    -- JSON blob for action-specific context (failure reason, target
    -- user, etc.). Avoids a wide column-soup table.
    detail       JSONB
);
CREATE INDEX ON auth.audit_log (occurred_at DESC);
CREATE INDEX ON auth.audit_log (user_id, occurred_at DESC);
```

### 5.1 Sample RLS policy

Applied to every tenant-owning table. `marts.fact_device_risk_profile`
shown as the canonical example.

```sql
ALTER TABLE marts.fact_device_risk_profile ENABLE ROW LEVEL SECURITY;

-- Tenants see only their own rows.
CREATE POLICY tenant_isolation
    ON marts.fact_device_risk_profile
    FOR ALL
    USING (
        tenant_id = current_setting('app.current_tenant')::int
    );

-- Superadmin role bypasses entirely. BYPASSRLS is a Postgres role
-- attribute, not a policy clause, but the explicit policy here makes
-- the intent legible.
CREATE POLICY superadmin_bypass
    ON marts.fact_device_risk_profile
    FOR ALL
    TO accent_superadmin
    USING (TRUE);
```

The corresponding application code does not change. The query

```python
conn.execute(text("SELECT * FROM marts.fact_device_risk_profile"))
```

returns:
- only the calling tenant's rows when the connection has
  `app.current_tenant = 5` set, regardless of what the SQL says;
- all rows when the connection is opened as the `accent_superadmin`
  role.

This is the property the design is built around.

---

## 6. Token lifecycle

| Aspect | Value | Rationale |
|---|---|---|
| Algorithm | HS256 | API is monolithic for now; shared-secret HMAC is simpler than asymmetric. RS256 is the upgrade when §2.3 frontend or §2.4 multi-service split lands. |
| Access token TTL | 15 minutes | Industry-standard short window. Caps blast radius of a stolen token. |
| Refresh token TTL | 7 days | Long enough that users don't see frequent re-logins; short enough that an abandoned token expires before turning into a long-term liability. |
| Access token claims | `sub` (user_id), `tnt` (tenant_id), `role`, `iat`, `exp`, `jti` | `tnt` is the claim RLS ultimately enforces. `jti` is reserved for a future revocation list if we ever need to revoke access tokens before expiry. |
| Refresh token storage | Server-side row in `auth.refresh_tokens`; client holds an opaque UUID | Stateful so logout revokes immediately. Opaque to the client so no claims need re-validating on rotation. |
| Rotation | One refresh emits a new refresh + new access. Old refresh is marked revoked. | Sliding session with revocation on logout. |
| Signing key | `JWT_SIGNING_KEY` env var, 256-bit random | Rotates by adding a second key (`JWT_SIGNING_KEY_PREVIOUS`) for a 24h window during which both verify; new tokens are signed only with the new key. Documented runbook. |

---

## 7. Request flow

A typical authenticated request to `GET /v1/devices/top-risk`:

1. Client sends `Authorization: Bearer <access-token>`.
2. FastAPI middleware `AuthMiddleware`:
   - Extracts the bearer token.
   - Verifies signature against `JWT_SIGNING_KEY` (with previous-key
     fallback during a rotation window).
   - Checks `exp` is in the future.
   - Builds `Principal(user_id, tenant_id, role)`.
   - Sets the request-scoped `_current_principal: ContextVar` to it.
   - On any failure → 401 short-circuit; no DB connection opened.
3. Route handler resolves `DbDep`. The dependency builds a SQLAlchemy
   connection from the pool.
4. SQLAlchemy `connect` event sets `SET LOCAL app.current_tenant =
   <tenant_id from contextvar>` on every checked-out connection.
5. The route's SQL runs. RLS adds `WHERE tenant_id =
   current_setting('app.current_tenant')::int` transparently.
6. Response is built and serialized. On the way out, the response
   middleware writes an audit row only for state-changing actions
   (login, refresh, logout, admin/*). Reads are not audited at the
   row level — the request log is sufficient and the volume would be
   prohibitive.

---

## 8. API surface

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/v1/auth/login` | None | `{email, password}` → `{access_token, refresh_token, expires_at}`. Writes `login_success` or `login_failure` audit row. Rate-limited per IP. |
| POST | `/v1/auth/refresh` | Refresh token in body | `{refresh_token}` → `{access_token, refresh_token, expires_at}`. Rotates the refresh token. |
| POST | `/v1/auth/logout` | Bearer access OR refresh | Revokes the calling refresh token. 204 No Content. |
| GET | `/v1/auth/me` | Bearer access | Returns `{user_id, email, tenant_id, role, tenant_name}`. Used by the dashboard / frontend to render the current-user UI. |
| POST | `/v1/admin/tenants` | `superadmin` | `{tenant_id, display_name}` → 201. Creates an `auth.tenants` row. tenant_id must already exist in `warehouse.dim_tenant`. |
| POST | `/v1/admin/users` | `superadmin` or `tenant_admin` (own tenant only) | `{email, role, tenant_id, initial_password}` → 201. Password must be rotated on first login (TODO: enforce via a `must_rotate` flag). |
| POST | `/v1/admin/users/{id}/disable` | `superadmin` or `tenant_admin` (own tenant) | Sets `is_active=false`. Existing refresh tokens are also revoked. |
| POST | `/v1/admin/users/{id}/reset-password` | `superadmin` or `tenant_admin` (own tenant) | Generates a new temporary password, revokes all refresh tokens for the user, returns the new password in the response (one-shot). |

The existing `/v1/devices/*`, `/v1/score/*`, `/v1/admin/reload-model`
endpoints gain a hard requirement for a valid bearer token. Versioning
already in place from v0.7.0 — no new prefix mounting.

---

## 9. Migration plan

The risky bit. Applied in this order so the system is never broken
for more than one DDL transaction.

1. **DDL only, no enforcement.** Land the `auth` schema and tables.
   No RLS, no middleware. Existing code runs unchanged. *(Commit M1.)*
2. **Backfill `auth.tenants`.** One row per existing
   `warehouse.dim_tenant` row. Idempotent. *(Commit M1.)*
3. **Seed initial users.** Create one `tenant_admin` per tenant + one
   `superadmin`. Initial passwords are generated by the seed script
   and printed once to its stdout, then immediately forgotten by the
   system. The runbook says: store them in 1Password before closing
   the terminal. *(Commit M2.)*
4. **Ship `/v1/auth/*` endpoints + middleware.** Middleware is
   **opt-in** at this point: protected only by a config flag
   `AUTH_ENFORCEMENT=advisory|enforce`. Advisory mode logs unauth'd
   requests but does not reject them, so we can watch the audit log
   for any client that breaks. *(Commit M3.)*
5. **Switch middleware to `enforce` after one week of clean advisory
   logs.** *(Commit M4.)*
6. **Add RLS policies and the SQLAlchemy event listener.** Policies
   are added on tables one schema at a time; each table-DDL is in its
   own transaction so a mistake on `marts.X` does not lock
   `marts.Y`. *(Commit M5.)*
7. **Switch the application DB user from `accent` (BYPASSRLS) to
   `accent_app` (no BYPASSRLS).** This is the moment RLS actually
   matters — before this, the app role bypasses every policy. Done
   last because rolling back is `ALTER ROLE accent_app BYPASSRLS`,
   not a code revert. *(Commit M6, gated behind a manual
   verification checklist.)*

---

## 10. Test strategy

Three layers; each one independently catches different failure modes.

### 10.1 Unit (pure Python, no DB)

- JWT issuance: claims contain expected `sub`, `tnt`, `role`, `exp`.
- JWT verification: rejects expired, malformed, wrong-signature,
  wrong-issuer tokens.
- Password hashing: `verify(hash(p)) == True`; same `hash(p)` produces
  different ciphertexts (per-call salt).
- Principal extraction: builds the right object from a valid token,
  raises 401-shaped exception on invalid.

### 10.2 Integration (live Postgres, DB-bound)

- `tenant_user` of tenant X with a fresh JWT gets only tenant-X rows
  from `/v1/devices/top-risk` regardless of query params (including
  `?tenant_id=Y` — the param must be ignored or rejected).
- Direct SQL via the app's connection: `SELECT *
  FROM marts.fact_device_risk_profile` with `app.current_tenant=5`
  returns only `tenant_id=5` rows.
- `superadmin` connection (using the BYPASSRLS role) sees all
  tenants.
- `INSERT` from a `tenant_user` connection with a row whose
  `tenant_id` differs from the session's `app.current_tenant`
  must be blocked by the RLS `WITH CHECK` clause.

### 10.3 Negative / fuzz

- 100 randomized invalid tokens (truncated, wrong-alg-claim,
  reversed-byte signatures) all return 401, never 500.
- Login rate limiter rejects the 6th attempt within 15 min from the
  same IP, and the rejection is itself audit-logged.

A failure in any one of these layers must fail CI, with a clearly
named test. The graduation report can then state: "the n cross-tenant
isolation tests pin the invariants described in §3."

---

## 11. Operational concerns

| Concern | Plan |
|---|---|
| Key rotation | Dual-key acceptance window. New key in `JWT_SIGNING_KEY`, old key in `JWT_SIGNING_KEY_PREVIOUS`. Runbook: 24h overlap, then drop the old. Documented in `docs/runbooks/jwt_key_rotation.md` (future). |
| Password reset | `tenant_admin` resets, generates new temp, revokes all refresh tokens. No email flow yet — that requires an SMTP integration we do not have. |
| Rate limiting | In-process token-bucket keyed by source IP, 5 attempts / 15 min for `/v1/auth/login`. Cheaper than wiring up Redis for a single-instance API. Replace with a real distributed limiter when §2.4 splits the API across pods. |
| Audit log retention | Reuse the existing retention policy framework (`§1.5`). Default 365 days; configurable. |
| Audit log read access | `superadmin` only via `GET /v1/admin/audit?since=...`. Never exposed to tenant_admin (could reveal cross-tenant timing). |
| Bootstrap password storage | One-shot stdout from the seed script, manual transcription to 1Password / secrets manager. Documented in `docs/runbooks/initial_provisioning.md` (future). |
| RLS bypass for ETL | The Prefect ETL flow needs to write rows for all tenants. Runs as a separate DB role `accent_etl` which is granted `BYPASSRLS`. Application API role does NOT have it. Two roles, one DB, clear policy. |

---

## 12. Phased rollout (commit-by-commit)

The implementation lands in six labelled commits so the change is
reviewable per phase and partly reversible.

| # | Title | Scope | Reversibility |
|---|---|---|---|
| M1 | `auth: schema DDL + tenants backfill` | `sql/50_auth.sql`, idempotent backfill | Drop `auth` schema |
| M2 | `auth: seed users + superadmin` | one-shot script, runs argon2id hashing | Truncate `auth.users` |
| M3 | `auth: /v1/auth/* endpoints + JWT plumbing (advisory mode)` | `app/auth/`, middleware, routes | Toggle `AUTH_ENFORCEMENT=off` |
| M4 | `auth: flip middleware to enforce` | one-line config + docs | Toggle back to advisory |
| M5 | `auth: RLS policies on marts.* + warehouse.*` | DDL only, app code untouched | `ALTER TABLE … DISABLE ROW LEVEL SECURITY` per table |
| M6 | `auth: switch app role to non-BYPASSRLS accent_app` | `.env` change + grants | `ALTER ROLE accent_app BYPASSRLS` |

Each commit lands with its own tests (unit for M3–M4, integration for
M5–M6). Tag `v0.9.0` after M6 verifies clean against the integration
suite.

---

## 13. Open questions

- **Tenant invitation flow.** Today users land in the DB through the
  superadmin admin endpoint. Eventually we want self-service tenant
  signup with email verification. Out of scope for v0.9.0.
- **Cross-tenant analytics for superadmin.** Some dashboards (the
  executive view) deliberately aggregate across tenants. The
  superadmin BYPASSRLS path handles this, but the dashboard needs to
  declare its intent explicitly to avoid accidental cross-tenant
  reads when a tenant_admin views the same page.
- **Token transport.** Bearer in `Authorization` header is canonical
  for the API. The Streamlit dashboard would need its own login
  page; deferred to §2.3 frontend.
- **Audit log of read access.** Currently audit covers writes and
  auth events. PII concerns may require auditing reads to specific
  tables. Punt to a follow-up if the use case materializes.

---

## 14. References

- OWASP ASVS v4.0, §V2 (Authentication) and §V3 (Session management).
- Postgres docs: Row Security Policies
  (https://www.postgresql.org/docs/16/ddl-rowsecurity.html).
- RFC 7519 (JSON Web Token), RFC 7515 (JWS), RFC 6819 (OAuth2
  threat model).
- Argon2id: PHC winner, OWASP-recommended password-hashing primitive.
- "Designing Multi-Tenant Applications" — common patterns and their
  trade-offs.
