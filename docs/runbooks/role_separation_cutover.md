# Runbook: Switch the API to `accent_app` (RLS goes live)

> **Tracks:** `docs/auth_design.md` §2.2 milestone M6.
> **Audience:** the on-call operator running the role cutover.
> **Pre-conditions:** M1–M5 deployed, `AUTH_ENFORCEMENT=enforce` stable
> for at least 7 days, `sql/51_rls_policies.sql` applied.

Until M6, the RLS policies installed by M5 are **dormant**: the API
connects as the BYPASSRLS-capable owner role (`accent_admin`), so
Postgres skips policy evaluation. M6 introduces three purpose-built
roles and points the API at the only one without BYPASSRLS. After
cutover, a missing `tenant_id` filter no longer leaks rows — Postgres
refuses to return them.

This runbook is deliberately conservative. The flip touches three
moving pieces (DB roles, API env, Prefect env) and a mistake is
silent — `accent_app` accidentally created with BYPASSRLS leaves you
believing you're protected when you aren't. Each step has an explicit
verification gate.

---

## 0. The three roles you're about to create

| Role | BYPASSRLS | SUPERUSER | Used by |
|------|---|---|---|
| `accent_app` | **no** | no | FastAPI service (the one that feels RLS) |
| `accent_etl` | yes | no | Prefect flows, `scripts/seed_auth.py`, batch scoring |
| `accent_superadmin` | yes | yes | Schema migrations, ad-hoc op tooling |

Only `accent_app` is constrained. The other two keep cross-tenant
reach because pipelines that are *defined* to walk every tenant's data
shouldn't fight the policies.

---

## 1. Generate three strong passwords

Run on your laptop (don't reuse one password for multiple roles):

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"   # APP
python -c "import secrets; print(secrets.token_urlsafe(32))"   # ETL
python -c "import secrets; print(secrets.token_urlsafe(32))"   # SUPERADMIN
```

Stash them in your password manager **before** moving on. The DDL
will reject obvious placeholders (single quotes in `__APP_PASSWORD__`
would cause a parse error if accidentally left in).

---

## 2. Apply the DDL with the placeholders filled

Copy `sql/52_role_separation.sql` to a scratch path, substitute the
three placeholders, then apply:

```bash
cp sql/52_role_separation.sql /tmp/52_role_separation_with_passwords.sql

# Use perl, not sed — handles arbitrary chars in the password (sed -i
# is fragile with /, &, etc.). Quote the env-var to avoid shell parse.
APP_PW='paste-app-password-here' \
ETL_PW='paste-etl-password-here' \
SUPERADMIN_PW='paste-superadmin-password-here' \
perl -i -pe '
  s/__APP_PASSWORD__/$ENV{APP_PW}/g;
  s/__ETL_PASSWORD__/$ENV{ETL_PW}/g;
  s/__SUPERADMIN_PASSWORD__/$ENV{SUPERADMIN_PW}/g;
' /tmp/52_role_separation_with_passwords.sql

# Apply as the current owner (still accent_admin at this point).
psql "$PG_URL" -f /tmp/52_role_separation_with_passwords.sql

# Destroy the temp file. It contains plaintext passwords.
shred -u /tmp/52_role_separation_with_passwords.sql
```

If `shred` is unavailable (some macOS/Windows shells), fall back to
`rm -P` (BSD) or `sdelete -p 3` (Windows Sysinternals).

---

## 3. Verify the role attributes

This is the **most important verification in the whole runbook**.

```sql
SELECT rolname, rolbypassrls, rolsuper, rolcanlogin
FROM pg_roles
WHERE rolname LIKE 'accent\_%' ESCAPE '\'
ORDER BY rolname;
```

Expected:

| rolname             | rolbypassrls | rolsuper | rolcanlogin |
|---------------------|--------------|----------|-------------|
| `accent_app`        | **f**        | f        | t           |
| `accent_etl`        | t            | f        | t           |
| `accent_superadmin` | t            | t        | t           |

**If `accent_app` shows `rolbypassrls = t`, STOP.** Run
`ALTER ROLE accent_app NOBYPASSRLS;` and re-verify before continuing.
A BYPASSRLS `accent_app` silently disables tenant isolation.

---

## 4. Smoke-test the policy on a throwaway connection

Before pointing the live API at `accent_app`, verify RLS actually
behaves the way you expect:

```bash
# Connect AS accent_app.
PGPASSWORD='<app password>' psql -h $PG_HOST -U accent_app -d accent_fleet
```

```sql
-- 4a. Without setting the GUC: zero rows visible.
SELECT COUNT(*) FROM warehouse.fact_trip;
-- expect: 0

-- 4b. With the GUC set to a real tenant: that tenant's rows visible.
SET LOCAL app.current_tenant = '1';
SELECT tenant_id, COUNT(*) FROM warehouse.fact_trip GROUP BY tenant_id;
-- expect: one row, tenant_id=1, count>0

-- 4c. Wrap that in a transaction and try setting two tenants — only
-- one survives per statement boundary.
BEGIN;
SET LOCAL app.current_tenant = '1';
SELECT COUNT(*) FROM warehouse.fact_trip;   -- tenant 1 count
SET LOCAL app.current_tenant = '2';
SELECT COUNT(*) FROM warehouse.fact_trip;   -- tenant 2 count
COMMIT;
```

If 4a returns rows (not zero), `accent_app` still has BYPASSRLS or a
policy is missing — back to §3.

---

## 5. Flip the API's credentials

Edit the API's environment:

```diff
- PG_USER=accent_admin
- PG_PASSWORD=<old admin password>
+ PG_USER=accent_app
+ PG_PASSWORD=<the APP password from §1>
```

Restart the API:

```bash
docker compose restart api
```

Then immediately verify the API still functions end-to-end:

```bash
# 5a. Login still works (auth uses auth.* tables, which accent_app can read).
TOKEN=$(curl -s -X POST http://localhost:8000/v1/auth/login \
    -H 'content-type: application/json' \
    -d '{"email":"admin@tenant_1.local","password":"<seed pw>"}' \
  | jq -r .access_token)
test -n "$TOKEN" && echo "login OK"

# 5b. A tenant_admin sees their own tenant.
curl -s -H "authorization: bearer $TOKEN" \
     http://localhost:8000/v1/devices?limit=3 | jq .

# 5c. /v1/auth/me returns the expected tenant.
curl -s -H "authorization: bearer $TOKEN" \
     http://localhost:8000/v1/auth/me | jq .
```

---

## 6. Cross-tenant smoke test (the whole point of this work)

Acquire tokens for *two different tenants* (use either two seeded
`tenant_admin`s or a superadmin who lists every tenant) and confirm
the API never crosses the boundary:

```bash
# Token for tenant 1 user.
T1=$(curl ... )
# Token for tenant 2 user.
T2=$(curl ... )

# Sample a device ID that belongs to tenant 2.
T2_DEVICE=$(curl -s -H "authorization: bearer $T2" \
                http://localhost:8000/v1/devices?limit=1 | jq -r '.[0].device_id')

# Hit that device with TENANT 1's token. Must NOT return data.
curl -i -H "authorization: bearer $T1" \
     "http://localhost:8000/v1/devices/$T2_DEVICE"
# expect: HTTP/1.1 404 (RLS returns no rows; the route 404s as a result)
```

A `200` with tenant-2 data on a tenant-1 token is a **critical
finding** — abort, run §8 rollback, and investigate.

---

## 7. Flip Prefect + scripts to `accent_etl`

The pipeline and the seed script keep BYPASSRLS via a different role.
Edit Prefect's secrets/env:

```diff
- PG_USER=accent_admin
+ PG_USER=accent_etl
```

```diff
- PG_PASSWORD=<old admin password>
+ PG_PASSWORD=<the ETL password from §1>
```

Trigger an incremental flow run and watch the logs:

```bash
prefect deployment run accent-fleet/incremental-batch
prefect flow-run inspect <run-id>
```

If a `permission denied for table` shows up, the most likely cause is
a table created BEFORE the `ALTER DEFAULT PRIVILEGES` lines in §2
took effect — manually `GRANT` it to `accent_etl` and the default
privileges will cover future tables.

---

## 8. Rollback (symmetric)

If anything in §5–§7 looks wrong:

```diff
- PG_USER=accent_app
+ PG_USER=accent_admin
- PG_PASSWORD=<APP>
+ PG_PASSWORD=<old admin password>
```

```bash
docker compose restart api
```

Tenants again see everything they used to, RLS is dormant. No state
is lost. The three new roles can sit unused until the next attempt;
no rush to `DROP ROLE`.

---

## 9. After the cutover — housekeeping

Once `accent_app` has been live and clean for a week:

- Revoke the `accent_admin` super-role from anything that doesn't
  need it. Treat it the way you'd treat a `postgres` superuser
  password.
- Confirm the audit log isn't blooming with `auth.*` errors that
  weren't there pre-cutover.
- Update `NEXT_STEPS.md` §2.2 to mark M6 complete and v0.9.0 closed.

The work delivered by §2.2 of the design doc is now done: every
tenant_id-scoped row in the warehouse and marts is filtered by
Postgres before it ever crosses the wire to the API.
