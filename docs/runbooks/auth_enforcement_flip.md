# Runbook: Flip `AUTH_ENFORCEMENT` from `advisory` to `enforce`

> **Tracks:** `docs/auth_design.md` §6 "Migration plan" step 5 + §12 milestone M4.
> **Audience:** the on-call operator doing the cutover.
> **Pre-conditions:** M1, M2, M3 already deployed.

The middleware in `app/auth/middleware.py` reads
`Settings.auth_enforcement` on **every request**, so the flip is a
configuration change, **not** a code change. The point of this runbook
is to make sure the watch period before the flip catches every client
the team forgot about, and that the rollback path is one env-var edit
away.

---

## 0. Three modes — what each means

| Mode | Behaviour for a request with **no** bearer token | Behaviour for a request with an **invalid** token | Use when |
|------|---|---|---|
| `off` | Pass; principal stays `None`. No auth events logged. | (Token is not inspected.) | Local dev only. |
| `advisory` *(default)* | Pass; emit `auth.missing_token` to audit log. | Pass; emit `auth.invalid_token`. | Soft launch. Watch the log for clients you forgot. |
| `enforce` | **401** with `WWW-Authenticate: Bearer`. | **401**. | Production. |

`/health`, `/metrics`, `/v1/auth/login`, `/v1/auth/refresh`, `/docs`,
`/openapi.json`, `/redoc`, and `/` are exempt in every mode (see
`_EXEMPT_PATHS` in `app/auth/middleware.py`).

---

## 1. Watch period (minimum 7 days in `advisory`)

The first M3 deploy lands with `AUTH_ENFORCEMENT=advisory`. **Do not
flip on day 1.** Run for at least one full business week so weekly
batch jobs, monitoring probes, and any humans on PTO have had a chance
to exercise their codepaths.

### 1.1 What to watch in the audit log

```sql
-- Top offenders by source IP, last 7 days.
SELECT source_ip,
       COUNT(*) AS unauth_requests,
       array_agg(DISTINCT detail->>'path') AS paths_hit
FROM auth.audit_log
WHERE action IN ('auth.missing_token', 'auth.invalid_token')
  AND occurred_at > now() - INTERVAL '7 days'
GROUP BY source_ip
ORDER BY unauth_requests DESC
LIMIT 20;
```

Every row in that result is a **client that will break when you flip
to `enforce`**. Reach each owner; either:

- Issue them a service account + refresh token, **or**
- Add their callsite under `/health` or `/metrics` (operational paths
  remain exempt), **or**
- Decide the client is dead and silence the source.

### 1.2 The flip gate

You may flip when **all four** are true:

1. The unauth-request query above returns **zero rows** for the last
   72 hours (a long weekend's worth of traffic).
2. `JWT_SIGNING_KEY` is set to a value with **>= 32 bytes** of
   entropy (HS256 RFC 7518 §3.2 minimum). Verify:
   ```bash
   docker compose exec api python -c 'import os; print(len(os.environ["JWT_SIGNING_KEY"]))'
   ```
3. At least one **superadmin** user can successfully log in via
   `POST /v1/auth/login` and exercise a protected endpoint. Run:
   ```bash
   TOKEN=$(curl -s -X POST http://localhost:8000/v1/auth/login \
       -H 'content-type: application/json' \
       -d '{"email":"superadmin@accent.local","password":"<from-seed>"}' \
     | jq -r .access_token)
   curl -s -H "authorization: bearer $TOKEN" http://localhost:8000/v1/auth/me
   ```
   The response must include `"role":"superadmin"`.
4. The rollback procedure in §3 is rehearsed (or at minimum re-read).

---

## 2. The flip

### 2.1 Edit `.env`

```diff
- AUTH_ENFORCEMENT=advisory
+ AUTH_ENFORCEMENT=enforce
```

### 2.2 Restart the API service

Because `Settings()` is `@lru_cache`-singleton'd, a fresh process is
required to pick up the new env var:

```bash
docker compose restart api
```

### 2.3 Verify enforcement

```bash
# 1. No token → must return 401.
curl -i http://localhost:8000/v1/score/risk -d '{}'   # expect: HTTP/1.1 401

# 2. Exempt path → must still pass.
curl -i http://localhost:8000/health                  # expect: HTTP/1.1 200

# 3. Valid token → must pass.
curl -i -H "authorization: bearer $TOKEN" \
     http://localhost:8000/v1/auth/me                 # expect: HTTP/1.1 200
```

All three checks **must** succeed before you walk away.

### 2.4 Confirm the audit log shows the new state

```sql
SELECT action, COUNT(*)
FROM auth.audit_log
WHERE occurred_at > now() - INTERVAL '10 minutes'
GROUP BY action;
```

You should see `auth.login_success` and (zero or more)
`auth.login_failure` rows. You should **not** see new
`auth.missing_token` events — those are now 401'd before the audit
write would happen.

---

## 3. Rollback

If protected endpoints are returning unexpected 401s in production
(genuine clients are being rejected), the rollback is symmetric:

```diff
- AUTH_ENFORCEMENT=enforce
+ AUTH_ENFORCEMENT=advisory
```

```bash
docker compose restart api
```

The system returns to advisory mode in < 30s. **You have not lost any
state** — refresh tokens, audit history, and password hashes are
unchanged. The flip is purely about *response policy*, so rollback is
genuinely free.

Investigate the broken client(s), fix them, and restart the watch
period from §1.

---

## 4. After the flip — what's next

Once `enforce` has been stable for at least a week:

- **M5** lands RLS policies on `marts.*` and `warehouse.*` so a SQL bug
  cannot cause cross-tenant data leakage even with a valid token.
- **M6** swaps the application's Postgres role from BYPASSRLS to
  `accent_app` (no BYPASSRLS) so the RLS policies actually fire.

Until M5/M6 ship, isolation depends on application-layer tenant
filters. After M6 it depends on Postgres itself.
