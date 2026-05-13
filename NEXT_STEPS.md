# Accent Fleet Analytics — Post v0.5.0 Roadmap

> Status as of 2026-05-13: v0.9.0 is tagged. Auth + multi-tenancy
> (§2.2) shipped across six milestones M1–M6: argon2id-hashed users,
> HS256 JWT with dual-key rotation, opaque refresh tokens with
> server-side state, `AuthMiddleware` with advisory/enforce/off modes,
> a `tenant_isolation` RLS policy on every tenant-owning table, and a
> three-role separation runbook that takes the API to NOBYPASSRLS.
>
> Earlier: v0.8.0 shipped silhouette-gated retraining with a
> frozen-dataclass audit trail and a compose-managed `retrain-
> scheduler` (supercronic, first Monday of month). v0.7.0 shipped
> structured logging + `/metrics`, `/v1` API versioning with a
> deprecation-headed legacy mount, a `pg_dump`-based backup script,
> the `/devices/{id}/profile` perf fix, and feature-level KL-
> divergence drift detection. Everything below is what is still **not
> yet** in the platform.

The remaining work splits cleanly into two halves:

- **Part 1 — Hardening sprint.** Tactical, sprint-sized. Closes the real gaps
  in what we already shipped. Unblocks the platform going beyond the two-person
  hand-off.
- **Part 2 — Path to v1.0.** The phases the original plan explicitly deferred:
  streaming, auth, cloud deploy, multi-tenant UI. Each is a multi-week effort
  on its own.

Do Part 1 first. Part 2 only makes sense once Part 1 is solid.

---

## Part 1 — Hardening sprint  ✅ **closed at v0.6.0** (kept as a changelog)

The original DOD said *"Tag `v0.6.0` when all of 1.1–1.5 are done"* — v0.6.0
was tagged. The tables below are preserved as a changelog so the line-item
trail is visible. The two unchecked items are deferred deliberately, not
forgotten: see notes per row. A separate **§1.6 Residual paper-cuts** below
collects what's still genuinely open from Part 1.

### 1.1 Real bugs

| Item | Status |
|---|---|
| `/devices/top-risk` 500s without `tenant_id` (untyped NULL parameter) | ✅ v0.6.0 — `app/routes/devices.py` branches the SQL on `tenant_id is None` instead of using `(:x IS NULL OR ...)`. Regression-pinned in `tests/test_api_devices.py::test_top_risk_without_tenant_does_not_500`. |
| `streamlit-aggrid` declared in `requirements.txt` but never imported | ✅ v0.6.0 — removed from requirements; dashboard pages use native `st.dataframe`. |
| MLflow client/server pinned — bump server to match? | ⏭ Deferred — current pin (server v2.16.2 / client v2.16.x) is stable; bump it the next time it actively blocks something rather than chasing version drift. |

### 1.2 Test coverage gaps from the original plan

| Item | Status |
|---|---|
| `tests/test_api_devices.py` — covers `/devices/top-risk` (with and without tenant_id) and `/devices/{id}/profile` | ✅ v0.6.0 — 8 tests; DB-bound tests auto-skip when Postgres isn't reachable so CI passes without a database. |
| `tests/test_dashboard_smoke.py` — headless Streamlit + page-load asserts | ⏭ Deferred — Streamlit is on the deprecate-list (see §2.3). Building a smoke test for code we plan to replace is poor ROI. |
| SQL parity assertion — `cluster_id` from API equals `cluster_id` in `marts.fact_device_cluster_assignment` | 📌 **Open — graduation-relevant.** Concrete evidence that online and offline compute paths agree. Tracked in §1.6. |
| Performance check — p95 for `/score/risk` (<100ms) and `/devices/{id}/profile` (<300ms), numbers in README | 📌 **Open — graduation-relevant.** Concrete latency numbers belong in the report. Tracked in §1.6. |

### 1.3 CI

| Item | Status |
|---|---|
| GitHub Actions workflow that runs `pytest -m "not slow"` on every push and PR | ✅ v0.6.0 — `.github/workflows/ci.yml`, jobs `lint (ruff)` + `test (pytest)` + `docker (build base image)`. |
| Same workflow runs `docker compose build base` to catch Dockerfile breakage | ✅ v0.6.0 — `docker (build base image)` job in the same workflow. |
| Branch protection on `main` requiring CI green before merge | ⏭ Deliberately skipped — single-maintainer repo, see commit `09afaacf` ("docs: record decision to skip branch protection on main"). Revisit when a second contributor lands. |

### 1.4 API operability

| Item | Status |
|---|---|
| `POST /admin/reload-model` endpoint — picks up a freshly-promoted Production model without container restart | ✅ v0.6.0 — `app/routes/admin.py`, fail-closed `X-API-Key` auth, calls `ClusterPredictor.reload()`. Critical now that the v0.8.0 retrain scheduler can promote candidates while the API is up. |
| Dashboard footer: "Last ETL refresh: {timestamp}" pulled from `warehouse.etl_run_log` | 📌 Open — minor UX, ~1hr. Tracked in §1.6. |

### 1.5 Minimum operational hygiene

| Item | Status |
|---|---|
| Document `.env` hand-off process | 📌 Open — boring but worth doing before the project changes hands. Tracked in §1.6. |
| Retention policy on `warehouse.etl_run_log` and `marts.fact_device_cluster_assignment` | ✅ v0.6.0 — daily Prefect task purges rows older than the configured retention horizon. |
| Backup the MLflow SQLite + artifacts volume | ✅ v0.6.0 — `docker/scripts/backup_mlflow.sh` plus the matching `backup_postgres.sh` added in v0.7.0. |
| Auth — stick API + dashboard behind a single nginx with HTTP basic auth, or an API key middleware | ✅ v0.6.0 — `--profile auth` adds an nginx reverse-proxy with htpasswd-protected access to dashboard / mlflow / api. Stopgap only; the durable answer is §2.2. |

### 1.6 Residual paper-cuts (still genuinely open)

Not blocking the platform; collected here so they aren't forgotten:

- **SQL parity assertion** (§1.2 row 3) — extend `tests/test_feature_computation.py` to cross-check `cluster_id` between the API and `marts.fact_device_cluster_assignment` for a sample of devices. ~2hr. Graduation-defensive.
- **Performance check** (§1.2 row 4) — one-off `locust`/`ab` run against a warm stack; record p95 latency in the README. ~2hr. Graduation-defensive (concrete numbers for the report).
- **Dashboard ETL-refresh footer** (§1.4 row 2) — ~1hr UX polish.
- **`.env` hand-off docs** (§1.5 row 1) — ~30 min, README "Deployment" section.

Total residual: ~6 hours. Pick these up opportunistically between bigger
chunks, or in a single afternoon before the final report draft.

---

## Part 2 — Path to v1.0 (multi-month)

The original plan parked these explicitly. Each is a real phase of work, not a
ticket. They are listed roughly in the order they unblock each other.

### 2.1 Streaming activation

The `_deferred/` consumer code exists, the `redpanda` service is in
`docker-compose.yml` behind `--profile streaming`, but nothing actually
consumes. To turn this on:

- Wire one Kafka consumer per staging table (`staging.path`, `staging.archive`,
  …) that the current batch flow loads from Azure Postgres.
- Switch the watermark semantics: today watermarks track `last_seen_id` per
  staging table; for streaming they need to track Kafka offsets per partition.
- Run consumers under Prefect deployments instead of a `while true` loop.
- Build a back-pressure story for when the consumer falls behind the producer.
- Decide retention: do we keep raw Kafka events for replay, or land them in
  Postgres and trust that as the durable store?

Estimated: 3–4 weeks. Mostly architectural decisions, not code volume.

### 2.2 Real auth + multi-tenancy  ✅ v0.9.0 — shipped 2026-05-13

Part 1.5 landed "any auth at all" (nginx + htpasswd, single shared
credential). v0.9.0 replaces that stopgap with platform-grade auth:

  - **M1** — `auth` schema (tenants, users, refresh_tokens, audit_log)
    with backfill from `warehouse.dim_tenant`.
    *(`sql/50_auth.sql`, commit `ab72d32c`.)*
  - **M2** — `scripts/seed_auth.py` creates one tenant_admin per
    active tenant + one superadmin, argon2id hashing with OWASP
    parameters. *(commit `dd7f4315`.)*
  - **M3** — `app/auth/` package: JWT (HS256, dual-key rotation),
    refresh tokens (opaque UUID + server-side state), in-process
    token-bucket rate limiter, `AuthMiddleware`, `/v1/auth/login |
    refresh | logout | me` and `/v1/admin/tenants | users | users/{id}/
    disable | users/{id}/reset-password`. Ships in `advisory` mode by
    default (logs but does not 401). 30 unit + middleware tests.
    *(commit `08cd7a3a`.)*
  - **M4** — `docs/runbooks/auth_enforcement_flip.md`: the 7-day
    watch period + flip-gate procedure to move
    `AUTH_ENFORCEMENT=advisory → enforce`. No code; the M3 middleware
    already reads the env var per request. *(commit `8912ccf6`.)*
  - **M5** — `sql/51_rls_policies.sql` puts a `tenant_isolation`
    policy on every tenant-owning table in `warehouse.*` and
    `marts.*` (24 objects total). `src/accent_fleet/db/engine.py`
    grows a SQLAlchemy `begin` listener that issues `SET LOCAL
    app.current_tenant` from the request's Principal. Policies are
    loaded but dormant until M6 swaps the role.
    *(commit `49ea7e50`.)*
  - **M6** — `sql/52_role_separation.sql` creates `accent_app`
    (NOBYPASSRLS), `accent_etl` (BYPASSRLS), `accent_superadmin`
    (BYPASSRLS + SUPERUSER). The DDL is NOT auto-applied;
    `docs/runbooks/role_separation_cutover.md` walks the operator
    through password generation, the `rolbypassrls=f` verification
    gate, the cross-tenant smoke test, and the symmetric rollback.
    *(commit `51a99f10`.)*

Deferred to v0.10.0: OIDC / Azure AD swap-in (the JWT claim shape
is already OIDC-compatible) and a tenant-aware admin UI.

See [`docs/auth_design.md`](docs/auth_design.md) for the threat
model, data model, and rejected-alternatives appendix.

### 2.3 Production frontend (deprecate Streamlit)

Streamlit was always a v1 choice. By the time multi-tenant + auth land, it
will be limiting:

- Replace `dashboard/` with a Next.js or Remix app.
- Keep the FastAPI service as the only backend (currently the dashboard reads
  Postgres directly — that needs to stop once we have RLS-aware API auth).
- Charting: keep Plotly via `react-plotly.js`, or switch to ECharts / Recharts
  for tighter React integration.

Estimated: 6–8 weeks. Coincides naturally with 2.2.

### 2.4 Cloud deployment

Today: `docker compose up` on someone's laptop or a single Azure VM. Real
deployment:

- Container registry (ACR / GHCR) for the built images.
- Kubernetes (AKS) or Azure Container Apps for runtime.
- Terraform for the AKS cluster, Postgres flexible server, key vault, log
  analytics workspace, etc.
- GitHub Actions deploys on tag push: `v*` → staging, manual approval → prod.
- Secrets via Key Vault, not `.env`.
- Replace the MLflow SQLite backend with the Postgres backend (file-on-volume
  doesn't survive a pod restart cleanly).

Estimated: 3–4 weeks if the team already knows AKS + Terraform. Double that
if not.

### 2.5 ML platform maturity

- ~~**Drift detection** — Prefect task computing PSI per feature against a
  rolling reference window, threshold 0.25, exposed as
  `accent_ml_feature_drift_score{feature=...}`.~~ Shipped in v0.7.0.
- ~~**Retraining cadence** — schedule `train_clustering.py` monthly, gate
  promotion to Production on silhouette ≥ training-time score − 0.02.~~
  Shipped in v0.8.0. Scheduler is the compose `retrain-scheduler` service
  (supercronic, `--profile scheduler`).
- **Model A/B** — extend the API to optionally route a percentage of traffic
  to the `Staging` model and log both predictions for offline comparison.
  *Next natural chunk.*
- **Risk model upgrade** — current risk score is a hand-tuned formula in
  `risk_score.py`. Replace with a supervised model once labelled outcome data
  (accidents / claims) is available. *Blocked on labels.*

Estimated: ongoing. A/B routing is the next 1–2 week chunk.

### 2.6 Geo

Once lat/long lands in `marts.fact_trip` (currently we have aggregates only):

- Add a "Fleet map" page using `pydeck`. Heat-map of harsh events. Route
  replays for a selected device.
- Geocode the depot list and overlay it.

Estimated: 1–2 weeks once the upstream data is there.

---

## Sequencing notes

- **Part 1 is closed at v0.6.0.** The §1.6 residuals are nice-to-haves, not
  gates — they get folded into normal iteration.
- **§2.2 closed at v0.9.0.** Auth + multi-tenancy shipped across M1–M6; the
  M4 enforce-flip and M6 role-cutover are operator-driven runbooks, not
  pending code. The next active chunk inside Part 2 is `2.5 model A/B
  routing → 2.3 frontend → 2.4 cloud`. A/B routing is now unblocked because
  the v0.8.0 scheduler has been accumulating gate decisions throughout the
  §2.2 build.
- Streaming (§2.1) can run in parallel with any of the above but is currently
  deferred pending Kafka data access from the source system.
- ML maturity (§2.5) — drift detection + retraining cadence already shipped
  at v0.7.0 / v0.8.0. The remaining two items (A/B routing, supervised risk
  score) are both *partly blocked* — A/B on signal accumulation, supervised
  on label availability.
- §2.6 Geo is independent and unscheduled.
- Calendar: ~4–6 weeks for §2.2, then re-evaluate. By the time §2.2 lands
  there should be enough scheduler history to decide whether §2.5 A/B is
  worth doing or whether the gate is sufficient on its own.

## Out of scope for this document

- Vendor/build decisions inside Part 2 (Next.js vs Remix, AKS vs ACA, etc.) —
  those get their own RFCs when each phase starts.
- Cost modeling — depends on cloud choice in 2.4.
- Team / hiring — also a function of where 2.2–2.4 land.
