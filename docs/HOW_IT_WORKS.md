# How the app works — end-to-end walkthrough

A single document that connects every piece of `accent-fleet-analytics`
into one story. Aimed at someone who can already build and run the stack
but is fuzzy on **why there are so many moving parts and which one does
what**.

Companion to:
- [`README.md`](../README.md) — quick start + repo layout.
- [`REFACTOR_RATIONALE.md`](../REFACTOR_RATIONALE.md) — why this design
  replaced the v1 SQL-only pipeline.
- [`docs/auth_design.md`](auth_design.md) — deep dive on the JWT + RLS
  model that ships in v0.9.0.
- [`NEXT_STEPS.md`](../NEXT_STEPS.md) — milestone changelog and roadmap.

---

## 1. What is this thing?

**Accent Fleet Analytics** ingests raw telematics events from a fleet of
GPS-instrumented vehicles (~600 devices, ~50M raw rows, multiple tenants)
and turns them into two products:

1. **A risk score per device, per month** — "how dangerously is device
   `42` being driven *this* month?" Composite formula + KMeans clusters.
   Consumed via a REST API (FastAPI) and a what-if scoring page.
2. **A fleet-BI dashboard** — executive KPIs, daily ops volume,
   maintenance cost leaderboards, harsh-event hotspots. Streamlit.

The repo is structured around the **CRISP-DM** methodology
(Cross-Industry Standard Process for Data Mining): six phases from
"business understanding" through "deployment", each with a numbered
notebook folder so the work is traceable.

> **Unit of analysis: device-month.** The original plan said "driver",
> but `staging.assignment` only covers 12 of 633 devices (1.9%). See
> [REFACTOR_RATIONALE.md §6](../REFACTOR_RATIONALE.md) for the pivot.
> Anywhere you see `device_id` in the codebase, that's the unit; driver
> attribution exists where the assignment table can prove it but is
> never required.

---

## 2. The 30,000-foot picture

```
                    ┌──────────────────────────────────────┐
  source system     │  Azure Postgres "raw" (staging.*)    │
  (out of repo)     │  path, stop, archive, notification,  │
                    │  vehicle, device, maintenance, …      │
                    └────────────────┬─────────────────────┘
                                     │
                          ┌──────────▼──────────┐
                          │   ETL container     │  Prefect flows.
                          │   (accent_etl)      │  Runs incrementally
                          │   role: accent_etl  │  every 5 min.
                          └──────────┬──────────┘
                                     │
                    ┌────────────────▼──────────────────────┐
                    │  warehouse.* (silver)                 │
                    │  dim_tenant, dim_vehicle, dim_device, │
                    │  fact_trip, fact_overspeed, …         │
                    │  + etl_watermark + etl_run_log        │
                    └────────────────┬──────────────────────┘
                                     │ same ETL rebuilds…
                    ┌────────────────▼──────────────────────┐
                    │  marts.* (gold)                       │
                    │  ML side    : mart_device_monthly_*   │
                    │               v_ml_features_full      │
                    │  BI side    : mart_fleet_daily        │
                    │               mart_tenant_monthly     │
                    │               v_executive_dashboard   │
                    │               v_operational_dashboard │
                    │               v_maintenance_dashboard │
                    │               v_fleet_risk_dashboard  │
                    └────┬───────────────────────────┬──────┘
                         │ role: accent_app          │ role: accent_app
                         │ (NOBYPASSRLS — RLS clamps)│ (with superadmin
                         │                           │  Principal at
                  ┌──────▼──────┐             ┌──────▼──────┐ page init)
                  │   API       │             │  Dashboard  │
                  │  (FastAPI)  │             │ (Streamlit) │
                  │  port 8000  │             │  port 8501  │
                  │             │             │             │
                  │ JWT auth    │◄────────────│  hits API   │
                  │ /v1/score/* │   What-If   │  for ad-hoc │
                  │ /v1/devices │   page only │   scoring   │
                  │ /v1/admin/* │             │             │
                  └─────────────┘             └─────────────┘
                         │
                         │   reads model
                  ┌──────▼──────┐
                  │   MLflow    │   Tracks training runs.
                  │  registry   │   Holds the Production
                  │  port 5000  │   KMeans model the API
                  └─────────────┘   loads at boot + reload.
```

Two things to internalise:

- **Every arrow above is incremental, idempotent, and watermark-driven.**
  Re-running the pipeline on the same window produces the same rows.
  This is the central refactor v0.5.0 made over the v1 SQL-script
  pipeline.
- **The dashboard talks to Postgres directly** (not through the API)
  for read traffic, and to the API only for what-if scoring. That's a
  v1 simplification; replacing it with API-only access is part of §2.3
  in `NEXT_STEPS.md`.

---

## 3. The five layers of data

Postgres is the only datastore. Everything else (object storage,
warehouses, lakes) was deliberately rejected to keep the v1 stack
operable by one person. Inside Postgres we use **schema as layer**:

| Schema | What's in it | Who writes | Who reads |
|---|---|---|---|
| `staging` | Raw rows from the source system. One-to-one with the upstream tables. Cleaned only by C1–C7 rules (see §6.1). | The source loader (out of repo) | The ETL flow |
| `warehouse` | Dimensions + facts in star-schema shape. Stable contract. Backfilled once + extended every 5 min. | Prefect `incremental_flow` | Marts, ad-hoc analyses |
| `marts` | Aggregates joined to the grain we report at: device-month for ML, day/vehicle-month/tenant-month for BI. | Prefect (same flow, later tasks) | Views, dashboard, API |
| `marts` (views) | `v_*` views on top of marts. Add window functions, MoM deltas, risk categorisation. Cheap, no storage. | Defined once in `sql/20-35_*.sql` | Dashboard pages, `/score/*` (training only) |
| `auth` | Auth model: tenants, users, refresh tokens, audit log. Added in M1 (v0.9.0). | seed_auth.py + `/v1/admin/users` | API auth middleware |

The same Prefect flow walks the first three layers in one pass: load
new facts → rebuild only the months/dates those new facts touched →
refresh the device-risk snapshot → score with the current model.

---

## 4. The three pipeline modes

`scripts/run_batch.py` is the entrypoint for everything ETL-side.
It exposes three modes via `--mode`:

| Mode | When you use it | What it does |
|---|---|---|
| `bootstrap` | Once, on a fresh database | Creates `staging`/`warehouse`/`marts` schemas, full-refreshes the 5 dimensions, builds mart DDL and views. Idempotent — safe to re-run after schema changes. |
| `backfill --from YYYY-MM-DD` | Once, after bootstrap, on a stack that's never been loaded | Replays history in N-day chunks (defaults to 7), advancing the watermark each chunk. Handles the 54.7M archive rows / 7.4M trips. |
| `incremental` | Every 5 minutes (the `etl` container's default loop) | The production loop: load fact deltas since `etl_watermark.last_seen_id`, rebuild touched marts, refresh views, score, drift-check, retention. |

The order of operations inside `incremental_flow`
(`src/accent_fleet/pipeline/flow_batch.py`) is worth knowing because
it's exactly what you'd debug when "the dashboard is stale":

1. `task_refresh_dimensions` — pick up new devices/vehicles.
2. `task_load_fact` × N — `fact_trip`, `fact_overspeed`, `fact_stop`,
   `fact_harsh_event`, `fact_telemetry_daily`, `fact_notification`,
   `fact_maintenance`, `fact_maintenance_line`, `fact_fueling`, …
3. `task_recompute_mart` family — `mart_device_monthly_behavior`,
   `mart_device_monthly_telemetry`, `mart_fleet_daily`,
   `mart_vehicle_monthly`, `mart_tenant_monthly_summary`. Only the
   months/dates touched by step 2 are rebuilt; everything else is
   left alone.
4. `task_refresh_device_risk_profile` — write the current
   `v_device_risk_profile` snapshot into a real table the API can
   index.
5. `task_score_latest_partition` — push KMeans assignments for the
   newest year_month into `marts.fact_device_cluster_assignment`.
6. `task_detect_drift` — PSI per ML feature against the rolling
   reference window; updates `accent_ml_feature_drift_score{}` gauge.
7. `task_run_validation` — run `sql/99_validation_suite.sql`. Counts,
   nulls, freshness.
8. `task_apply_retention` — prune `etl_run_log`, `quarantine`,
   `fact_device_cluster_assignment` beyond the retention horizon
   (config `pipeline.yaml`).

If the dashboard is showing stale data, that's the order to scan: a
broken step N still lets steps 1..N-1 commit, so the downstream
artefact (a mart, a view, the cluster table) tells you where it stopped.

---

## 5. The services in `docker-compose.yml`

```
make up   ⇒   docker compose up -d mlflow api dashboard etl
```

Four long-running services boot by default. Three more sit behind
profile flags.

| Service | Role | What happens inside |
|---|---|---|
| `mlflow` | Model registry + artifact store | Tracking server v2.16.2. SQLite backend in a named volume. Models register under `device-behavior-clustering`. |
| `api` | FastAPI scoring service, port 8000 | Loads RiskScorer at startup (pure-Python). Lazy-loads ClusterPredictor on first call. AuthMiddleware enforces JWT in `enforce` mode (M4). |
| `dashboard` | Streamlit UI, port 8501 | Pages `Home → Executive → Operations → Maintenance → Risk → What-If`. Reads Postgres directly (with a service Principal so RLS lets the rows through). What-If hits the API for ad-hoc scoring. |
| `etl` | Prefect runtime | `while true: run incremental_flow; sleep`. Connects as `accent_etl` (BYPASSRLS) per M6. |
| `retrain-scheduler` | `--profile scheduler` | supercronic that fires every Monday 04:00 UTC; the wrapper short-circuits unless today is the *first* Monday of the month. Triggers `train_clustering.py`. |
| `nginx` | `--profile auth` | Reverse proxy with htpasswd at port 8080. v0.6.0 stopgap for "any auth at all"; the durable answer is the JWT model in §8. |
| `redpanda` | `--profile streaming` | Kafka-compatible broker for the deferred streaming work. Nothing consumes from it yet. |
| `postgres` | `--profile localdb` | Local Postgres 16 if you don't have an Azure DB to point at. |

The `base` image (`docker/base.Dockerfile`) is a build-only target that
the other three (`api`, `dashboard`, `etl`) FROM. Re-building base
once propagates dep upgrades to all four services.

> **Image-layering gotcha (real, hit during v0.9.0):** the `api` and
> `dashboard` Dockerfiles each overlay `COPY src ./src` on top of
> `accent-fleet-base:latest`. Without that overlay, slow rebuild
> cadences leave the base image's snapshot of `src/` stale, and you
> get the "page renders but every query returns zero rows" surprise
> because the RLS listener inside `src/accent_fleet/db/engine.py` is
> from before M5. See the commit message of
> `docker(dashboard): stamp service Principal so RLS doesn't clamp queries`.

---

## 6. The two consumer projects, side by side

### 6.1 Project 1 — Device behavior scoring (ML)

| | |
|---|---|
| **Grain** | One row per `(tenant_id, device_id, year_month)` |
| **Source mart** | `marts.mart_device_monthly_behavior` (+ telemetry mart) |
| **Source view** | `marts.v_ml_features_full` — frozen training contract |
| **Risk score** | Pure-Python formula in `src/accent_fleet/features/risk_score.py`. Driven by `config/feature_definitions.yaml` (weights are config, not code). |
| **Clusters** | KMeans on the same features. Trained by `scripts/train_clustering.py`, registered in MLflow, promoted on silhouette gate. |
| **API** | `POST /v1/score/risk` — pure-Python, no DB hit. `POST /v1/score/cluster` — uses the loaded MLflow model. |
| **Dashboard** | "Risk and Behavior" page reads `marts.v_fleet_risk_dashboard` + `marts.v_device_risk_profile` + `marts.fact_device_cluster_assignment`. |

The risk formula has seven components (overspeed rate, severity high,
severity extreme, high-speed-trip ratio, speed-alert rate, night-trip
ratio, max-speed). The `POST /v1/score/risk` response splits the
contribution per component so the What-If page can render the bar
chart you saw.

### 6.2 Project 2 — Fleet BI dashboard

| | |
|---|---|
| **Grain** | Three: day (`mart_fleet_daily`), vehicle-month (`mart_vehicle_monthly`), tenant-month (`mart_tenant_monthly_summary`) |
| **Views** | `v_executive_dashboard` (tenant-month, exec KPIs + MoM deltas + 3mo rolling), `v_operational_dashboard` (fleet-day, ops volume), `v_maintenance_dashboard` (tenant-month, cost + work order counts) |
| **Pages** | `dashboard/pages/0_Executive_Overview.py`, `1_Operations.py`, `2_Maintenance.py` |
| **Aggregation gotcha** | The views are per-(tenant, period). When the sidebar leaves "all tenants" empty, dashboard pages MUST aggregate across tenants before computing KPIs — otherwise a single-row pick picks one arbitrary tenant's slice. See the fix in `0_Executive_Overview.py` (groupby year_month + sum additive cols, recompute ratios from totals). |

---

## 7. CRISP-DM walkthrough — notebooks 00 → 06

Each `notebooks/0X_*/` folder corresponds to one CRISP-DM phase. The
notebooks are **the** authoritative tutorial — they teach the codebase
piece by piece, and every notebook's last cell asserts its exit
criterion so you know when you're done.

### Phase 0 — Setup (`notebooks/00_setup/`)

- `00_environment_check.ipynb` — checks Python version, that `.env` is
  populated, that Postgres is reachable. **Run first.**
- `01_create_schemas.ipynb` — DDL for `staging`/`warehouse`/`marts` +
  `etl_watermark` + `etl_run_log`.

### Phase 1 — Business understanding (in the parent project)

Not in this repo. The headline: fleet operators want to know which
devices are risky **enough** to act on, plus a BI surface for the
executive. The original "driver behavior" framing was rejected during
EDA (see REFACTOR_RATIONALE).

### Phase 2 — Data understanding (`notebooks/01_data_understanding/`)

- `01_staging_profile.ipynb` — row counts, distinct keys, time ranges
  across all staging tables. Surfaces "we have 7.4M trips" and
  "assignment covers 1.9% of devices".
- `02_cleaning_rules_exploration.ipynb` — proposes the **C1–C7
  cleaning rules** that the rule engine in
  `src/accent_fleet/cleaning/` then implements. Rules cover negative
  distances, future timestamps, zero-duration trips, …
- `03_eda_chapter3.ipynb` — feature distributions, correlations, the
  ones that survive Phase 3.

### Phase 3 — Data preparation (`notebooks/02_data_preparation/`)

The heart of the pipeline. Three subfolders:

- `dimensions/01_load_dimensions.ipynb` — backfills `dim_tenant`,
  `dim_vehicle`, `dim_device`, `dim_driver`, `dim_date`, `dim_hour_band`,
  `bridge_device_driver`.
- `cleaning/01_apply_cleaning_rules_preview.ipynb` — applies C1–C7
  against staging, shows what would be dropped/repaired.
- `facts/01..08_*.ipynb` — one notebook per fact backfill. Trip,
  overspeed, stop, harsh-event, telemetry-daily, notification,
  maintenance, fueling. Each shows the SQL template and the row-count
  delta.
- `marts/01..04_*.ipynb` — build marts + views. The `04_build_unified_ml_view`
  notebook is the **frozen ML contract** — anything the model trains
  on, this view has to expose.
- `validation/01_run_validation_suite.ipynb` — the
  `sql/99_validation_suite.sql` smoke pass. Counts, nulls, freshness.

### Phase 3.5 — Feature engineering (`notebooks/03_feature_engineering/`)

Bridges Phase 3 to Phase 4. By M8e you have `X.parquet`, `y.parquet`,
and `preprocess.joblib` ready for modelling.

- `01_explore_ml_features.ipynb` — EDA on the ML view.
- `02_handle_missing_values.ipynb`, `03_handle_outliers.ipynb` —
  preprocessing decisions.
- `04_data_encoding.ipynb` — OHE / Ordinal / target-guided.
- `05_handle_imbalanced_dataset.ipynb` — SMOTE.
- `06_build_ml_dataset.ipynb` — **the exit gate**. Produces the parquet
  + joblib bundle the next phase trains on.

### Phase 4 — Modeling (`notebooks/04_modeling/`)

KMeans clustering on the device-month features. `scripts/train_clustering.py`
is the production version of this notebook: load → fit → register in
MLflow → optionally promote.

### Phase 5 — Evaluation (`notebooks/05_evaluation/`)

Cluster quality (silhouette per K), feature importance proxies,
qualitative cluster narratives ("cluster 2 = night drivers with
moderate overspeed"). Anomaly/risk score evaluation lives here too.

### Phase 6 — Deployment (`notebooks/06_deployment/`)

- `01_incremental_mode_demo.ipynb` — runs `incremental` once and
  shows the watermark advancing.
- `02_scheduled_runs.md` — the cron recipe for the Azure VM
  (superseded by the `etl` container's internal loop in v0.6.0+, but
  useful if you ever take Prefect out of the picture).

---

## 8. Auth + multi-tenancy (v0.9.0)

Two layers, doing different jobs.

### 8.1 JWT bearer auth (M1–M4)

- `auth.users` is a real table with argon2id password hashes, seeded
  by `scripts/seed_auth.py`. One `tenant_admin` per active tenant,
  one global `superadmin`.
- `POST /v1/auth/login` exchanges email + password for an `access_token`
  (15 min TTL) + `refresh_token` (7 days, opaque UUID, server-side row).
- Every non-exempt request hits `AuthMiddleware` (`app/auth/middleware.py`),
  which extracts the bearer token, verifies it, and stashes a
  `Principal` in a `ContextVar`.
- Three modes: `off` / `advisory` / `enforce`. Production is `enforce`
  (a `401` is returned for missing/invalid tokens). Advisory mode is
  the soft-launch state: logs + lets the request through.

### 8.2 Row-level security (M5–M6, with FORCE in v0.9.0 post-B7)

The Postgres side enforces tenant isolation **even if the API code is
buggy**. Three roles + a per-transaction GUC:

| Role | BYPASSRLS | SUPERUSER | Who uses it |
|---|---|---|---|
| `accent_app` | no | no | The API. Has NO bypass — every query is RLS-clamped to `app.current_tenant`. |
| `accent_etl` | **yes** | no | The ETL container. Needs to see all tenants. |
| `accent_superadmin` | **yes** | yes | Migrations, ops tooling, and (with role-swap) ad-hoc dashboard queries. |

The flow on every API request:

1. `AuthMiddleware` extracts the bearer → sets `Principal`
   (`user_id`, `tenant_id`, `role`).
2. A SQLAlchemy `begin` event listener in
   `src/accent_fleet/db/engine.py:_set_tenant_guc` reads the Principal:
   - `tenant_user/admin` → `SET LOCAL app.current_tenant = <id>` on
     the transaction. Policies in `sql/51_rls_policies.sql` filter
     rows by it.
   - `superadmin` → `SET LOCAL ROLE accent_superadmin` for that
     transaction. The role has `BYPASSRLS`, so all tenants are visible.
     Reverts at COMMIT.
3. After v0.9.0 post-B7, every RLS-enabled table is `FORCE ROW LEVEL
   SECURITY` (see `sql/55_force_rls.sql`). That removes the owner
   exemption — even the table owner sees zero rows unless they
   elevate.

**The dashboard's twist:** there is no per-user login. So
`dashboard/lib/theme.py:_ensure_dashboard_principal` stamps a service
"superadmin" Principal into the ContextVar at page init, the engine
listener emits `SET LOCAL ROLE accent_superadmin`, and queries see
all tenants for the duration of one transaction. The What-If page is
the exception — it hits the API and needs a real JWT
(`dashboard/lib/api.py:post_json` handles the login + bearer
injection).

> **Operator recovery — lost seed password.** `seed_auth.py` prints
> generated passwords once and stores only the argon2id hash. If you
> lose them, `scripts/reset_password.py --email <e> --password <p>`
> writes a fresh hash directly to `auth.users`. Use `--allow-weak`
> for dev (refuses <12 chars otherwise). The admin route
> `POST /v1/admin/users/{id}/reset-password` exists but is itself
> bearer-auth-gated, so it doesn't help when the lost credential is
> the *superadmin's*.

---

## 9. The day-to-day operator's guide

### 9.1 Bring up a fresh stack

```bash
cp .env.example .env                                # fill DB creds + DASHBOARD_API_PASSWORD
docker compose build base                           # one-time shared image
make build                                          # api + dashboard + etl
make up                                             # mlflow + api + dashboard + etl
make seed                                           # bootstrap + small backfill

# Seed auth (M2). CAPTURE THE OUTPUT — passwords are printed once.
docker compose exec api python scripts/seed_auth.py
```

Then visit:
- Dashboard: <http://localhost:8501>
- API docs:  <http://localhost:8000/docs>
- MLflow:    <http://localhost:5000>

### 9.2 Train + register a model

```bash
make train
# or, with full control:
docker compose run --rm etl python scripts/train_clustering.py
```

The script picks K via silhouette, fits, registers in MLflow under
`device-behavior-clustering`, and respects the silhouette gate when
deciding whether to promote to Production. The `retrain-scheduler`
service does this on the first Monday of each month if you enable
`--profile scheduler`.

After a manual promotion, the API needs to reload its in-memory model:

```bash
curl -X POST http://localhost:8000/v1/admin/reload-model \
     -H "X-API-Key: $API_ADMIN_KEY"
```

### 9.3 Backfill / re-run / debug

```bash
# Run the incremental flow manually (the etl container does this every 5min):
docker compose run --rm etl python scripts/run_batch.py --mode incremental

# Replay history in 30-day chunks:
docker compose run --rm etl python scripts/run_batch.py --mode backfill \
                                                       --from 2024-01-01 \
                                                       --chunk-days 30
```

### 9.4 Verify the RLS / role separation is healthy

```bash
sudo -u postgres psql -d accent_fleet -f sql/verify_force_rls.sql
```

Expected outcome printed in the COUNT column:

```
baseline                  : N rows
accent_etl_bypassrls      : N    (BYPASSRLS beats FORCE)
accent_superadmin_bypass  : N    (BYPASSRLS beats FORCE)
accent_app_no_guc         : 0    (NOBYPASSRLS, missing GUC ⇒ policy NULL)
accent_app_with_guc       : <tenant rows>
medamine_dev_force_clamp  : 0    (THE proof FORCE actually works)
medamine_dev_elevated     : N    (escape hatch via accent_superadmin)
```

### 9.5 Common surfaces you'll touch

| Concern | File |
|---|---|
| Add a new fact table | `sql/1X_fact_*_incremental.sql` + a notebook in `notebooks/02_data_preparation/facts/` + register in the flow's task list |
| Add a new dashboard page | `dashboard/pages/N_<name>.py` — copy a sibling and adapt. Don't forget `apply_layout()` + `render_sidebar_filters()` |
| Add a new API endpoint | `app/routes/<file>.py` — add to `app/main.py` via `include_versioned_router` so it gets `/v1` + legacy paths |
| Add a new cleaning rule | `config/cleaning_rules.yaml` + `src/accent_fleet/cleaning/` + a unit test in `tests/test_cleaning_rules.py` |
| Change a risk weight | `config/feature_definitions.yaml` — no code change needed |
| Reset a forgotten password | `scripts/reset_password.py` (allow `--allow-weak` for dev) |
| Promote a model out-of-band | `docker compose exec etl python scripts/train_clustering.py --force-promote` then `POST /v1/admin/reload-model` |

---

## 10. When things go wrong — recent real bugs

A few real failure modes you (or future-you) will hit again, with the
exact symptom and the exact fix.

### 10.1 Dashboard shows "No data for the current filters" on every page

**Symptom.** Every dashboard page is empty even though `psql` as
`postgres` shows the marts are populated.

**Cause.** Post-B7 + FORCE RLS the dashboard's queries land as
`accent_app` (NOBYPASSRLS) with no `app.current_tenant` GUC set, so
the policies in `sql/51_rls_policies.sql` evaluate to NULL ⇒ zero
rows.

**Fix.** Ensure `dashboard/lib/theme.py:_ensure_dashboard_principal`
stamps a superadmin Principal **before** any `read_sql()` call. The
engine listener then emits `SET LOCAL ROLE accent_superadmin` per
transaction. Requires `app/` in the dashboard image AND the
post-M5 `src/accent_fleet/db/engine.py` (so the listener's superadmin
branch is present). Both are now baked in
`docker/dashboard.Dockerfile` via `COPY app ./app` + `COPY src ./src`.

### 10.2 What-If page: "401 Unauthorized" on Score

**Symptom.** Score button → "Risk API call failed: Client error
'401 Unauthorized'".

**Cause.** Post-M3 the API enforces JWT bearer auth on every
non-exempt path. The What-If page was hitting `/score/risk` with no
`Authorization` header.

**Fix.** Use `dashboard/lib/api.post_json("/v1/score/risk", payload)`.
The helper lazy-logs-in, caches the access token across Streamlit
reruns (`@st.cache_resource`), refreshes 30s before expiry, and
retries once on a server-returned 401. Credentials come from
`DASHBOARD_API_EMAIL` / `DASHBOARD_API_PASSWORD` in the dashboard
container env.

### 10.3 KPI says "all tenants = 46 devices, tenant 235 alone = 126"

**Symptom.** Aggregate filter shows a smaller number than a single
tenant in the same filter.

**Cause.** `df.iloc[-1]` picked one arbitrary row from a multi-tenant
dataframe instead of summing across tenants for the latest month.

**Fix.** Aggregate per month before computing KPIs:

```python
monthly = (df.groupby("year_month", as_index=False)[ADDITIVE_COLS]
             .sum().sort_values("year_month"))
# Ratios must be recomputed from totals, NOT summed:
monthly["cost_per_km"] = (monthly["total_operating_cost"]
                          / monthly["total_distance_km"])
latest = monthly.iloc[-1]
```

Pattern lives in `dashboard/pages/0_Executive_Overview.py`. The other
pages (`1_Operations.py`, `2_Maintenance.py`) already use `.sum()`;
`3_Risk_and_Behavior.py` uses `value_counts`/`nunique`. Audit before
copy-pasting the iloc pattern anywhere.

### 10.4 Stale base image: "the listener doesn't fire even with a Principal"

**Symptom.** You stamp a Principal correctly, but the engine listener
still no-ops and queries return zero rows.

**Cause.** `accent-fleet-base:latest` baked an older snapshot of
`src/accent_fleet/db/engine.py` — pre-M5, without the superadmin
role-swap branch.

**Fix.** The api/dashboard Dockerfiles both `COPY src ./src` on top of
the base image. Rebuild with `--no-cache` if you suspect cached
layers are involved. Verify inside the container:

```bash
docker compose exec dashboard grep -n accent_superadmin /app/src/accent_fleet/db/engine.py
```

Should show ~6 lines including `conn.exec_driver_sql("SET LOCAL ROLE accent_superadmin")`.

---

## 11. Glossary

- **Argon2id** — the password hashing algorithm seed_auth.py + the API
  use. OWASP-recommended params (`time=3, memory=64MB, parallelism=4`)
  hard-coded in `scripts/seed_auth.py`.
- **BYPASSRLS** — a Postgres role attribute that exempts that role
  from row-level-security policies. `accent_etl` and
  `accent_superadmin` have it; `accent_app` deliberately does not.
- **CRISP-DM** — Cross-Industry Standard Process for Data Mining. Six
  phases: business understanding, data understanding, data
  preparation, modeling, evaluation, deployment. Our `notebooks/`
  layout follows this.
- **`device-month`** — the unit of analysis. One row per
  `(tenant_id, device_id, year_month)`. The output of `marts.mart_device_monthly_behavior`.
- **FORCE ROW LEVEL SECURITY** — Postgres knob that removes the owner
  exemption from RLS, so even the table owner sees the same row
  filter as everyone else (unless they have BYPASSRLS). Applied in
  `sql/55_force_rls.sql` post-B7.
- **GUC** — Grand Unified Configuration. Postgres's term for a
  per-session/per-transaction setting. We use
  `app.current_tenant` as a GUC the RLS policies read.
- **Mart** — a query-optimized rollup table. `marts.*` schema. Built
  from `warehouse.*` facts. One grain per mart.
- **MLflow Production** — a stage label in the MLflow model registry.
  `ClusterPredictor` loads the version with that label. The
  retrain-scheduler promotes only when the silhouette gate passes.
- **Principal** — the auth identity for one request. Dataclass in
  `app/auth/principal.py`: `user_id`, `tenant_id`, `role`, `email`.
  Lives in a `ContextVar` so deep code (the SQLAlchemy listener) can
  read it without a function arg.
- **Prefect** — workflow framework we use as the ETL runtime. Each
  step is a `@task`; the orchestrator is a `@flow`.
- **PSI** — Population Stability Index. Drift metric we compute per
  feature in `task_detect_drift`. Threshold 0.25 raises a warning.
- **RLS** — Row-Level Security. Postgres feature: per-row filters
  attached to a table, evaluated against `current_role` and `SET
  LOCAL`-style GUCs.
- **Silhouette score** — clustering quality metric. KMeans training
  picks K to maximize it. The retrain gate refuses to promote a
  candidate that scores worse than the current production version.
- **Watermark** — the marker in `warehouse.etl_watermark` that tells
  the incremental flow "I've already loaded everything up to here."
  Each fact has its own watermark row keyed by `(source_table,
  cursor_name)`.
- **`year_month`** — `CHAR(7)`, e.g. `"2026-03"`. The month grain used
  across marts. Cheap to filter (`BETWEEN '2026-01' AND '2026-03'`)
  and survives timezone changes better than a date column.

---

## 12. What to read next

- **`README.md` §"Pipeline modes"** — the table that maps the three
  modes to what they touch.
- **`docs/auth_design.md`** — full threat model, data model, and
  rejected-alternatives appendix for the v0.9.0 auth + RLS work.
- **`REFACTOR_RATIONALE.md`** — why the v1 SQL-only pipeline was
  replaced with this incremental Python + SQL design.
- **`notebooks/00_setup/00_environment_check.ipynb`** — the canonical
  starting point for someone running the stack for the first time.
- **`tests/`** — the test suite is the most up-to-date specification
  of how each component is expected to behave. When this document
  drifts, the tests are right and the document is stale.
