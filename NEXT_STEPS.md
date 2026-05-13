# Accent Fleet Analytics — Post v0.5.0 Roadmap

> Status as of 2026-05-13: v0.8.0 is tagged. Foundation week (v0.7.0)
> shipped structured logging + `/metrics`, `/v1` API versioning with a
> deprecation-headed legacy mount, a `pg_dump`-based backup script, the
> `/devices/{id}/profile` perf fix, and feature-level KL-divergence
> drift detection. ML maturity (v0.8.0) shipped silhouette-gated
> retraining with a frozen-dataclass audit trail, plus a compose-managed
> `retrain-scheduler` service (supercronic) that fires the monthly
> retrain on the first Monday of each month. Everything below is what
> is still **not yet** in the platform.

The remaining work splits cleanly into two halves:

- **Part 1 — Hardening sprint.** Tactical, sprint-sized. Closes the real gaps
  in what we already shipped. Unblocks the platform going beyond the two-person
  hand-off.
- **Part 2 — Path to v1.0.** The phases the original plan explicitly deferred:
  streaming, auth, cloud deploy, multi-tenant UI. Each is a multi-week effort
  on its own.

Do Part 1 first. Part 2 only makes sense once Part 1 is solid.

---

## Part 1 — Hardening sprint (~1 week)

Goal: take the stack from "works on our two machines" to "safe to give to a
third person without supervision". No new features — close the real gaps in
what we already have.

### 1.1 Real bugs

| Item | Where | Effort |
|---|---|---|
| `/devices/top-risk` 500s without `tenant_id` (untyped NULL parameter) | `app/routes/devices.py:~95` | 30 min |
| `streamlit-aggrid` declared in `requirements.txt` but never imported | `requirements.txt`, dashboard pages | 15 min — either remove the dep or wire it into the risk page table |
| MLflow client/server pinned — bump server to match? | `docker-compose.yml`, `requirements.txt` | 1 hr — optional; current pin is stable |

### 1.2 Test coverage gaps from the original plan

| Item | Where | Effort |
|---|---|---|
| `tests/test_api_devices.py` — covers `/devices/top-risk` (with and without tenant_id) and `/devices/{id}/profile` | new file | 2 hr |
| `tests/test_dashboard_smoke.py` — headless Streamlit + `curl /_stcore/health` + GET each page URL, assert non-500 | new file | 3 hr |
| SQL parity assertion — extend `tests/test_feature_computation.py` so `cluster_id` from API equals `cluster_id` in `marts.fact_device_cluster_assignment` | existing test file | 2 hr |
| Performance check — measure p95 for `/score/risk` (target <100ms) and `/devices/{id}/profile` (target <300ms). Document numbers in README. | one-off `locust` or `ab` run | 2 hr |

### 1.3 CI

| Item | Where | Effort |
|---|---|---|
| GitHub Actions workflow that runs `pytest -m "not slow"` on every push to a branch and on PR to main | `.github/workflows/ci.yml` (new) | 2 hr |
| Same workflow runs `docker compose build base` to catch Dockerfile breakage | same | 30 min |
| Branch protection on `main` requiring CI green before merge | GitHub settings | 5 min |

### 1.4 API operability

| Item | Where | Effort |
|---|---|---|
| `POST /admin/reload-model` endpoint — calls `app.state.cluster_predictor.reload()` so a freshly-promoted Production model is picked up without restarting the container | `app/routes/admin.py` (new), `app/main.py` lifespan | 3 hr |
| Dashboard footer: "Last ETL refresh: {timestamp}" pulled from `warehouse.etl_run_log` — already a documented risk mitigation in the original plan | `dashboard/Home.py`, `dashboard/lib/queries.py` | 1 hr |

### 1.5 Minimum operational hygiene

| Item | Where | Effort |
|---|---|---|
| Document `.env` hand-off process (1Password? Encrypted file? In-person only?) — pick one and write it down | `README.md` "Deployment" section | 30 min |
| Retention policy on `warehouse.etl_run_log` and `marts.fact_device_cluster_assignment` — purge rows older than N months in a daily Prefect task | `sql/`, `src/accent_fleet/pipeline/` | 4 hr |
| Backup the MLflow SQLite + artifacts volume — even a nightly `docker run --rm -v mlflow_data:/data alpine tar czf` is enough | `docker/scripts/backup_mlflow.sh` (new), cron on host | 2 hr |
| Auth — stick API + dashboard behind a single nginx with HTTP basic auth, or add an API key middleware. Pick one. Even bad auth is better than no auth. | `docker/nginx/`, `docker-compose.yml` | 1 day |

### Definition of Done — Part 1

- `pytest` runs in CI on every PR, green before merge.
- A new developer can clone the repo, run `make up` + `make seed`, and have a
  working dashboard in under 10 minutes (verifying the original plan's
  Definition of Done that we never actually verified).
- Anyone on the LAN hitting `:8501` or `:8000` gets prompted for credentials.
- p95 latency numbers for `/score/risk` and `/devices/{id}/profile` are
  documented in the README.
- Tag `v0.6.0` when all of 1.1–1.5 are done.

### Estimated effort
~5 working days, one developer.

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

### 2.2 Real auth + multi-tenancy

Part 1.5 lands "any auth at all". This phase is "auth that scales":

- SSO via OIDC (Azure AD is the natural choice given the Azure Postgres host).
- Row-level security: a user logged in for tenant X should not see tenant Y
  data even if they craft the SQL. Postgres RLS policies on `marts.*` views.
- Per-tenant dashboard URL (`/t/{tenant}/...`) and a `tenant_id` claim in every
  API token, enforced in `app/deps.py`.
- Admin panel for managing tenants + users.

Estimated: 4–6 weeks. The hard part is RLS migration on existing data.

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

- **Part 1 is gating.** Don't start Part 2 phases until 1.1–1.5 are done — you
  will end up debugging missing tests and missing auth in the middle of a
  Kubernetes migration, and that is a bad time.
- Inside Part 2, the order is loose, but `2.2 (auth) → 2.3 (frontend) → 2.4
  (cloud)` is the cleanest sequence. Streaming (2.1) can run in parallel with
  any of them. ML maturity (2.5) and geo (2.6) are independent.
- A real-world calendar: Part 1 = 1 week, then ~4 months to reach a usable
  v1.0 (auth + new frontend + cloud deploy), then ongoing iteration on
  streaming + ML.

## Out of scope for this document

- Vendor/build decisions inside Part 2 (Next.js vs Remix, AKS vs ACA, etc.) —
  those get their own RFCs when each phase starts.
- Cost modeling — depends on cloud choice in 2.4.
- Team / hiring — also a function of where 2.2–2.4 land.
