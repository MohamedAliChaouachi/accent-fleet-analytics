# Hardening Sprint Summary — Part 1 of NEXT_STEPS.md

**Date range:** session up to 2026-05-12
**Branch:** `main`
**Goal:** Take v0.5.0 from "works on our two machines" to "safe to give to a
third person without supervision". Implements §1.1–§1.5 of `NEXT_STEPS.md`.

---

## Commits

| SHA | Title |
|---|---|
| `b66ea575` | hardening: API + dashboard fixes, admin auth, parity/regression tests |
| `d3163f76` | ops: retention, MLflow backup, nginx basic-auth, CI, hand-off docs |
| `2298ac81` | ci: make ruff pass on the existing codebase |
| `d7cde3dc` | ci: deselect integration tests in CI pytest run |
| `212837d5` | test: override get_db dep in validation-only API tests |

CI on `main` is green (lint + test, both jobs).

---

## What landed

### 1.1 Real bugs

- **`GET /devices/top-risk` 500 without `tenant_id`** — psycopg sends a
  typeless NULL for `None`, and Postgres can't infer the parameter type in
  `(:tenant_id IS NULL OR tenant_id = :tenant_id)`, raising
  `AmbiguousParameter`. Replaced with two branched SQL strings, one with the
  filter and one without. `app/routes/devices.py`.

### 1.2 Test coverage

- **`tests/test_api_devices.py`** (new) — regression test for the typeless-NULL
  bug above, plus `/devices/{id}/profile` shape + 404 + query-param bounds
  validation. DB-touching tests self-skip via a `db_available` module fixture.
- **`tests/test_dashboard_smoke.py`** (new) — two-tier check:
  - Static: every page parses, every page imports `dashboard.lib.*`
    cleanly, every page uses `apply_layout(...)`.
  - Live (skipped unless `STREAMLIT_BASE_URL` is set): hits
    `/_stcore/health` and each page URL.
- **`tests/test_feature_computation.py`** — added two SQL-parity tests:
  - `test_python_risk_score_matches_sql_view` proves
    `RiskScorer.score()` is within ±1 of `marts.v_device_risk_profile`
    for up to 50 sampled rolling rows.
  - `test_cluster_predictor_matches_persisted_assignment` proves
    `ClusterPredictor.predict()` reproduces the persisted
    `fact_device_cluster_assignment.cluster_id` for the currently loaded
    model version.
- **`tests/test_inference.py`** — fixed
  `test_cluster_predictor_raises_when_no_model` using `monkeypatch`.

### 1.3 CI

- **`.github/workflows/ci.yml`** (new) — two jobs, lint → test:
  - `lint` runs `ruff check src app dashboard tests scripts` on Python 3.11.
  - `test` installs the package + dev deps and runs `pytest -ra --maxfail=5 -m "not integration"`.
  - `concurrency: cancel-in-progress` so older runs on the same ref are
    cancelled when a new commit lands.
- **`pyproject.toml`** — tightened ruff config to make the existing
  codebase pass:
  - Selected rule families: `E, F, W, I, N, UP, B, A, C4, SIM`.
  - Per-file ignores for Streamlit page-naming convention
    (`dashboard/Home.py`, `dashboard/pages/*.py` — `N999, E501`) and the
    sklearn `X`/`y` ML convention (`src/accent_fleet/ml/*.py` — `N806`).
  - Excluded one-off figure generators that intentionally use one-liner
    matplotlib style (`scripts/python/figures`, `scripts/build_eval_notebooks.py`).

### 1.4 API operability

- **`POST /admin/reload-model`** — `app/routes/admin.py` (new),
  X-API-Key-gated (`API_ADMIN_KEY` env var, fail-closed when unset).
  Calls `app.state.cluster_predictor.reload()` so a freshly-promoted
  Production model is picked up without restarting the container.
- **`ClusterPredictor.reload()`** — `src/accent_fleet/ml/inference.py`. Re-resolves
  the latest model version from MLflow and swaps it in atomically.
- **Dashboard freshness footer** — `dashboard/lib/theme.py`. Sidebar now
  shows "Last ETL refresh: {timestamp}" pulled from `warehouse.etl_run_log`.
- **`Settings.api_admin_key`** — `src/accent_fleet/config.py`. New field,
  empty by default → admin router refuses every request unless set.

### 1.5 Operational hygiene

- **Retention policy** — `sql/40_retention.sql` (new) plus
  `task_apply_retention` wired into the incremental flow in
  `src/accent_fleet/pipeline/flow_batch.py`. Purges
  `warehouse.etl_run_log` and `marts.fact_device_cluster_assignment` rows
  older than the configured horizon. Knobs in `config/pipeline.yaml` →
  `retention:` section.
- **MLflow backup** — `docker/scripts/backup_mlflow.sh` (new). Single-shot
  online backup of the MLflow SQLite + artifacts volume. Wire into host
  cron.
- **nginx basic-auth profile** — `docker/nginx/nginx.conf` (new) +
  service in `docker-compose.yml` under `--profile auth`. Anyone hitting
  `:8501` or `:8000` on the LAN gets prompted.
- **`.env` hand-off process** — README "Deployment" section documents how
  the `.env` and the `API_ADMIN_KEY` are passed between operators.
- **`.env.example`** — `API_ADMIN_KEY` documented.

---

## CI debugging trail (this session)

After pushing the hardening commits, CI initially red-flagged:

1. **Ruff lint failed with 133 errors** on the existing brownfield codebase.
   Fix: auto-fix 39 (imports, deprecated typing, `B007`), per-file
   ignores for legitimate framework conventions (Streamlit page names,
   sklearn `X`/`y`), exclude one-off figure generators, wrap four
   over-long lines, restore a `numpy` import that ruff auto-fix
   over-pruned in `tests/test_batch_scoring.py`, and add `strict=True`
   to a `zip()` in `train_clustering.py`. Commit `2298ac81`.

2. **Pytest failed with a misleading Prefect shutdown traceback.** The
   real failures, buried above the cleanup noise, were
   `test_top_risk_validates_n_bounds` and
   `test_device_profile_validates_months` connecting to Postgres even
   though their docstrings claim "no DB needed".

   **Root cause:** FastAPI's `solve_dependencies` resolves every
   `Depends()` in the same pass that validates `Query()` params. So
   `get_db()` opened a real connection before the 422 short-circuit
   could trigger — fine on a dev box with Postgres up, hard failure in
   CI where nothing is listening on `:5432`. Other DB-touching tests
   self-skipped via `db_available`, but these two tried to bypass the
   DB entirely on the (incorrect) assumption that FastAPI would
   short-circuit on validation first.

   **Fix:** New `client_no_db` fixture in `tests/test_api_devices.py`
   that installs `app.dependency_overrides[get_db]` with a no-op
   generator for the duration of the test, then tears it down. The
   overridden value is never read because validation still fails first.
   Verified locally with `PG_HOST=127.0.0.1 PG_PORT=1`: 62 passed, 14
   skipped, 2 deselected, exit 0 — matching CI conditions. Commit
   `212837d5`.

3. **`test_incremental_semantics.py` was running in CI** despite being
   marked `@pytest.mark.integration`, because the CI invocation didn't
   filter by mark. Boots a real Prefect flow, which in turn tries to
   reach Postgres and fails. Fix: CI now runs
   `pytest -ra --maxfail=5 -m "not integration"`. Commit `d7cde3dc`.

---

## Definition of Done — status

From `NEXT_STEPS.md` §"Definition of Done — Part 1":

| Criterion | Status |
|---|---|
| `pytest` runs in CI on every PR, green before merge | ✅ done (`ci.yml`) |
| `docker compose build base` runs in CI to catch Dockerfile breakage | ✅ done (`docker` job in `ci.yml`, commit `52e726db`) |
| Anyone on the LAN hitting `:8501` or `:8000` gets prompted for credentials | ✅ done (`--profile auth`) |
| p95 latency benchmarked and documented in README | ✅ done (`scripts/bench_api.py` + README "Performance" section, commit `52e726db`) |
| Branch protection on `main` requires CI green before merge | ⏭️ **declined** — see "Out of scope" below |
| New developer can `make up` + `make seed` and have a working dashboard in <10 min | ⚠️ not re-verified this sprint |
| p95 latency for `/score/risk` (<100ms) and `/devices/{id}/profile` (<300ms) documented in README | ✅ done (commit `52e726db`) |
| Tag `v0.6.0` | ✅ done (this sprint) |

### Branch protection — out of scope

`NEXT_STEPS.md` §1.3 listed branch protection on `main` (require CI green
before merge) as a 5-minute GitHub-UI step. **Declined by repo owner**
during this sprint — direct pushes to `main` remain allowed. CI still
runs on every push, but a red CI does not block a merge.

If revisited later, the click path is:

1. `https://github.com/MohamedAliChaouachi/accent-fleet-analytics/settings/branches`
2. Add ruleset → target `main` → require status checks
   `lint (ruff)`, `test (pytest)`, `docker (build base image)`.

### Performance findings (commit `52e726db`)

The bench (`scripts/bench_api.py`) measured:

- **`POST /score/risk`** — 3.8 ms p95 single-shot, 54.6 ms p95 at concurrency 16. **Under target.**
- **`GET /devices/{id}/profile`** — 499.7 ms p95 single-shot, 1692.7 ms p95 at concurrency 16. **OVER the 300 ms target** even at concurrency 1.

The profile endpoint runs two SQL hits per request. The bottleneck is
`marts.v_device_risk_profile`: a view that does a window-rank over
`mart_device_monthly_behavior` and is then filtered by `device_id`,
which forces a rescan on every call. Two fix options (Tier-1 follow-up,
not Part 2):

1. Materialize the view as a refreshed-by-flow table.
2. Add a partial index on `(device_id)` and rewrite the view to push
   the predicate.

The bench script makes this trivially regression-testable once fixed.

---

## What this unblocks

Part 1 done = Part 2 (`NEXT_STEPS.md` §2.x) becomes tractable:

- Streaming activation (§2.1)
- Real auth + multi-tenancy / RLS (§2.2)
- Production frontend, deprecate Streamlit (§2.3)
- Cloud deployment, AKS/Terraform (§2.4)
- ML platform maturity — drift detection, retraining cadence, A/B (§2.5)
- Geo / fleet map (§2.6)

None of these should start until the five "still open" items above are
closed and `v0.6.0` is tagged.
