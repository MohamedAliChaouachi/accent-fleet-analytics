# Smoke Test Runbook — Accent Fleet Analytics v0.5.0 (Sprints 1 + 2)

**Audience:** developer or QA validating that the work pushed to `main` actually runs.
**Estimated time:** 30–45 minutes on a warm laptop, longer on first clone.
**Goal:** prove the full stack works end-to-end — API, dashboard, ETL, batch scoring, validation suite.

If every section ends with **PASS**, Sprints 1 and 2 are functionally done and the `v0.5.0` tag can be cut.

---

## 0. Prerequisites

| Requirement | How to check |
|---|---|
| Windows 10/11 with PowerShell 5.1+ (or any Unix shell — commands adapt) | `$PSVersionTable.PSVersion` |
| Python 3.11+ | `python --version` |
| Git | `git --version` |
| Docker Desktop (optional, only for local-PG mode) | `docker --version` |
| Access to the Azure Postgres at `104.214.222.52:5432` **or** ability to run Docker locally | see Step 1.3 |
| Ports `8000` (API), `8501` (dashboard), `5000` (MLflow) free on the host | `netstat -ano | findstr ":8000 :8501 :5000"` |

---

## 1. Setup

### 1.1 Clone and check out

```powershell
git clone https://github.com/MohamedAliChaouachi/accent-fleet-analytics.git
cd accent-fleet-analytics
git log --oneline -5
```

**Expected:** the latest commit on `main` includes the Sprint 2 merge (look for "sprint 2" or `fact_device_cluster_assignment` in commit messages).

### 1.2 Create venv and install

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e . --quiet
pip install -r requirements.txt --quiet
```

**Expected:** no errors. Last line is silent (`--quiet`).

### 1.3 Configure the database connection

Copy `.env.example` to `.env` and fill in **one** of these two paths:

**Path A — Azure (preferred, validates against real data):**
```ini
PG_HOST=104.214.222.52
PG_PORT=5432
PG_DATABASE=accent_data
PG_USER=<your_user>
PG_PASSWORD=<your_password>
PG_SSLMODE=require
API_BASE_URL=http://127.0.0.1:8000
MLFLOW_TRACKING_URI=file:./mlflow
```

> If using Azure, your DB user must have the grants listed in `DB_ACCESS_REQUEST.md`. Verify in Step 2.

**Path B — Local Postgres via Docker (no Azure access needed):**
```powershell
docker compose up -d postgres
```
```ini
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=accent_data
PG_USER=accent
PG_PASSWORD=accent_local
PG_SSLMODE=disable
API_BASE_URL=http://127.0.0.1:8000
MLFLOW_TRACKING_URI=file:./mlflow
```

> Local mode is faster but starts with an empty DB — you'll need to backfill in Step 3.

**Status:** ☐ PASS ☐ FAIL

---

## 2. Unit tests + DB connectivity

### 2.1 Run the full pytest suite

```powershell
pytest -q
```

**Expected:**
```
.................                                                  [100%]
19 passed in <time>s
```

If anything is red, stop and fix before continuing — every later step assumes the unit suite is green.

### 2.2 Verify the configured role can reach the DB

```powershell
@'
from accent_fleet.db.engine import engine
from sqlalchemy import text
c = engine().connect()
for q in [
    "SELECT current_user, current_database(), version()",
    "SELECT COUNT(*) FROM marts.mart_device_monthly_behavior",
    "SELECT COUNT(*) FROM marts.v_ml_features_full",
    "SELECT to_regclass('marts.fact_device_cluster_assignment')",
]:
    print(q, "->", c.execute(text(q)).scalar())
c.close()
'@ | python -
```

**Expected (Azure, before Step 3 has run):**
```
SELECT current_user ... -> medamine_dev
SELECT COUNT(*) FROM marts.mart_device_monthly_behavior -> 24413  (or similar)
SELECT COUNT(*) FROM marts.v_ml_features_full -> <positive integer>
SELECT to_regclass('marts.fact_device_cluster_assignment') -> None
```

`None` on the last line is correct here — that table doesn't exist yet, Step 3 will create it.

**Common failures:**
- `permission denied for view v_ml_features_full` → DB grants missing. See `DB_ACCESS_REQUEST.md`.
- `connection refused` / `password authentication failed` → `.env` not loaded or wrong creds. Check `python -c "from accent_fleet.config import settings; print(settings.pg_host, settings.pg_user)"`.

**Status:** ☐ PASS ☐ FAIL

---

## 3. Sprint 2 — Apply new DDL

```powershell
python scripts\run_batch.py bootstrap
```

**Expected:** the script enumerates `sql/*.sql` files in order, prints each one applied, and ends without errors. Specifically look for these lines:
```
applied sql/27_fact_device_cluster_assignment.sql
applied sql/99_validation_suite.sql
```

### 3.1 Verify the new fact table exists

```powershell
@'
from accent_fleet.db.engine import engine
from sqlalchemy import text
c = engine().connect()
print("table:", c.execute(text("SELECT to_regclass('marts.fact_device_cluster_assignment')")).scalar())
for idx in [
    "idx_fact_cluster_assignment_cluster",
    "idx_fact_cluster_assignment_tenant_month",
    "idx_fact_cluster_assignment_run",
]:
    found = c.execute(text("SELECT to_regclass(:n)"), {"n": "marts." + idx}).scalar()
    print(f"index {idx}:", found)
c.close()
'@ | python -
```

**Expected:**
```
table: marts.fact_device_cluster_assignment
index idx_fact_cluster_assignment_cluster: marts.idx_fact_cluster_assignment_cluster
index idx_fact_cluster_assignment_tenant_month: marts.idx_fact_cluster_assignment_tenant_month
index idx_fact_cluster_assignment_run: marts.idx_fact_cluster_assignment_run
```

Any `None` here = the DDL didn't apply correctly.

**Status:** ☐ PASS ☐ FAIL

---

## 4. Sprint 1 — Train the clustering model

```powershell
python scripts\train_clustering.py --month-from 2025-01
```

**Expected:**
- Console prints feature row count, silhouette score, cluster sizes.
- Files appear on disk:
  ```
  models\clustering\kmeans_v1.joblib
  models\clustering\scaler_v1.joblib
  models\clustering\metadata.json
  ```
- An MLflow run is logged under `mlflow/` (or wherever `MLFLOW_TRACKING_URI` points).

### 4.1 Verify artifacts

```powershell
dir models\clustering\
type models\clustering\metadata.json
```

`metadata.json` should contain `feature_order`, `training_window`, `silhouette`, and `model_version`.

**Common failures:**
- `permission denied for view v_ml_features_full` → grants missing.
- `No data returned from v_ml_features_full` → backfill the DB first (`python scripts\run_batch.py backfill --from 2024-01`).

**Status:** ☐ PASS ☐ FAIL

---

## 5. Sprint 1 — Launch the API

In a **new terminal** (keep the first one for follow-up checks):

```powershell
.\.venv\Scripts\Activate.ps1
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

**Expected:** uvicorn prints `Application startup complete`. Leave it running.

### 5.1 Health probe

In the first terminal:

```powershell
curl http://127.0.0.1:8000/health
```

**Expected:** JSON with `status: "ok"` and a `db: "connected"` (or similar) field.

### 5.2 Risk score endpoint

```powershell
curl -X POST http://127.0.0.1:8000/score/risk `
  -H "Content-Type: application/json" `
  -d '{\"overspeed_per_100km\": 8.5, \"overspeed_count\": 120, \"overspeed_severity_high\": 20, \"overspeed_severity_extreme\": 5, \"high_speed_trip_ratio\": 0.25, \"speed_alert_per_100km\": 15.0, \"night_trip_ratio\": 0.30, \"avg_max_speed_kmh\": 150}'
```

**Expected:** JSON containing `risk_score` (float), `category` (string), `components` (object).

### 5.3 Cluster prediction endpoint

```powershell
curl -X POST http://127.0.0.1:8000/score/cluster `
  -H "Content-Type: application/json" `
  -d '{\"overspeed_per_100km\": 8.5, \"overspeed_count\": 120, \"overspeed_severity_high\": 20, \"overspeed_severity_extreme\": 5, \"high_speed_trip_ratio\": 0.25, \"speed_alert_per_100km\": 15.0, \"night_trip_ratio\": 0.30, \"avg_max_speed_kmh\": 150}'
```

**Expected:** JSON containing `cluster_id` (integer), `distance` (float), `version` (string).

If you get `503` here, Step 4 didn't actually persist a model — re-run it.

### 5.4 Auto-generated docs

Open in browser: <http://127.0.0.1:8000/docs>

**Expected:** Swagger UI lists all the endpoints under tags `health`, `score`, `devices`.

**Status:** ☐ PASS ☐ FAIL

---

## 6. Sprint 2 — Trigger batch scoring via the incremental flow

Back in the first terminal:

```powershell
python scripts\run_batch.py incremental
```

**Expected:** the flow runs every task and logs `task_score_latest_partition` near the end with a non-zero row count.

### 6.1 Verify rows landed in the fact table

```powershell
@'
from accent_fleet.db.engine import engine
from sqlalchemy import text
c = engine().connect()
print("rows:", c.execute(text("SELECT COUNT(*) FROM marts.fact_device_cluster_assignment")).scalar())
print("distinct months:", c.execute(text("SELECT COUNT(DISTINCT year_month) FROM marts.fact_device_cluster_assignment")).scalar())
print("distinct clusters:", c.execute(text("SELECT COUNT(DISTINCT cluster_id) FROM marts.fact_device_cluster_assignment")).scalar())
print("model versions:", c.execute(text("SELECT array_agg(DISTINCT model_version) FROM marts.fact_device_cluster_assignment")).scalar())
c.close()
'@ | python -
```

**Expected:**
- `rows` > 0
- `distinct months` >= 1
- `distinct clusters` >= 2 (K-Means with k>=2)
- `model versions` contains the version from `metadata.json`

### 6.2 Idempotency check (Sprint 2 contract)

Re-run the same incremental flow:

```powershell
python scripts\run_batch.py incremental
```

Then check the row count hasn't doubled:

```powershell
python -c "from accent_fleet.db.engine import engine; from sqlalchemy import text; print(engine().connect().execute(text('SELECT COUNT(*) FROM marts.fact_device_cluster_assignment')).scalar())"
```

**Expected:** identical (or near-identical, depending on data) row count to step 6.1. The DELETE-then-INSERT inside `_upsert_rows()` should replace, not append.

**Status:** ☐ PASS ☐ FAIL

---

## 7. Sprint 2 — Validation suite

```powershell
@'
from accent_fleet.db.engine import engine
from sqlalchemy import text
c = engine().connect()
rows = c.execute(text("""
    SELECT check_name, status, details
      FROM warehouse.etl_run_log
     WHERE check_name IN ('V9_cluster_assignment_sanity','V10_cluster_assignment_ref_integrity')
     ORDER BY logged_at DESC
     LIMIT 4
""")).all()
for r in rows:
    print(r)
c.close()
'@ | python -
```

**Expected:** the two new checks (V9, V10) appear with `status = 'PASS'`. If they're missing entirely, the validation suite didn't run — check `99_validation_suite.sql` was applied in Step 3.

**Status:** ☐ PASS ☐ FAIL

---

## 8. Sprint 2 — Streamlit dashboard

In a **third terminal** (keep API running):

```powershell
.\.venv\Scripts\Activate.ps1
streamlit run dashboard\Home.py
```

**Expected:** browser opens at <http://localhost:8501> with the welcome page.

### 8.1 Home page sanity strip

The three tiles at the top should show:
- **Database: connected** (green)
- **Last ETL run:** a timestamp (info blue)
- **Clusters: N rows / Model: fake-1 (or similar) / Latest:** ... (green)

If the third tile says "No cluster assignments yet", Step 6 didn't write rows.

### 8.2 Click through every page in the sidebar

| Page | What to look for |
|---|---|
| **0. Executive Overview** | KPIs render, MoM trend chart shows a line |
| **1. Operations** | Trip volume + harsh-event chart render |
| **2. Maintenance** | "Highest-cost vehicles" bar chart shows bars |
| **3. Risk & behavior** | Risk distribution bar, top-20 table, **Device clusters** section shows a non-empty bar chart and the "Cluster × risk-category crosstab" |
| **4. What-If** | Form renders, sliders work |

For page 3, the **Device clusters** section is the Sprint 2 deliverable — confirm:
- Metric "Clusters" shows >= 2
- Metric "Scored devices" matches row count from Step 6.1
- Cluster bar chart renders
- Model version caption appears

### 8.3 Filter check

In the sidebar, change the date range to the last 30 days and pick a tenant. Every page should re-render without errors and tables/charts shrink accordingly. Empty results should produce a yellow warning, not a stack trace.

**Status:** ☐ PASS ☐ FAIL

---

## 9. Sprint 1 — What-If form hitting the API

Still on the dashboard, navigate to **4. What-If**.

1. Leave sliders at defaults.
2. Click **Score**.

**Expected:**
- Risk score appears with a colored category badge (low / moderate / high).
- A "Per-factor contribution" Plotly bar chart renders.
- Below that, a **Cluster prediction** section shows `Cluster #N` and a distance.

If you see "Risk API call failed: ConnectError", the API isn't reachable from the dashboard process — confirm `API_BASE_URL` in `.env` and that uvicorn is still running.

If you see "Cluster model not yet available — the API returned 503", training didn't persist a model correctly — back to Step 4.

**Status:** ☐ PASS ☐ FAIL

---

## 10. Cleanup

```powershell
# In the API terminal: Ctrl+C
# In the dashboard terminal: Ctrl+C
# Optional — stop the local Postgres
docker compose down
```

---

## 11. Final verdict

| # | Section | Result |
|---|---|---|
| 1 | Setup | ☐ |
| 2 | Unit tests + DB connectivity | ☐ |
| 3 | Sprint 2 DDL applied | ☐ |
| 4 | Sprint 1 model trained | ☐ |
| 5 | Sprint 1 API up + endpoints respond | ☐ |
| 6 | Sprint 2 batch scoring rows written + idempotent | ☐ |
| 7 | Sprint 2 validation V9 + V10 PASS | ☐ |
| 8 | Sprint 2 dashboard renders all 5 pages | ☐ |
| 9 | Sprint 1 What-If form hits API successfully | ☐ |

**All boxes ticked → Sprints 1 and 2 are functionally done.** Tag with:

```powershell
git tag -a v0.5.0 -m "Operationalize platform: Sprint 1 (API+model) + Sprint 2 (dashboard+pipeline)"
git push origin v0.5.0
```

---

## 12. Troubleshooting cheat sheet

| Symptom | Likely cause | Fix |
|---|---|---|
| `ModuleNotFoundError: accent_fleet` | venv not activated, or `pip install -e .` not run | Activate `.venv` and re-install |
| `accent_fleet.__file__` points outside repo | Editable install linked to a stale path (e.g. a worktree) | `pip install -e . --force-reinstall --no-deps` from the repo root |
| `psycopg.OperationalError: password authentication failed` | `.env` not being read or wrong creds | `python -c "from accent_fleet.config import settings; print(settings.pg_user, settings.pg_host)"` |
| `permission denied for view v_ml_features_full` | DB grants missing | See `DB_ACCESS_REQUEST.md` |
| `to_regclass(...) -> None` after bootstrap | DDL didn't apply, likely missing CREATE on schema | Re-run Step 3 as a role with `CREATE ON SCHEMA marts` |
| API `/score/cluster` returns 503 | No model file under `models/clustering/` | Re-run Step 4 |
| Dashboard 4_What_If: "Cluster model not yet available" | Same as above | Re-run Step 4 |
| Dashboard 3_Risk: "No cluster assignments yet" | Step 6 didn't write rows | Re-run Step 6 |
| V9 / V10 missing from `etl_run_log` | `99_validation_suite.sql` not applied | Re-run Step 3, confirm file lists `V9` and `V10` |
| Streamlit port 8501 already in use | Old streamlit process | `streamlit run dashboard\Home.py --server.port 8502` |

---

## 13. What's deliberately NOT tested

These are documented gaps, not bugs:

- **Authentication** — API and dashboard are unauthenticated. Don't expose to the internet.
- **Multi-tenant scoping** — sidebar has a tenant filter but no per-user enforcement.
- **Streaming (Redpanda)** — service commented out in `docker-compose.yml`.
- **Geo views** — `pydeck` map views deferred until lat/long surface in marts.
- **Cloud deployment** — Kubernetes / Terraform / cloud-hosted MLflow not in scope for v0.5.0.

These are tracked for Sprints 3+.

---

**Author of this runbook:** Mohamed Amine
**Last verified against:** `main` @ commit `<paste hash from Step 1.1>`
