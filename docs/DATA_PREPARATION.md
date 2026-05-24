# Data Preparation — Deep Dive

> Companion to [`docs/HOW_IT_WORKS.md`](HOW_IT_WORKS.md) and
> [`docs/MODELING_AND_EVALUATION.md`](MODELING_AND_EVALUATION.md). This
> document zooms into the **Data Preparation** phase of the CRISP-DM cycle
> for Accent Fleet Analytics — every transformation that takes the raw
> telematics feed and produces the ML-ready feature contract.
>
> Reading order: this doc explains *how the marts are built*; the modeling
> doc explains *what the models do with them*.

---

## 0. CRISP-DM scope of this phase

In CRISP-DM, **Data Preparation** is "all activities to construct the final
dataset from the initial raw data" — selecting, cleaning, constructing,
integrating, formatting. In this project that maps to:

| CRISP-DM sub-task | Where it lives in the repo |
|---|---|
| Select data | `sql/01_*` – `sql/06_*` (dims), `config/pipeline.yaml::sources` |
| Clean data | `config/cleaning_rules.yaml` + `src/accent_fleet/cleaning/rules.py` (C1–C7) and inline SQL in `sql/15_*`, `sql/16_*` (C8–C11) |
| Construct data | `sql/10_*` – `sql/19_*`, `sql/24_*`, `sql/27_*` (facts + telemetry reconstruction) |
| Integrate data | `sql/20_*` – `sql/26_*`, `sql/30_*` – `sql/32_*` (marts joining the facts) |
| Format data | `StandardScaler` in `ml/train_clustering.py`; SQL `CAST`s + `COALESCE`s in views |
| Verify quality | `sql/99_validation_suite.sql` + `tests/test_cleaning_rules.py` |

The phase **outputs two SQL views** that the modeling phase consumes:
`marts.v_ml_features_full` (clustering) and `marts.v_device_risk_profile`
(risk score).

---

## 1. Raw sources

All raw data lives in the **`staging`** schema of the project's Postgres
instance (by default Azure Database for PostgreSQL; can be swapped for a
local Postgres via the `localdb` Docker target). The staging schema is
**bronze**: typed but otherwise untrusted — same shape as the upstream
telematics exports.

The pipeline reads from these staging tables (declared in
[`config/pipeline.yaml::sources`](../config/pipeline.yaml)):

| Table | Grain | What it carries |
|---|---|---|
| `staging.path` | one row per trip segment | `begin_path_time`, `path_duration`, `distance_driven`, `max_speed`, `fuel_used`, GPS coords |
| `staging.rep_overspeed` | one row per overspeed event | `begin_path_time`, `max_speed`, `speed_limit` |
| `staging.stop` | one row per stop | `stop_start`, `stop_duration` |
| `staging.notification` | one row per alert | `created_at`, `alert_type` / `description` (filtered to `description LIKE 'SPEED%'` for the ML feature contract; full coverage kept for BI) |
| `staging.rep_activity_daily` | one row per device-day | `activity_start_time`, `working_hours`, ignition minutes |
| `staging.archive` | one row per high-frequency telemetry ping (~30 s cadence) | accelerometer `x/y/z`, `rpm`, `speed`, lat/lon, `ignition` |
| `staging.maintenance` | one row per work-order header (BI only) | `date_operation`, costs |
| `staging.maintenance` line items | one row per offense / sinistre / réparation (BI only) | derived from the same staging table |
| `staging.document` | fueling events (BI only — filtered to `doc_type = 'Fueling'`) | `date_operation`, fuel cost, litres |

Documented row volume: **`staging.archive` ≈ 54.7 M rows** (per
[`config/cleaning_rules.yaml`](../config/cleaning_rules.yaml) C8–C11 block
and [`config/pipeline.yaml`](../config/pipeline.yaml) source comment).
Other source volumes are not documented in the repo.

The pipeline assumes the staging schema is fed by an external ingestion job
(not in this repo) — the ETL service inside this project picks up from
`staging` and is responsible for everything downstream.

---

## 2. Schema layout — bronze / silver / gold

The warehouse is split into three Postgres schemas (created in
[`sql/00_schemas_and_state.sql`](../sql/00_schemas_and_state.sql)):

```
staging   (bronze)  →  warehouse  (silver)  →  marts  (gold)
   raw external          cleaned facts +          aggregated
   exports               static dimensions        features / KPIs
```

- **`staging`** — bronze: raw external data, no FKs, possibly dirty.
- **`warehouse`** — silver: cleaned, deduplicated, typed; one fact table per
  business event + the static dimensions (`dim_tenant`, `dim_vehicle`,
  `dim_device`, `dim_driver`, `dim_date`). Operational tables
  `warehouse.etl_watermark`, `warehouse.etl_run_log`, and
  `warehouse.quarantine_rejected` also live here.
- **`marts`** — gold: aggregations at business grains (device-month,
  fleet-day, tenant-month) plus the ML-ready feature views.

This split is reflected in the SQL migration numbering: `00_*` for schemas
and state, `01-09` for dims, `10-19` for facts, `20-29` for ML marts,
`30-39` for BI marts, `40_*` for retention, `50-59` for auth/RLS,
`60_*` for the AI-query log, `99_*` for validation.

---

## 3. ETL architecture

**Orchestrator:** Prefect 3 (`prefect>=3.0.0` in `requirements.txt`; not
Airflow / not dbt). Flows live in
[`src/accent_fleet/pipeline/flow_batch.py`](../src/accent_fleet/pipeline/flow_batch.py).

Four flows, each runnable independently:

| Flow name | When it runs | What it does |
|---|---|---|
| `bootstrap_flow` (`accent-bootstrap`) | once per environment | Runs DDL (`sql/00_*` – `sql/06_*`) + loads static dims |
| `backfill_flow` (`accent-backfill`) | once on first deploy | Historical load of staging → warehouse → marts in 30-day chunks (`backfill_chunk_days: 30`) |
| `incremental_flow` (`accent-incremental`) | every 5 minutes | Processes only rows newer than the watermark |
| `retrain_flow` (`accent-retrain`) | manual / gated | Re-fits the K-Means model and gates promotion to MLflow registry on silhouette ≥ threshold |

The cadence (`config/pipeline.yaml::window.incremental_interval_minutes: 5`)
ties the incremental flow to a tight loop; a **10-minute lookback overlap**
(`overlap_minutes: 10`) catches late-arriving rows.

### 3.1 Watermarks

Incremental bounds are driven by `warehouse.etl_watermark`, declared in
[`sql/00_schemas_and_state.sql`](../sql/00_schemas_and_state.sql):

```sql
CREATE TABLE warehouse.etl_watermark (
  layer             TEXT NOT NULL,            -- 'warehouse' | 'marts'
  table_name        TEXT NOT NULL,            -- fact_trip, fact_stop, ...
  last_event_time   TIMESTAMP,
  last_run_at       TIMESTAMPTZ,
  last_etl_run_id   BIGINT,
  rows_loaded_total BIGINT NOT NULL DEFAULT 0,
  notes             TEXT,
  PRIMARY KEY (layer, table_name)
);
```

Every incremental SQL is parameterised with `:window_start` and
`:window_end`, e.g. `sql/10_fact_trip_incremental.sql`:

```sql
WHERE p.begin_path_time >= :window_start
  AND p.begin_path_time <  :window_end
```

After a successful load, the watermark for that table is advanced to the
`max(event_time)` from the chunk just written.

### 3.2 Lineage

Every flow run writes one row to `warehouse.etl_run_log` with
`mode` (bootstrap | backfill | incremental | stream), `started_at`,
`finished_at`, `status`, `window_start/end`, `rows_read`, `rows_loaded`,
`rows_rejected`, `rejected_by_rule` (JSONB: `{"C2": 12, "C3": 3, …}`),
`git_sha`, and `config_hash` (MD5 of the YAML configs). This is the audit
trail used by the validation suite and by the operations dashboards.

---

## 4. Cleaning rules — C1 through C11

All cleaning is **declarative**: rules live in
[`config/cleaning_rules.yaml`](../config/cleaning_rules.yaml) and a thin
Python engine in
[`src/accent_fleet/cleaning/rules.py`](../src/accent_fleet/cleaning/rules.py)
turns YAML into either Polars filters (in-memory tests) or SQL `WHERE` /
`CASE` fragments (in-database execution).

There are **11 rules** — split by execution path:

### 4.1 Path / trip / event rules (C1 – C7) — Polars engine + SQL

| ID | Scope | Condition | Action | Reason |
|---|---|---|---|---|
| **C1** | `path`, `stop`, `rep_overspeed`, `notification`, `rep_activity_daily` | `time_column >= '2019-10-01'::timestamp` (column resolved per table) | **reject** | GPS devices without a synced clock default to Unix epoch (1970) or GPS epoch (1980). Earliest valid tenant data is 2019-10-01. |
| **C2** | `path` | `path_duration > 0` | reject | 144 trips have negative duration (end < start); 1,473 have zero — both are data errors. |
| **C3** | `path` | `distance_driven > 0` | reject | Zero-distance trips are GPS noise (16 records). |
| **C4** | `path` | `fuel_used BETWEEN 0 AND 500` | **nullify** | `fuel_used` has INT32 overflow and 94.7 % zeros. Out-of-range values are nullified so models see `NULL` instead of junk. |
| **C5** | `path`, `rep_overspeed` | `max_speed <= 200` | **clamp** (to 200) | Device hard limit is 200 km/h. Clamp rather than reject to preserve the row for other features. |
| **C6** | `stop` | `stop_duration > 0 AND stop_duration < 31_536_000` (1 year) | reject | Zero/negative durations are errors; stops > 1 year indicate decommissioned vehicles. |
| **C7** | `device` | `vehicule_id IS NOT NULL` | reject (at dim-load) | 16 devices have no vehicle link. Enforced as an `INNER JOIN` in `sql/03_dim_device.sql`. |

### 4.2 Archive (telemetry) rules (C8 – C11) — inline in SQL

The archive table is **too large (~54.7 M rows)** for in-memory Polars
filtering, so C8–C11 are enforced **inline** inside
[`sql/15_fact_harsh_event_incremental.sql`](../sql/15_fact_harsh_event_incremental.sql)
and
[`sql/16_fact_telemetry_daily_incr.sql`](../sql/16_fact_telemetry_daily_incr.sql)
rather than via the Polars engine.

| ID | Scope | Condition | Action | Reason |
|---|---|---|---|---|
| **C8** | `archive` | `ignition = 1` | reject | Accelerometer readings while the engine is off are sensor noise (passers-by vibration, doors). Harsh-event detection requires ignition on. |
| **C9** | `archive` | `ABS(x) <= 127 AND ABS(y) <= 127 AND ABS(z) <= 127` | reject | Accelerometer is a signed `int8` sensor (`-128..127`). Out-of-range values are corrupted packets. |
| **C10** | `archive` | `speed BETWEEN 0 AND 250` | **clamp** | Reasonable physical bounds for fleet vehicles; clamp rather than reject to preserve the row for other aggregates. |
| **C11** | `archive` | `rpm BETWEEN 0 AND 8000` | **clamp** | RPM bounds for diesel / petrol fleet engines; out-of-range values are decoder errors. |

### 4.3 Other transforms declared in the same file

`config/cleaning_rules.yaml` also carries two non-row-filter lookups,
applied in `transforms/dimensions.py` and `sql/02_dim_vehicle.sql`:

- **`make_normalization`** — 17 entries collapsing 38 vehicle-make
  variants (`IVECO`, `Iveco`, `iveco` → `Iveco`; same for Isuzu, Scania,
  Renault, MAN, Volvo, Dacia, Suzuki, Fiat, Mitsubishi).
- **`vehicle_class_map`** — derives `vehicle_class ∈ {heavy, medium, light}`
  from the normalised make.

### 4.4 Quarantine

Rejected rows are not lost: when `quarantine.enabled: true` (default in
`config/pipeline.yaml`) they are written to `warehouse.quarantine_rejected`
with the violated `rule_id`, the rejected row as `JSONB`, and a 90-day
retention.

### 4.5 Tests

Every C1–C7 rule has a dedicated Polars test in
[`tests/test_cleaning_rules.py`](../tests/test_cleaning_rules.py) that
asserts the YAML-described behaviour matches the engine. C8–C11 are
covered by the SQL validation suite (V3–V5 etc.) and the smoke tests in
`tests/test_incremental_semantics.py`.

---

## 5. Constructing the facts

The silver layer materialises one fact table per business event. The
SQL files are numbered to fix their load order:

| File | Fact | Source | Notes |
|---|---|---|---|
| `sql/10_fact_trip_incremental.sql` | `warehouse.fact_trip` | `staging.path` | Primary trip table |
| `sql/11_fact_overspeed_incremental.sql` | `warehouse.fact_overspeed` | `staging.rep_overspeed` | Severity bucketed (low / medium / high / extreme) |
| `sql/12_fact_stop_incremental.sql` | `warehouse.fact_stop` | `staging.stop` | Stop-type tags (micro / short / medium / long) |
| `sql/13_fact_speed_notification_incr.sql` | `warehouse.fact_speed_notification` | `staging.notification` filtered to `description LIKE 'SPEED%'` | ML feature channel |
| `sql/14_fact_daily_activity_incr.sql` | `warehouse.fact_daily_activity` | `staging.rep_activity_daily` | Working hours, ignition minutes |
| `sql/15_fact_harsh_event_incremental.sql` | `warehouse.fact_harsh_event` | `staging.archive` accelerometer pings | C8–C9 applied inline |
| `sql/16_fact_telemetry_daily_incr.sql` | `warehouse.fact_telemetry_daily` | `staging.archive` aggregated to device-day | C10–C11 applied inline |
| `sql/17_fact_notification_incr.sql` | `warehouse.fact_notification` | `staging.notification` (full coverage) | BI superset of `fact_speed_notification` |
| `sql/18_fact_maintenance_incr.sql` | `warehouse.fact_maintenance` | `staging.maintenance` | BI only |
| `sql/19_fact_maintenance_line_incr.sql` | `warehouse.fact_maintenance_line` | `staging.maintenance` line items | BI only |
| `sql/24_fact_fueling_incr.sql` | `warehouse.fact_fueling` | `staging.document` filtered to `doc_type = 'Fueling'` | BI only |

### 5.1 `fact_trip` — column derivations

`sql/10_fact_trip_incremental.sql` casts staging columns and derives
analytical helpers:

```sql
duration_seconds  = path_duration
distance_km       = distance_driven                     -- already in km
avg_speed_kmh     = distance_km / NULLIF(duration_seconds / 3600.0, 0)
speed_ratio       = max_speed / NULLIF(avg_speed_kmh, 0)
is_night_trip     = EXTRACT(hour FROM begin_path_time) IN (21..23, 0..4)
is_weekend_trip   = EXTRACT(dow  FROM begin_path_time) IN (0, 6)
is_rush_hour_trip = EXTRACT(hour FROM begin_path_time) IN (7, 8, 17, 18)
```

Idempotency: `UNIQUE (tenant_id, device_id, begin_path_time)` — the same
trip never enters the fact twice, even across overlapping incremental
windows.

### 5.2 Telemetry-reconstructed trips

For tenants where the `path` feed is incomplete (notably **tenant 7486**,
called out as `telemetry-first` in `config/pipeline.yaml::modeling`),
[`sql/27_fact_trip_reconstruct_telemetry_last_month.sql`](../sql/27_fact_trip_reconstruct_telemetry_last_month.sql)
re-derives trips from raw `staging.archive` pings by stitching consecutive
ignition-on segments. Each row in `fact_trip` carries
`trip_source ∈ {'path', 'archive'}` and a `reconstruction_method` tag so
downstream code can tell them apart for QA.

### 5.3 Harsh-event thresholds

[`config/pipeline.yaml::archive_thresholds`](../config/pipeline.yaml)
configures the harsh-event detector applied in `sql/15`:

| Threshold | Value (int8 axis) | Approx g-force | Use |
|---|---|---|---|
| `brake` / `accel` / `corner` | 40 | ≈ 0.31 g | Event trigger (per-axis) |
| `high` | 60 | ≈ 0.47 g | Severity tier upper bound |
| `extreme` | 80 | ≈ 0.63 g | Severity tier upper bound |

Telemetry aggregation parameters (`ping_seconds: 30`,
`rpm_high_threshold: 3000`) are also declared there.

---

## 6. Feature engineering

### 6.1 Two feature surfaces, one mart

| Surface | Built in | Grain | Purpose |
|---|---|---|---|
| `marts.v_ml_features_driver_behavior` | `sql/22_v_ml_features.sql` (view over `marts.mart_device_monthly_behavior`, `sql/20`) | (tenant, device, year_month) | 35 trip-derived baseline features |
| `marts.mart_device_monthly_telemetry` | `sql/25_mart_device_monthly_telemetry.sql` | same | Archive-derived features (harsh events, idle, RPM) |
| **`marts.v_ml_features_full`** | `sql/26_v_ml_features_full.sql` | same | **The ML feature contract** — `LEFT JOIN` of the two: 35 baseline + 22 archive-derived columns |

The `LEFT JOIN` (not `INNER`) is deliberate: devices with no archive
pings still get a feature row (telemetry columns come out `0` / `NULL`),
so the clustering model never silently drops them.

The 22 archive-derived columns are grouped (per the SQL header comments) as:

| Group | Columns | What it captures |
|---|---|---|
| T1 — raw harsh counts | `harsh_brake_count`, `harsh_accel_count`, `harsh_corner_count`, `harsh_event_total` | Event-level counts |
| T2 — severity tiers | `harsh_moderate_count`, `harsh_high_count`, `harsh_extreme_count` | Distribution of severity |
| T3 — per-100km rates | `harsh_brake_per_100km`, `harsh_accel_per_100km`, `harsh_corner_per_100km`, `harsh_events_per_100km` | Distance-normalised harsh rates |
| T4 — engine / idling | `total_ignition_on_minutes`, `total_moving_minutes`, `total_idle_minutes`, `monthly_idle_ratio`, `active_telemetry_days` | Engine time + idle exposure |
| T5 — RPM | `avg_rpm`, `max_rpm`, `total_high_rpm_seconds`, `high_rpm_minutes_per_day` | Over-revving exposure |
| T6 — telemetry speed | `avg_telemetry_speed_kmh`, `max_telemetry_speed_kmh` | Speed from archive (parallel channel to fact_trip) |
| T7 — fuel | `total_fuel_used_archive` | Archive-side fuel measure |

### 6.2 The 13 features the K-Means model consumes

From `FEATURES` in
[`src/accent_fleet/ml/train_clustering.py`](../src/accent_fleet/ml/train_clustering.py)
(line 40):

```
overspeed_per_100km           — overspeed events normalised by distance
avg_speed_over_limit          — average severity per overspeed event
high_speed_trip_ratio         — share of trips with max_speed > 100 km/h
speed_alert_per_100km         — speed-notification rate
harsh_brake_per_100km         — harsh brakes normalised by distance
harsh_accel_per_100km         — harsh accelerations normalised
harsh_corner_per_100km        — harsh corners normalised
monthly_idle_ratio            — idle_minutes / ignition_on_minutes
high_rpm_minutes_per_day      — over-revving exposure
night_trip_ratio              — share of trips between 21:00 and 05:00
rush_hour_trip_ratio          — share in rush bands (7, 8, 17, 18 h)
stddev_trip_distance          — within-month variability
short_trip_ratio              — share of trips < 1 km (cold-start wear)
```

### 6.3 The 13 features the risk model consumes

**The same 13 features that feed clustering**, listed in §6.2 above.
The per-tenant **Isolation Forest** risk model (see
[`MODELING_AND_EVALUATION.md`](MODELING_AND_EVALUATION.md) §3.1) treats
the full feature vector as a single multivariate anomaly signal rather
than weighting individual factors — so there's no separate "factor
table" any more. The features that used to be hand-weighted (overspeed
rate, severe-overspeed share, high-speed-trip ratio, speed-alert rate,
night-driving share, max-speed proxy) are all still in the input
vector; the IF just decides on their joint significance per tenant
instead of summing fixed weights.

Hyperparameters live in
[`config/feature_definitions.yaml::risk_score_model`](../config/feature_definitions.yaml)
(`n_estimators`, `contamination`, `random_state`, `min_rows`, and the
quantile thresholds 0.50 / 0.80 / 0.95 that carve the per-tenant
distribution into low/moderate/high/critical bands).

The persisted output lives in `marts.fact_device_risk_score` (one row
per device-month, with `model_version` + `model_source` stamping). The
legacy view `marts.v_device_risk_profile` is now a thin compat wrapper
over that fact table — same column names, same grain — so consumers
still working off the view (the dashboard, older notebooks) keep
working unchanged. `tests/test_feature_computation.py` pins
`RiskPredictor.predict()` to within 0.1 of the persisted fact values
(filtered to the same `model_version` the predictor is currently
serving).

### 6.4 Activity gates

Two thresholds from
[`config/feature_definitions.yaml`](../config/feature_definitions.yaml):

- `minimum_trips_for_scoring: 5` — applied in
  `marts.v_ml_features_driver_behavior` (clustering input gate)
- `minimum_trips_for_risk: 10` — applied in `marts.v_device_risk_profile`
  (risk-score gate, computed over a 3-month rolling window — `risk_window_months: 3`)

The clustering training query in `ml/train_clustering.py::load_training_frame()`
adds two further filters:

```sql
WHERE year_month         >= :month_from       -- default '2025-01'
  AND total_distance_km   >= 100              -- ≥ 100 km in the month
  AND total_ignition_on_minutes > 0           -- engine actually started
```

This excludes device-months that are too sparse to be statistically
informative.

---

## 7. Aggregation marts

The marts the **dashboard** reads (the modelling phase only uses
`v_ml_features_full` and `v_device_risk_profile`):

| Table / view | Grain | Built from | Used by |
|---|---|---|---|
| `marts.mart_device_monthly_behavior` | device-month | `fact_trip` + `fact_overspeed` + `fact_stop` + `fact_speed_notification` + `fact_daily_activity` | clustering input, behaviour dashboards |
| `marts.mart_device_monthly_telemetry` | device-month | `fact_harsh_event` + `fact_telemetry_daily` | telemetry features |
| `marts.v_ml_features_driver_behavior` | device-month | the behaviour mart | 35-column baseline view |
| `marts.v_ml_features_full` | device-month | join of the two above | 35 + 22 = **the ML feature contract** |
| `marts.v_device_risk_profile` | device (3-month rolling) | `mart_device_monthly_behavior` | risk dashboard, risk leaderboard |
| `marts.v_fleet_risk_dashboard` | tenant | `v_device_risk_profile` | fleet-level rollup |
| `marts.fact_device_cluster_assignment` | device-month | written by `batch_scoring.py` | dashboard cluster overlay |
| `marts.mart_fleet_daily` | tenant-day | `fact_trip` + `fact_stop` + `fact_overspeed` + `fact_harsh_event` + `fact_notification` + `fact_maintenance` + `fact_fueling` | Executive / Operations dashboards |
| `marts.mart_vehicle_monthly` | tenant-vehicle-month | `fact_trip` + `fact_maintenance` + `fact_maintenance_line` + `fact_fueling` | BI fleet-operations rollup |
| `marts.mart_tenant_monthly_summary` | tenant-month | rollup of `mart_vehicle_monthly` + `mart_fleet_daily` | Tenant-level summary |

Mart recomputation is driven by **touched windows** (declared in
[`config/pipeline.yaml::marts`](../config/pipeline.yaml)):

- Monthly marts recompute only the months that received new fact rows in
  this run (`recompute_scope: touched_months`).
- `fleet_daily` is day-grain (`recompute_scope: touched_dates`).
- `tenant_monthly_summary` carries `depends_on_marts: [vehicle_monthly,
  fleet_daily]` so it always runs after its inputs.

---

## 8. Scaling and normalisation

Two different strategies, by model:

### 8.1 Clustering — StandardScaler

In `ml/train_clustering.py`:

```python
X  = df[FEATURES].fillna(0)
scaler = StandardScaler().fit(X)
Xs = scaler.transform(X)
KMeans(n_clusters=k, random_state=42, n_init=10).fit(Xs)
```

K is chosen from `DEFAULT_K_RANGE = range(3, 7)` (so k ∈ {3, 4, 5, 6}) by
silhouette score. `RANDOM_STATE = 42` and `MIN_ROWS = 50` (training
aborts below this).

The fitted scaler is persisted alongside the K-Means model in two places:

- `models/clustering/scaler_v<n>.joblib` (local-disk fallback)
- MLflow artifact under model name `device_clustering` (preferred source)

At inference time `ml/inference.py` loads the same scaler so an arbitrary
feature vector is transformed identically to what was used in training.

### 8.2 Risk score — linear-divide-and-clamp in SQL

Each factor in `sql/21_v_device_risk_profile.sql` is normalised by
dividing by the hand-chosen maximum in §6.3 and clamping to `[0, 1]`.
This is intentionally simple and deterministic so the result can be
explained per-factor in the dashboard's contribution chart.

---

## 9. Data quality — the validation suite

[`sql/99_validation_suite.sql`](../sql/99_validation_suite.sql) is run by
the Prefect `task_run_validation` step after every incremental flow. Ten
checks, each returning `(check_name, passed, value)`:

| ID | Check | Pass condition |
|---|---|---|
| V1 | `dims_populated` | `dim_tenant`, `dim_vehicle`, `dim_device`, `dim_date` all non-empty |
| V2 | `fact_trip_ref_integrity` | Zero `fact_trip` rows with no matching `dim_device` |
| V3 | `fact_trip_C1_enforced` | No `fact_trip` rows with `begin_path_time < '2019-10-01'` |
| V4 | `fact_trip_C2C3_enforced` | No `fact_trip` row with `duration_seconds <= 0` or `distance_km <= 0` |
| V5 | `fact_trip_C5_enforced` | No `fact_trip` row with `max_speed_kmh > 200` |
| V6 | `fact_trip_null_rates` | `distance_km` and `max_speed_kmh` null rates each below 1 % |
| V7 | `mart_grain_coverage` | At least one `(device, year_month)` row in the current window |
| V8 | `risk_score_distribution_sanity` | Categories not all-`null`, not all-`critical` |
| V9 | `cluster_assignment_sanity` | All `cluster_id >= 0` and `distance_to_centroid` finite (empty table is allowed; V10 covers that) |
| V10 | `cluster_assignment_referential` | Every cluster assignment has a matching row in the feature mart (no orphans) |

Python-level tests live in [`tests/`](../tests/) — notably:

- `tests/test_cleaning_rules.py` — every YAML rule has a Polars assertion
- `tests/test_feature_computation.py` — SQL ≡ Python parity for the risk
  score
- `tests/test_incremental_semantics.py` — watermark advance, overlap
  handling, idempotency
- `tests/test_watermark.py` — watermark state machine

Monitoring thresholds for run health are declared in
[`config/pipeline.yaml::monitoring`](../config/pipeline.yaml):
`fact_trip_max_lag_minutes: 15`, `max_rejection_rate_pct: 5.0`,
`expected_min_new_trips_per_window: 50`.

There is **no Great Expectations / dbt-tests layer** in the project —
quality is enforced exclusively by the SQL suite + pytest, written
against the same YAML rules.

---

## 10. Train / test split

There is **no row-level train/test split** for clustering — K-Means is
unsupervised; evaluation is silhouette score on the same training rows
(by design).

What *is* split:

- **Time gate** — `year_month >= '2025-01'` by default in
  `load_training_frame()`. Older months are excluded from training to
  keep the model representative of current driving patterns.
- **Tenant coverage gate** — `config/pipeline.yaml::modeling.expected_tenants`
  lists the tenants the training set must contain
  (`[235, 238, 264, 1787, 7486]`); `required_tenants: [7486]` fails the
  run if tenant 7486 is absent (it's the telemetry-first tenant — losing
  it would silently halve the cohort).

Risk-score evaluation is structural (every device gets a score by
formula), so it doesn't need a split.

---

## 11. Outputs — what data preparation hands to modelling

Three kinds of artefacts cross the phase boundary:

### 11.1 SQL views (the model's data contract)

- `marts.v_ml_features_full` — 35 + 22 = 57 columns at
  (tenant, device, year_month). This is what
  `ml/train_clustering.py::load_training_frame()` reads on every run.
- `marts.v_device_risk_profile` — the deterministic risk score; no model
  training required.

### 11.2 Joblib + MLflow artefacts (after the first training run)

Produced by `ml/train_clustering.py`, written to both local disk
(`models/clustering/`) and the MLflow registry (preferred):

- `kmeans_v<n>.joblib`
- `scaler_v<n>.joblib`
- `metadata.json` (`k`, `silhouette`, `n_rows`, `feature_order`,
  `cluster_sizes`, `tenants`, `random_state`, `training_window`)

These are loaded by:

- `app/routes/score.py` — for the live `/v1/score/cluster` endpoint
- `scripts/batch_scoring.py` — to fill `marts.fact_device_cluster_assignment`
  at the end of every incremental flow

### 11.3 Operational artefacts

Not consumed by modelling but essential to running the pipeline:

- `warehouse.etl_watermark` — incremental bounds, one row per
  `(layer, table)`
- `warehouse.etl_run_log` — one row per Prefect flow run (lineage +
  validation results, with `rejected_by_rule` JSONB breakdown)
- `warehouse.quarantine_rejected` — rows that failed cleaning rules,
  retained 90 days for audit
- `sql/40_retention.sql` prunes these on every incremental run per
  `config/pipeline.yaml::retention` (`etl_run_log_days: 90`,
  `quarantine_days: 30`, `cluster_assignment_months: 12`)

---

## 12. Hand-off to the next phase

Once a Prefect incremental flow finishes successfully and the validation
suite is green, the modelling phase can run with no further preparation:

```bash
# uses marts.v_ml_features_full directly
python scripts/train_clustering.py

# uses marts.v_device_risk_profile directly  (no training needed)
# nothing to do — the view is already current
```

This is the boundary between Data Preparation and Modeling — anything
upstream of `marts.v_*` is described in this document; everything
downstream is in [`MODELING_AND_EVALUATION.md`](MODELING_AND_EVALUATION.md).
