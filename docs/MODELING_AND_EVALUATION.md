# Modeling & Evaluation — Deep Dive

> Companion to `docs/HOW_IT_WORKS.md`. This document zooms into the **Modeling**
> and **Evaluation** phases of the CRISP-DM cycle for Accent Fleet Analytics:
> exactly which algorithms run, what they consume, what they produce, and how
> we decide whether a freshly trained model is good enough to ship.

---

## 1. The two models in the system

Accent Fleet Analytics is *not* a single big model. It is **two independent
scoring systems** that consume the same monthly feature mart but answer
different questions:

| Model | Type | Question it answers | Output |
|---|---|---|---|
| **Composite Risk Score** | Deterministic weighted formula (no learning) | "How risky is this device's driving over the last 3 months?" | Integer `risk_score` ∈ [0, 100] + category (`low` / `moderate` / `high` / `critical`) |
| **Device-Behavior Clustering** | Unsupervised ML (KMeans) | "Which behavioural archetype does this device belong to?" | `cluster_id` ∈ {0, …, K-1} + Euclidean `distance_to_centroid` |

Both run in two places that **must agree numerically**:

- **In-database** as SQL views/marts (used by the dashboard for batch reads).
- **In-process** as Python (`features/risk_score.py`, `ml/inference.py`) called
  by the FastAPI service for ad-hoc and online scoring.

A pinned unit test (`tests/test_feature_computation.py`) enforces SQL ≡ Python
parity on the risk score so we can never drift the two implementations apart.

---

## 2. Inputs — the feature contract

### 2.1 The feature mart

Both models read from the same view: **`marts.v_ml_features_full`**
(definition in `sql/26_v_ml_features_full.sql`).

Grain: **one row per `(tenant_id, device_id, year_month)`**.

It's a `LEFT JOIN` of:

- `marts.v_ml_features_driver_behavior` — 35 trip-derived features (overspeed,
  alerts, trip volumes, temporal patterns).
- `marts.mart_device_monthly_telemetry` — ~20 archive-derived features (harsh
  events, idling, RPM).

`LEFT JOIN` (not inner) ensures devices with no archive pings still produce a
feature row — telemetry columns just come out as `0` / `NULL`.

### 2.2 Activity gate

The training query in `ml/train_clustering.py::load_training_frame()` filters
to **active** device-months only:

```sql
WHERE year_month  >= :month_from         -- default '2025-01'
  AND total_distance_km     >= 100       -- must have driven at least 100 km
  AND total_ignition_on_minutes > 0      -- engine actually started that month
```

Devices below those thresholds are dropped — they don't have enough signal to
cluster meaningfully and would just collapse into a degenerate "barely-driven"
cluster.

The risk-score view applies its own gate (`trips_3m >= 10`) in
`sql/21_v_device_risk_profile.sql`.

### 2.3 Feature lists

#### Clustering — 13 features (`FEATURES` in `ml/train_clustering.py`)

These were picked because they cover the **four behavioural axes** the
clustering is meant to separate:

| Axis | Features |
|---|---|
| Overspeed behaviour | `overspeed_per_100km`, `avg_speed_over_limit`, `high_speed_trip_ratio`, `speed_alert_per_100km` |
| Harsh driving | `harsh_brake_per_100km`, `harsh_accel_per_100km`, `harsh_corner_per_100km` |
| Engine / efficiency | `monthly_idle_ratio`, `high_rpm_minutes_per_day` |
| Exposure / pattern | `night_trip_ratio`, `rush_hour_trip_ratio`, `stddev_trip_distance`, `short_trip_ratio` |

#### Risk score — 6 weighted factors (`config/feature_definitions.yaml`)

| Factor (config name) | Source feature(s) | Weight | Normalization cap |
|---|---|---:|---:|
| `overspeed_rate` | `overspeed_per_100km` | **0.30** | linear / 10.0 |
| `severe_overspeed_share` | `(overspeed_severity_high + overspeed_severity_extreme) / overspeed_count` | **0.20** | identity (already 0–1) |
| `high_speed_trip_ratio` | `high_speed_trip_ratio` | **0.15** | linear / 0.30 |
| `speed_alert_rate` | `speed_alert_per_100km` | **0.15** | linear / 20.0 |
| `night_driving` | `night_trip_ratio` | **0.10** | linear / 0.30 |
| `max_speed` | `avg_max_speed_kmh` | **0.10** | linear / 200.0 |

Weights sum to **1.00**. The composite score is `Σ (weight × normalized_value) × 100`.

---

## 3. Modeling Phase

### 3.1 Risk score — deterministic composite formula

There is **no training step** for the risk score. It is a fixed weighted sum
of normalized factors. Everything that defines its behaviour lives in
`config/feature_definitions.yaml` under the `risk_score:` key.

#### Algorithm

```
For each device-month (rolling 3-month window):
  1.  raw_value(factor) ←  pull from features dict OR evaluate formula
  2.  normalized        ←  normalize(raw, factor.normalize_spec)
                           ├─ linear:   clamp(raw / max,  0, 1)
                           └─ identity: clamp(raw,        0, 1)
  3.  contribution      ←  factor.weight × normalized
  4.  total             ←  Σ contributions
  5.  risk_score        ←  round(total × 100, 1)
  6.  category          ←  bucket(risk_score)
                           ├─ [ 0,  19] → low
                           ├─ [20,  44] → moderate
                           ├─ [45,  69] → high
                           └─ [70, 100] → critical
```

#### Where it runs

- **SQL view** `sql/21_v_device_risk_profile.sql` — used by the dashboard's
  device drill-down and risk distribution charts. Operates on a rolling
  3-month aggregation per `(tenant_id, device_id)`.
- **Python** `src/accent_fleet/features/risk_score.py` (`RiskScorer.score()`)
  — used by `POST /v1/score/risk` for what-if scoring of arbitrary feature
  vectors and by the online scoring path.
- **Per-factor breakdown** `src/accent_fleet/ml/inference.py::score_with_components()`
  — returns the same total **plus** each factor's weighted contribution so the
  dashboard's What-If page can render the bar chart that explains *why* the
  score is what it is.

#### Why two implementations?

Performance: the dashboard renders thousands of devices and the SQL view
pushes the math down to Postgres. The Python implementation makes the score
available online (e.g., scoring a single trip without round-tripping through
the database) and is used by the API.

We pay a small price: any change to weights or normalization caps must land
in **both** files (YAML and the SQL view). The parity test pins them
together.

### 3.2 Device-behavior clustering — KMeans pipeline

This is the only *learned* model in the system today. Full training entrypoint
is `src/accent_fleet/ml/train_clustering.py::run()`, callable as:

```bash
python scripts/train_clustering.py [--month-from 2025-01] [--no-promote]
```

#### Algorithms used

| Step | Library | Function | Why |
|---|---|---|---|
| Feature standardization | scikit-learn | `StandardScaler` | KMeans uses Euclidean distance — without scaling, large-magnitude features (e.g., `high_rpm_minutes_per_day`) would dominate small-magnitude ones (e.g., ratios) |
| Clustering | scikit-learn | `KMeans(n_init=10, random_state=42)` | Standard centroid-based partitional clustering. `n_init=10` runs k-means++ initialization 10 times and keeps the best inertia. `random_state` pinned for reproducibility |
| K selection | scikit-learn | `silhouette_score` | Picks the K that maximizes silhouette across `K ∈ {3, 4, 5, 6}` (`DEFAULT_K_RANGE = range(3, 7)`) |
| Persistence | joblib + MLflow | `joblib.dump`, `mlflow.sklearn.log_model` | Local disk fallback + remote registry |

#### Algorithm — step by step

```
load_training_frame(month_from)
    ↓
    DataFrame  shape ≈ (n_device_months, 13_features)
    [filter: total_distance_km ≥ 100 AND total_ignition_on_minutes > 0]
    ↓
_validate_tenant_coverage(df)        # see §4.1 — fails fast if 7486 missing
    ↓
fit_clustering(df)
    │
    ├── X       = df[FEATURES].fillna(0).to_numpy()
    ├── scaler  = StandardScaler().fit(X)
    ├── Xs      = scaler.transform(X)
    │
    └── For each k in range(3, 7):
            km   = KMeans(n_clusters=k, n_init=10, random_state=42).fit(Xs)
            sil  = silhouette_score(Xs, km.labels_)
            keep (km, k) if sil > best_so_far
        → returns (best_kmeans, scaler, TrainResult)
    ↓
save_local(kmeans, scaler, result)   # models/clustering/kmeans_v1.joblib + metadata.json
    ↓
log_to_mlflow(kmeans, scaler, result, promote=True)
    │
    ├── mlflow.log_params({k, random_state, n_features, n_rows, n_tenants})
    ├── mlflow.log_metric("silhouette", best_sil)
    ├── mlflow.log_metric("cluster_<i>_size", size) for each cluster
    ├── mlflow.log_dict(metadata.json)
    ├── mlflow.sklearn.log_model({"kmeans": ..., "scaler": ..., "feature_order": ...})
    └── transition_model_version_stage(... stage="Production", archive_existing_versions=True)
```

#### Why these choices?

- **KMeans, not hierarchical / DBSCAN / Gaussian Mixture** — we want a fixed
  small number of human-interpretable groups (e.g., "safe long-haul",
  "aggressive urban"), and we need a model that can score a single new
  feature vector in microseconds for the online API. DBSCAN doesn't predict
  on new points natively; hierarchical clustering scales poorly.
- **`n_init=10`** — KMeans converges to a local optimum; running 10 different
  seeds and keeping the lowest-inertia fit is the standard guard against
  getting stuck.
- **`random_state=42`** — pinned everywhere (K selection, KMeans). Same
  features in, same labels out, every time. Critical for the parity tests
  and for reproducing investigations.
- **`K ∈ {3, 4, 5, 6}`** — silhouette becomes meaningless below 3 and noisy
  above 6 for our row counts (typically a few thousand device-months).
- **Single global model (not per-tenant)** — gives us a shared
  cross-tenant vocabulary ("cluster 2 means high-overspeed urban") and
  enough rows to learn stable centroids. Per-tenant models can be added
  later as separate registered models if needed.
- **Single model artifact** — we log a dict `{kmeans, scaler, feature_order}`
  as one MLflow artifact. That way the API can never load a scaler from one
  run with a KMeans from another.

#### Outputs (artifacts produced)

| Where | Content |
|---|---|
| `models/clustering/kmeans_v1.joblib` | Pickled fitted `KMeans` |
| `models/clustering/scaler_v1.joblib` | Pickled fitted `StandardScaler` |
| `models/clustering/metadata.json` | `{version, trained_at, k, silhouette, n_rows, feature_order, cluster_sizes, tenants, rows_by_tenant, random_state, training_window}` |
| MLflow run | params, metrics, full artifact dict |
| MLflow Model Registry | `device-clustering` version `N` at stage `Production` (older versions auto-archived) |

#### Predictions

The `ClusterPredictor` class in `src/accent_fleet/ml/inference.py` is what
the API uses at runtime. Loading strategy:

1. Try MLflow registry: `models:/device-clustering/Production`.
2. Fallback to local `models/clustering/*.joblib`.
3. If both fail, stay "unloaded" — `/health` still returns 200 (so the API
   can be deployed before the first training run); `/v1/score/cluster`
   returns **503 Service Unavailable** with a "no model" message.

Single-row predict:

```python
vec    = [features.get(name, 0.0) for name in feature_order]   # in trained order!
scaled = scaler.transform([vec])
label  = kmeans.predict(scaled)[0]
dist   = kmeans.transform(scaled)[0][label]
return (cluster_id=label, distance_to_centroid=dist)
```

Vectorised batch predict (`ml/batch_scoring.py::score_partitions`) is the
same math applied to the whole `marts.v_ml_features_full` frame for a set of
touched months — ~1000× faster than per-row calls — then upserted into
`marts.fact_device_cluster_assignment` via **delete-then-insert** so the
dashboard never sees mixed-version cluster ids.

### 3.3 Where the prediction endpoints live

| Endpoint | Function | Returns |
|---|---|---|
| `POST /v1/score/risk` | `score_risk` in `app/routes/score.py` | `{risk_score, category, components, version}` |
| `POST /v1/score/cluster` | `score_cluster` in `app/routes/score.py` | `{cluster_id, distance, model_name, model_version}` |

Both accept a free-form `FeatureVector` (missing keys default to 0, matching
SQL `COALESCE` semantics). Both require a bearer JWT post-M3.

---

## 4. Evaluation Phase

Evaluation runs at **three** different cadences, each catching a different
class of failure:

| When | What we check | Where the code lives |
|---|---|---|
| Every training run | Silhouette ≥ previous, K stable, tenant coverage | `ml/train_clustering.py` + manual review of MLflow run |
| Every commit / CI | SQL risk score ≡ Python risk score | `tests/test_feature_computation.py` |
| Every batch scoring | Cluster sizes monitor, model version stamped on every row | `ml/batch_scoring.py` |

### 4.1 Tenant coverage gate — `_validate_tenant_coverage`

Before any clustering math runs, `load_training_frame` calls
`_validate_tenant_coverage(df)`. It compares the tenants actually present in
the training frame against two lists from `config/pipeline.yaml`:

- **`expected_tenants`** — should be there in a healthy run. Missing one is a
  warning (logged, not fatal).
- **`required_tenants`** — must be there. Missing one **raises ValueError**
  and aborts training.

This catches an entire class of silent regressions: e.g., if tenant 7486's
telemetry-to-trip reconstruction job didn't run, the device-months for that
tenant would silently drop from the activity-filtered frame and the new
clusters would shift to fit the remaining tenants. The error message
literally tells you what to fix:

```
training frame is missing required tenant(s) [7486]. Tenant 7486 depends on
telemetry-to-trip reconstruction; run scripts/reconstruct_telemetry_trips.py
--tenant-id 7486 --from-month 2025-01, then rebuild the marts.
```

### 4.2 Silhouette score — the K-selection metric

For each candidate K ∈ {3, 4, 5, 6}, we compute:

```
silhouette(i)  =  (b(i) - a(i)) / max(a(i), b(i))
```

where:
- `a(i)` = mean distance from point `i` to other points in its own cluster.
- `b(i)` = mean distance from point `i` to points in the nearest other cluster.

The overall silhouette is the mean of `silhouette(i)` over all points; it
lives in [-1, +1]:

| Range | Interpretation |
|---|---|
| ≥ 0.50 | Strong, well-separated clusters |
| 0.25 – 0.50 | Reasonable structure |
| 0.00 – 0.25 | Weak / overlapping — borderline |
| < 0 | Wrong K (points are on average closer to a different cluster than their own) |

The K with the highest silhouette is the one shipped. This is logged as the
MLflow metric `silhouette` on every run — you can see it in the MLflow UI
at `http://localhost:5000` and use the **Compare runs** view to confirm a
new run hasn't regressed.

### 4.3 Cluster-size sanity check

`metadata.json` and MLflow both record `cluster_sizes = {0: n0, 1: n1, ...}`.
A healthy run produces roughly comparable cluster sizes (no cluster <1% of
the population). A degenerate run typically shows one giant "everything-else"
cluster and 2-3 micro-clusters — the silhouette will catch this most of the
time, but the size dict is the human-readable confirmation.

### 4.4 SQL ≡ Python parity test

`tests/test_feature_computation.py` materializes a tiny synthetic
`marts.mart_device_monthly_behavior` slice, computes the risk score two
ways:

1. By querying `marts.v_device_risk_profile` (the SQL twin).
2. By calling `RiskScorer.score(features)` on the same rows.

…and asserts both produce **exactly** the same integer score. This is the
invariant that lets us trust the per-factor breakdown the API returns — it's
the same math the dashboard sees, just instrumented.

### 4.5 Model-version stamping (post-hoc evaluation)

Every row written to `marts.fact_device_cluster_assignment` carries the
`model_version` that produced it. So we can:

- Backfill comparisons: "for July 2026, how did v3 vs v4 assign tenant 235?"
- Detect stale partitions: "any month where model_version ≠ current Production?"
- Investigate user complaints: "why did device X's cluster change?" — answer
  is in the version column, not lost to history.

### 4.6 Promotion strategy

`scripts/train_clustering.py` accepts `--no-promote`:

```bash
python scripts/train_clustering.py --no-promote
```

When `--no-promote` is set, the new version is **registered** in MLflow but
**not** transitioned to `Production`. The API keeps serving the previous
Production version. An operator can then:

1. Open MLflow UI, compare silhouette + cluster sizes against the current
   Production run.
2. Manually transition stages via the UI (or via the MLflow CLI) if the new
   run looks healthy.
3. Hit `POST /v1/admin/reload-model` on the API to pick up the new artifact
   without a restart (`ClusterPredictor.reload()`).

This is the recommended path for any out-of-cycle retraining. The monthly
retrain (driven by `docker/retrain.crontab` via the `retrain-scheduler`
service, when run with `--profile scheduler`) currently auto-promotes; the
roadmap calls this out as a place to add a silhouette-regression gate
("don't promote if `new_silhouette < previous_silhouette - 0.02`") before
production traffic depends on it.

---

## 5. End-to-end flow — putting it together

```
                       ┌─────────────────────────────────────────────┐
                       │           marts.v_ml_features_full           │
                       │   one row per (tenant, device, year_month)    │
                       │   13 clustering features + 6 risk inputs +    │
                       │   all 35 baseline cols (audit / drill-down)  │
                       └────────────────────┬─────────────────────────┘
                                            │
              ┌─────────────────────────────┼──────────────────────────────┐
              │                             │                              │
              ▼                             ▼                              ▼
   ┌──────────────────────┐    ┌──────────────────────────┐    ┌─────────────────────────┐
   │   RISK SCORE (rule)   │    │  CLUSTERING (KMeans)     │    │  BATCH SCORING (ETL)    │
   │                       │    │                          │    │                         │
   │  YAML weights         │    │  StandardScaler          │    │  ml/batch_scoring.py    │
   │  ↓                    │    │  ↓                       │    │  loads predictor once,  │
   │  normalize (linear/   │    │  KMeans(n_init=10,       │    │  vectorised .transform/ │
   │   identity)           │    │     random_state=42)     │    │  .predict over month    │
   │  ↓                    │    │  ↓                       │    │  ↓                      │
   │  weighted sum         │    │  silhouette over K=3..6  │    │  DELETE + INSERT into   │
   │  ↓                    │    │  ↓                       │    │  fact_device_cluster_   │
   │  category bucket      │    │  best (kmeans, scaler)   │    │  assignment             │
   │                       │    │  ↓                       │    │                         │
   │  Used by:             │    │  joblib + MLflow         │    │  Dashboard reads from   │
   │   - SQL view          │    │  registry @ Production   │    │  the fact, never the    │
   │   - Python (API)      │    │                          │    │  API — page loads fast  │
   └──────────────────────┘    └────────────┬─────────────┘    └─────────────────────────┘
                                            │
                                            │ at API runtime
                                            ▼
                              ┌──────────────────────────────┐
                              │   ClusterPredictor (lazy)    │
                              │                              │
                              │   load from MLflow → joblib  │
                              │   fallback → 503             │
                              │                              │
                              │   POST /v1/score/cluster     │
                              │     {cluster_id, distance,   │
                              │      model_version}          │
                              └──────────────────────────────┘
```

---

## 6. Cheat sheet — common questions

**Q: What's the model's input?**
The 13 clustering features listed in `FEATURES` (`ml/train_clustering.py`),
pulled from `marts.v_ml_features_full`, filtered to device-months with
≥100 km distance and engine-on time > 0. Missing values default to 0.

**Q: What's the model's output?**
`(cluster_id ∈ {0..K-1}, distance_to_centroid: float)`. K is chosen
automatically per training run (typically 4 or 5).

**Q: What's the risk score's input?**
A feature dict containing (at minimum) `overspeed_per_100km`,
`overspeed_count`, `overspeed_severity_high`, `overspeed_severity_extreme`,
`high_speed_trip_ratio`, `speed_alert_per_100km`, `night_trip_ratio`,
`avg_max_speed_kmh`. The SQL view sources these from the rolling 3-month
aggregation of `marts.mart_device_monthly_behavior`.

**Q: What's the risk score's output?**
`(risk_score ∈ [0, 100], category ∈ {low, moderate, high, critical},
components: {factor_name: weighted_contribution})`.

**Q: How is the model "validated"?**
Three layers: tenant-coverage gate at training time (fails fast on missing
required tenants), silhouette score for K selection (logged to MLflow on
every run), and SQL≡Python parity test for the risk score (runs in CI).

**Q: How often is the model retrained?**
Manually on demand via `python scripts/train_clustering.py`, or once per
month on the first Monday at 04:00 UTC when the `retrain-scheduler`
compose profile is active (`docker compose --profile scheduler up -d`).

**Q: How do you roll back a bad model version?**
MLflow UI → transition the previous version back to `Production` (it'll
auto-archive the bad one) → call `POST /v1/admin/reload-model` on the
API. The `model_version` column in `fact_device_cluster_assignment` lets
you find which months need re-scoring.

---

## 7. Pointers

- Training entrypoint — `scripts/train_clustering.py`
- Training code — `src/accent_fleet/ml/train_clustering.py`
- Inference — `src/accent_fleet/ml/inference.py`
- Batch scoring — `src/accent_fleet/ml/batch_scoring.py`
- API routes — `app/routes/score.py`
- Risk score (Python) — `src/accent_fleet/features/risk_score.py`
- Risk score (SQL) — `sql/21_v_device_risk_profile.sql`
- Feature contract (SQL) — `sql/26_v_ml_features_full.sql`
- Feature/risk config — `config/feature_definitions.yaml`
- Parity test — `tests/test_feature_computation.py`
- Project overview — `docs/HOW_IT_WORKS.md`
