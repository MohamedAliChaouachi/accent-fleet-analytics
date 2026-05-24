# Modeling & Evaluation — Deep Dive

> Companion to [`docs/HOW_IT_WORKS.md`](HOW_IT_WORKS.md) and
> [`docs/DATA_PREPARATION.md`](DATA_PREPARATION.md). This document zooms into
> the **Modeling** and **Evaluation** phases of the CRISP-DM cycle for Accent
> Fleet Analytics: exactly which algorithms run, what they consume, what they
> produce, and how we decide whether a freshly trained model is good enough
> to ship.
>
> Reading order: data-prep doc explains how the marts get built; this doc
> explains what the models do with them and how we keep them honest.

---

## 0. CRISP-DM scope of this phase

In CRISP-DM, **Modeling** is "select modeling techniques, generate a test
design, build the model, assess the model"; **Evaluation** is "evaluate
results against business goals, review the process, determine next steps."
In this project that maps to:

| CRISP-DM sub-task                | Where it lives in the repo |
|---|---|
| Select modeling technique        | `src/accent_fleet/ml/train_clustering.py` (KMeans), `src/accent_fleet/ml/train_risk.py` (per-tenant Isolation Forest) |
| Generate test design             | `tests/test_train_clustering.py`, `tests/test_train_risk.py`, `tests/test_feature_computation.py`, `tests/test_inference.py` |
| Build the model                  | `scripts/train_clustering.py`, `scripts/train_risk_score.py` (CLIs), `scripts/retrain_monthly.py`, `scripts/retrain_risk_monthly.py` (gated flows) |
| Assess the model                 | Silhouette (clustering k-selection), per-tenant risk category mix + score PSI (risk stability), cluster parity, SQL ≡ Python parity for the risk fact |
| Approved model                   | MLflow registries `device-clustering` and `device-risk-score` at stage `Production`; `models/clustering/*.joblib` and `models/risk_score/risk_v1.joblib` as fallback |
| Review process / next steps      | `src/accent_fleet/ml/promotion.py` (`decide_promotion` silhouette gate + `decide_risk_promotion` stability gate), `src/accent_fleet/ml/drift.py` (PSI monitor + `compute_score_drift` for the risk gate) |

The phase consumes the feature view produced by data-prep
(`marts.v_ml_features_full`) and writes back into
`marts.fact_device_cluster_assignment` (KMeans labels) and
`marts.fact_device_risk_score` (per-tenant Isolation Forest scores +
categories) plus the MLflow registry. The legacy `marts.v_device_risk_profile`
view is preserved as a thin compat view that now sources from
`fact_device_risk_score` — dashboards that already read the view keep
working without code changes.

---

## 1. The two models in the system

Accent Fleet Analytics is **not** a single big model. It is **two
independent unsupervised ML systems** that share the same monthly feature
mart but answer different questions:

| Model                          | Type                                                    | Question it answers                                            | Output                                                                                  |
|---|---|---|---|
| **Device Risk Score**          | Per-tenant Isolation Forest (unsupervised anomaly)      | "How unusual is this device's driving for this tenant's fleet?" | `risk_score` ∈ [0, 100] + category (`low` / `moderate` / `high` / `critical`) + per-feature z-score components |
| **Device-Behavior Clustering** | Global KMeans (unsupervised partitioning)                | "Which behavioural archetype does this device belong to?"      | `cluster_id` ∈ {0, …, K-1} + Euclidean `distance_to_centroid`                           |

Both are unsupervised — we have no ground-truth "this driver crashed"
labels, so we can't train a supervised classifier. Anomaly detection +
clustering let us extract operational signal anyway, with the trade-off
that "high score" means *unusual* for the tenant's fleet, not *causally
dangerous*. The dashboard surfaces the score next to its driver
components (per-feature z-scores) so a fleet manager can see *why* a
device flagged anomalous instead of taking the number on faith.

The risk score is fitted **per tenant** because the baseline behaviour
distribution varies a lot between fleets (a delivery van in Sousse and a
school bus in Tunis don't share a "normal"). The clustering model is
**global** by design — we want a shared cross-tenant vocabulary
("cluster 2 means high-overspeed urban") and enough rows to learn stable
centroids.

Each model runs in two places that **must agree numerically**:

- **In-database** — `marts.fact_device_cluster_assignment` and
  `marts.fact_device_risk_score`, written by the Prefect batch flow
  after each marts refresh. Dashboards read pre-computed scores from
  the facts — no per-row API roundtrip.
- **In-process** — Python (`ml/inference.py::ClusterPredictor`,
  `ml/inference.py::RiskPredictor`) called by the FastAPI service for
  ad-hoc and online scoring of vectors that aren't yet in the mart.

Numerical parity is pinned by two tests in
[`tests/test_feature_computation.py`](../tests/test_feature_computation.py):

- **`test_cluster_predictor_matches_persisted_assignment`** — pulls up to
  25 rows from `marts.fact_device_cluster_assignment` filtered to the
  **same `model_version`** the predictor currently loads, then re-runs
  `ClusterPredictor.predict()` on the matching features and asserts the
  cluster id matches.
- **`test_risk_predictor_matches_persisted_fact`** — pulls up to 25 rows
  from `marts.fact_device_risk_score` (same `model_version` filter),
  re-runs `RiskPredictor.predict()` on the matching features, and asserts
  `|py − sql| ≤ 0.1` per row (small floor for IF decision-function
  numerics). Tenants with no per-tenant model in the loaded artifact are
  counted in `skipped_tenant_rows` rather than failing the test — they
  represent the same "no model for this tenant yet" state the API
  surfaces as 503.

---

## 2. Inputs — the feature contract

### 2.1 The feature mart

Both models read from the same view:
**`marts.v_ml_features_full`** (definition in
[`sql/26_v_ml_features_full.sql`](../sql/26_v_ml_features_full.sql)).

Grain: **one row per `(tenant_id, device_id, year_month)`**.

The view is a `LEFT JOIN` of:

- `marts.v_ml_features_driver_behavior` — 35 trip-derived feature columns
  (overspeed, alerts, trip volumes, temporal patterns).
- `marts.mart_device_monthly_telemetry` — 22 archive-derived columns (harsh
  events, idling, RPM).

`LEFT JOIN` (not inner) ensures devices with no archive pings still produce
a feature row — telemetry columns just come out as `0` / `NULL`.

### 2.2 Activity gates

Two different gates apply depending on the consumer:

| Gate                                              | Where                                                                                                   | Condition                                                                          |
|---|---|---|
| **Clustering training**                           | `src/accent_fleet/ml/train_clustering.py::load_training_frame`                                          | `year_month >= :month_from` AND `total_distance_km >= 100` AND `total_ignition_on_minutes > 0` |
| **Risk-model training (per tenant)**              | `src/accent_fleet/ml/train_risk.py::load_training_frame` + `fit_risk_model`                             | Same activity filter as clustering; **plus** `min_rows_per_tenant` (default 50) on the per-tenant groupby — tenants below threshold land in `skipped_tenants` |
| **Risk-profile compat view**                      | `sql/21_v_device_risk_profile.sql`                                                                      | Reads `fact_device_risk_score` directly — no filtering of its own; the activity filter happened upstream at scoring time |
| **Driver-behaviour view (feeds both)**            | `sql/v_ml_features_driver_behavior.sql` (via `minimum_trips_for_scoring` in `config/feature_definitions.yaml`) | `total_trips >= 5` (per month, pre-aggregate)                                       |
| **Minimum frame size for KMeans fit**             | `src/accent_fleet/ml/train_clustering.py::fit_clustering`                                               | `len(df) >= MIN_ROWS` (50)                                                          |

Devices below these thresholds are dropped — they don't have enough signal
to cluster or score meaningfully and would just collapse into a degenerate
"barely-driven" cluster or produce a noisy risk number.

### 2.3 Feature lists

#### Clustering — 13 features (`FEATURES` tuple in `ml/train_clustering.py:40-54`)

These were picked because they span **four behavioural axes** the
clustering is meant to separate:

| Axis                         | Features                                                                                                              |
|---|---|
| Overspeed behaviour          | `overspeed_per_100km`, `avg_speed_over_limit`, `high_speed_trip_ratio`, `speed_alert_per_100km`                       |
| Harsh driving                | `harsh_brake_per_100km`, `harsh_accel_per_100km`, `harsh_corner_per_100km`                                            |
| Engine / efficiency          | `monthly_idle_ratio`, `high_rpm_minutes_per_day`                                                                       |
| Exposure / pattern           | `night_trip_ratio`, `rush_hour_trip_ratio`, `stddev_trip_distance`, `short_trip_ratio`                                 |

Missing values become `0` (matches the SQL view's `COALESCE` semantics).

#### Risk score — 13 features (same as clustering)

The Isolation Forest is fitted on the **same 13 features** as the
clustering model. This is deliberate — both models read driver behaviour
from a single mart and the feature set has been validated to span the
four operational axes above (overspeed, harsh driving, engine/efficiency,
exposure/pattern). Using one feature surface keeps marts narrow and the
parity logic uniform.

`config/feature_definitions.yaml::risk_score_model.features` lists the
canonical names; `ml/train_risk.py::FEATURES` re-declares them as a tuple
so other modules (drift, batch scoring, inference) can import the constant
without round-tripping through YAML. The two must agree — the test
`test_risk_defaults_match_yaml_documented_values` pins the IF
hyperparameters and the gate-tolerance defaults so a careless YAML edit
fails fast.

There are **no per-factor weights** — the model learns the structure
itself from the multivariate distribution of feature values for each
tenant. The dashboard's "What-If" page renders per-feature **z-scores**
(`(x − μ_tenant) / σ_tenant` for the tenant's fitted scaler) as
"components" so a fleet manager can still see *which* features drove a
device into the anomalous tail, even though no single weight controls
that contribution. The components are returned by
`POST /v1/score/risk` alongside the score and category.

### 2.4 Tenant cohort

`config/pipeline.yaml::modeling` declares which tenants the training run
expects to see:

```yaml
modeling:
  expected_tenants: [235, 238, 264, 1787, 7486]
  required_tenants: [7486]
```

- **`expected_tenants`** — should be present in a healthy run. Missing one
  is a warning (logged, not fatal).
- **`required_tenants`** — must be present. Missing one **raises
  `ValueError`** and aborts training.

The split exists because tenant **7486** is telemetry-first in the source
system: its rows only land in the feature view after
`scripts/reconstruct_telemetry_trips.py` has run. If that step silently
breaks, the activity-filtered frame loses 7486 and the new clusters drift
to fit the remaining four tenants. The error message tells the operator
exactly what to fix:

```
training frame is missing required tenant(s) [7486]. Tenant 7486 depends on
telemetry-to-trip reconstruction; run scripts/reconstruct_telemetry_trips.py
--tenant-id 7486 --from-month 2025-01, then rebuild the marts.
```

This check is unit-tested by `tests/test_train_clustering.py`
(`test_required_modeling_tenant_missing_raises`).

---

## 3. Modeling Phase

### 3.1 Risk score — per-tenant Isolation Forest

The risk score is a learned **unsupervised anomaly score**, fitted per
tenant on the same 13 driver-behaviour features as the clustering model.
Everything that defines its behaviour lives in
`config/feature_definitions.yaml` under the `risk_score_model:` key.

Full training entrypoint is `src/accent_fleet/ml/train_risk.py::run()`,
callable as:

```bash
python scripts/train_risk_score.py [--month-from 2025-01] [--no-promote]
```

#### Algorithms used

| Step                    | Library      | Function                                  | Why |
|---|---|---|---|
| Per-tenant standardization | scikit-learn | `StandardScaler` (fitted per tenant) | IF uses axis-aligned splits and is theoretically scale-invariant, but the inference path returns **per-feature z-scores as components** — those need a fitted scaler. Fitting the scaler per tenant also keeps the components meaningful: "1.5σ above this tenant's mean idle ratio" travels well to the UI. |
| Anomaly detection       | scikit-learn | `IsolationForest(n_estimators=200, contamination="auto", random_state=42)` | Tree-ensemble that isolates points by random axis splits; few splits = anomalous. Fast to fit (linear in n), fast to predict (log-depth), and degrades gracefully when fed unseen rows at inference time. No assumption of normality. |
| Score rescaling         | NumPy        | linear map `raw → [0, 100]` using fit-time min/max | The raw `-decision_function` output is unbounded and unitless. Rescaling to 0–100 with the **fit-time bounds** preserved on the artifact gives the API a stable, dashboard-friendly range. Values outside the fit window are clipped (D2 in the design notes) so a wildly anomalous live row reads as 100, not 137. |
| Category thresholds     | NumPy        | per-tenant quantiles (50 / 80 / 95 by default) | Banding on quantiles means each tenant's category split adapts to its own distribution — a fleet where everyone is moderately fast doesn't have its whole population flagged "critical". The dashboards show "top 5% within your fleet", which is the operationally meaningful framing. |
| Persistence             | joblib + MLflow | one bundled artifact dict per registry version | All per-tenant scalers + models + thresholds + fit-time bounds in **one** dict so the registry version is atomic — rollback restores every tenant's model together, never a mix. |

#### Algorithm — step by step

```
load_training_frame(month_from)
    ↓
    DataFrame  shape ≈ (n_device_months, 13_features + ids)
    [filter: year_month >= month_from
             AND total_distance_km >= 100
             AND total_ignition_on_minutes > 0]
    ↓
_validate_tenant_coverage(df)        # same fail-fast as clustering — 7486 required
    ↓
fit_risk_model(df)
    │
    └── For each tenant in df.groupby("tenant_id"):
            sub = rows for this tenant
            if len(sub) < min_rows_per_tenant (default 50):
                skipped_tenants[tenant_id] = "below_min_rows (N < 50)"
                continue

            X       = sub[FEATURES].fillna(0).to_numpy(dtype=float)
            scaler  = StandardScaler().fit(X)
            Xs      = scaler.transform(X)
            iso     = IsolationForest(n_estimators=200,
                                       contamination="auto",
                                       random_state=42).fit(Xs)
            raw     = -iso.decision_function(Xs)         # higher = more anomalous
            raw_min, raw_max = raw.min(), raw.max()
            scores  = rescale_raw_to_0_100(raw, raw_min, raw_max)
            t_m, t_h, t_c = quantile_thresholds(scores, (0.50, 0.80, 0.95))
            labels  = categorize_scores(scores, (t_m, t_h, t_c))

            artifact["tenants"][tenant_id] = {
                "scaler":          scaler,
                "model":           iso,
                "raw_min":         raw_min,
                "raw_max":         raw_max,
                "thresholds":      {"moderate": t_m, "high": t_h, "critical": t_c},
                "feature_means":   scaler.mean_,         # for component z-scores
                "feature_stds":    scaler.scale_,
                "n_rows":          len(sub),
                "score_share":     _share(labels),       # gate input
            }
        → returns (artifact_dict, TrainResult)
    ↓
save_local(artifact, result)         # models/risk_score/risk_v1.joblib + metadata.json
    ↓
log_to_mlflow(artifact, result, promote=True)
    │
    ├── mlflow.log_params({algorithm, n_estimators, contamination, random_state,
    │                       n_features, n_tenants, n_rows, min_rows_per_tenant})
    ├── mlflow.log_metric(f"overall_share_{cat}", frac) for each category
    ├── mlflow.log_metric(f"tenant_{tid}_share_critical", ...)  for diagnostics
    ├── mlflow.log_dict({feature_order, tenants, skipped_tenants,
    │                     overall_share, rows_by_tenant}, "metadata.json")
    ├── mlflow.sklearn.log_model(artifact,                    # the bundled dict
    │                             registered_model_name="device-risk-score")
    └── if promote:
          client.transition_model_version_stage(
              name="device-risk-score",
              version=latest.version,
              stage="Production",
              archive_existing_versions=True,
          )
```

#### Why these choices?

- **Isolation Forest, not One-Class SVM / LOF / autoencoder** — IF is the
  closest match to "unusual within this fleet's distribution" without
  the parametric assumptions of OCSVM, the O(n²) memory cost of LOF, or
  the labelled-data appetite of a deep model. It also returns a smooth
  anomaly score (not just a binary label), which is what the dashboard
  bands need.
- **`contamination="auto"`** — we don't know the prior probability of
  "anomalous" for a fleet. Letting sklearn pick the offset from the
  training score distribution avoids hard-coding a fraction that would
  be wrong half the time. The bands come from quantiles downstream
  anyway, so the contamination value mostly affects the raw score scale
  before our rescale.
- **`random_state=42`, `n_estimators=200`** — pinned everywhere. Same
  features in, same scores out, every time. 200 trees is the
  sklearn-default sweet spot for IF stability without paying for a tail
  of barely-contributing trees.
- **Per-tenant, not global** — each tenant's "normal" is different.
  Tenant 235 (delivery vans) and tenant 7486 (school buses) have
  different speed profiles, idle patterns, and night-driving rates.
  A global model trained on the union would mostly flag the smaller
  tenants as anomalous because their data lives on the tails. Per-tenant
  fits also let the gate compare apples-to-apples: each retrain's
  category mix is contrasted against the same tenant's prior mix.
- **Single bundled artifact (one dict, all tenants)** — same atomicity
  story as clustering. The dashboard, batch scoring, and the API all
  load one registry version that's either fully consistent or fully
  rolled back. There's no path where tenant 235's scaler from v3 ends
  up paired with tenant 235's IF from v4.
- **Score bounds clipped at fit time** — values above `raw_max` rescale
  to 100, below `raw_min` to 0. The API contract says
  `risk_score ∈ [0, 100]`; without clipping, an unusually-anomalous
  live row would breach the contract and the dashboard's chart axes.

#### Outputs (artifacts produced)

| Where                                              | Content                                                                                                                                |
|---|---|
| `models/risk_score/risk_v1.joblib`                 | Pickled bundled dict `{tenants: {tid: {scaler, model, raw_min/max, thresholds, …}}, feature_order, config}`                              |
| `models/risk_score/metadata.json`                  | `{version, trained_at, model_name, feature_order, tenants, rows_by_tenant, skipped_tenants, tenant_stats, overall_share, n_rows, …}`     |
| MLflow run                                         | params, `overall_share_{cat}` metrics, per-tenant `tenant_{tid}_share_critical` / `_high` / `_n_rows` metrics, full artifact dict        |
| MLflow Model Registry                              | `device-risk-score` version `N` at stage `Production` (older Production versions auto-archived) when `promote=True`                     |

#### Predictions

The `RiskPredictor` class in
[`src/accent_fleet/ml/inference.py`](../src/accent_fleet/ml/inference.py)
is what the API uses at runtime. Loading strategy mirrors `ClusterPredictor`:

1. Try MLflow registry:
   `models:/device-risk-score/Production`.
2. Fallback to local `models/risk_score/risk_v1.joblib`.
3. If both fail, stay "unloaded" — `/health` still returns 200 (so the
   API can be deployed before the first training run);
   `/v1/score/risk` then returns **503 Service Unavailable** with a
   "no model" message.

Single-row predict:

```python
entry         = artifact["tenants"][tenant_id]       # raises TenantModelMissing if absent
vec           = np.asarray([[float(features.get(name) or 0.0)
                              for name in feature_order]], dtype=float)
scaled        = entry["scaler"].transform(vec)
raw           = -entry["model"].decision_function(scaled)
score_0_100   = rescale_raw_to_0_100(raw, entry["raw_min"], entry["raw_max"])[0]
category      = categorize_scores(np.array([score_0_100]), thresholds)[0]
components    = {name: (vec[0][i] - mean[i]) / std[i]
                 for i, name in enumerate(feature_order)}
return RiskPrediction(score=score_0_100, category=category, components=components, …)
```

`tenant_id` is **required** on the API request — a vector without a
tenant cannot be routed to a model. A request for a tenant the loaded
artifact doesn't know returns **503** with a message naming the missing
tenant; the dashboard surfaces this as "feature not ready for this
tenant" rather than a generic 5xx alarm.

#### The compat view — why `v_device_risk_profile` still exists

The dashboard reads `marts.v_device_risk_profile` for the per-device
drill-down. In v0.6 we redirected that view to source from
`marts.fact_device_risk_score` directly, so the dashboard code didn't
change but the numbers it shows are now IF-driven. The view exists for
two reasons:

1. It keeps an existing SQL surface stable across the migration — any
   dashboard, notebook, or `psql` query that already reads
   `v_device_risk_profile` keeps working.
2. It centralizes the join from the raw fact to the rolling 3-month
   window the device drill-down expects, so callers don't each
   reimplement that window themselves.

Definition: see [`sql/21_v_device_risk_profile.sql`](../sql/21_v_device_risk_profile.sql).

### 3.2 Device-behavior clustering — KMeans pipeline

This is the only *learned* model in the system today. Full training
entrypoint is `src/accent_fleet/ml/train_clustering.py::run()`, callable as:

```bash
python scripts/train_clustering.py [--month-from 2025-01] [--no-promote]
```

#### Algorithms used

| Step                    | Library      | Function                                  | Why |
|---|---|---|---|
| Feature standardization | scikit-learn | `StandardScaler`                          | KMeans uses Euclidean distance — without scaling, large-magnitude features (e.g., `high_rpm_minutes_per_day`) would dominate small-magnitude ones (e.g., ratios) |
| Clustering              | scikit-learn | `KMeans(n_init=10, random_state=42)`      | Standard centroid-based partitional clustering. `n_init=10` runs k-means++ initialization 10 times and keeps the best inertia. `random_state` pinned for reproducibility |
| K selection             | scikit-learn | `silhouette_score`                        | Picks the K that maximizes silhouette across `K ∈ {3, 4, 5, 6}` (`DEFAULT_K_RANGE = range(3, 7)`) |
| Persistence             | joblib + MLflow | `joblib.dump`, `mlflow.sklearn.log_model` | Local disk fallback + remote registry |

#### Algorithm — step by step

```
load_training_frame(month_from)
    ↓
    DataFrame  shape ≈ (n_device_months, 13_features + ids)
    [filter: year_month >= month_from
             AND total_distance_km >= 100
             AND total_ignition_on_minutes > 0]
    ↓
_validate_tenant_coverage(df)        # see §2.4 — fails fast if 7486 missing
    ↓
fit_clustering(df)                   # requires len(df) >= MIN_ROWS (50)
    │
    ├── X       = df[FEATURES].fillna(0).to_numpy(dtype=float)
    ├── scaler  = StandardScaler().fit(X)
    ├── Xs      = scaler.transform(X)
    │
    └── For each k in range(3, 7):
            if k >= len(Xs): continue
            km   = KMeans(n_clusters=k, n_init=10, random_state=42).fit(Xs)
            sil  = silhouette_score(Xs, km.labels_)
            keep (km, k) if sil > best_so_far
        → returns (best_kmeans, scaler, TrainResult)
    ↓
save_local(kmeans, scaler, result)   # models/clustering/{kmeans_v1,scaler_v1}.joblib
                                     # + metadata.json
    ↓
log_to_mlflow(kmeans, scaler, result, promote=True)
    │
    ├── mlflow.log_params({k, random_state, n_features, n_rows, n_tenants})
    ├── mlflow.log_metric("silhouette", best_sil)
    ├── mlflow.log_metric(f"cluster_{i}_size", size) for each cluster
    ├── mlflow.log_dict({feature_order, cluster_sizes}, "metadata.json")
    ├── mlflow.sklearn.log_model({"kmeans": ..., "scaler": ...,
    │                              "feature_order": ...},
    │                             registered_model_name=settings.mlflow_model_name)
    └── if promote:
          client.transition_model_version_stage(
              name=settings.mlflow_model_name,
              version=latest.version,
              stage=settings.mlflow_model_stage,    # default "Production"
              archive_existing_versions=True,
          )
```

#### Why these choices?

- **KMeans, not hierarchical / DBSCAN / Gaussian Mixture** — we want a
  fixed small number of human-interpretable groups (e.g., "calm city",
  "aggressive urban", "night owl"), and we need a model that can score a
  single new feature vector in microseconds for the online API. DBSCAN
  doesn't predict on new points natively; hierarchical clustering scales
  poorly.
- **`n_init=10`** — KMeans converges to a local optimum; running 10
  different seeds and keeping the lowest-inertia fit is the standard
  guard against getting stuck.
- **`random_state=42`** — pinned everywhere (K selection, KMeans). Same
  features in, same labels out, every time. Critical for the parity
  tests and for reproducing investigations.
- **`K ∈ {3, 4, 5, 6}`** — silhouette becomes meaningless below 3 and
  noisy above 6 for our row counts (typically a few thousand
  device-months).
- **Single global model (not per-tenant)** — gives us a shared
  cross-tenant vocabulary ("cluster 2 means high-overspeed urban") and
  enough rows to learn stable centroids. Per-tenant models can be added
  later as separately registered models if needed.
- **Single bundled artifact** — we log a dict
  `{kmeans, scaler, feature_order}` as one MLflow artifact. That way the
  API can never load a scaler from one run with a KMeans from another.

#### Outputs (artifacts produced)

| Where                                              | Content                                                                                                                                |
|---|---|
| `models/clustering/kmeans_v1.joblib`              | Pickled fitted `KMeans`                                                                                                                |
| `models/clustering/scaler_v1.joblib`              | Pickled fitted `StandardScaler`                                                                                                        |
| `models/clustering/metadata.json`                  | `{version, trained_at, k, silhouette, n_rows, feature_order, cluster_sizes, tenants, rows_by_tenant, random_state, training_window}`   |
| MLflow run                                         | params, `silhouette` metric, one `cluster_<i>_size` metric per cluster, full artifact dict                                              |
| MLflow Model Registry                              | `device-clustering` version `N` at stage `Production` (older Production versions auto-archived) when `promote=True`                     |

#### Predictions

The `ClusterPredictor` class in
[`src/accent_fleet/ml/inference.py`](../src/accent_fleet/ml/inference.py)
is what the API uses at runtime. Loading strategy:

1. Try MLflow registry: `models:/{settings.mlflow_model_name}/{settings.mlflow_model_stage}`
   (defaults: `device-clustering` / `Production`).
2. Fallback to local `models/clustering/*.joblib` + `metadata.json`.
3. If both fail, stay "unloaded" — `/health` still returns 200 (so the API
   can be deployed before the first training run); `/v1/score/cluster`
   then returns **503 Service Unavailable** with a "no model" message.

Single-row predict:

```python
vec    = np.asarray([[float(features.get(name) or 0.0)
                      for name in feature_order]], dtype=float)
scaled = scaler.transform(vec)
label  = int(kmeans.predict(scaled)[0])
dist   = float(kmeans.transform(scaled)[0][label])
return (cluster_id=label, distance_to_centroid=dist)
```

Vectorised batch predict
([`ml/batch_scoring.py::score_partitions`](../src/accent_fleet/ml/batch_scoring.py))
is the same math applied to the whole `marts.v_ml_features_full` frame
for a set of touched months — ~1000× faster than per-row calls — then
upserted into `marts.fact_device_cluster_assignment` via
**delete-then-insert**. Why not `ON CONFLICT`?

1. Rows whose `(tenant, device)` disappeared from a month should also
   disappear from the assignment table. `ON CONFLICT` can't express that.
2. After a model swap, every row's `cluster_id` needs to be invalidated
   atomically — delete-then-insert guarantees the dashboard never sees a
   partition with mixed-version cluster ids.
3. The fact is small (one row per active device-month) so the delete cost
   is negligible.

Both writes happen inside a single transaction so a partial failure
leaves the prior state intact. Every row stamps `model_version`,
`model_source`, and `_etl_run_id` for post-hoc auditing.

### 3.3 Where the prediction endpoints live

| Endpoint                  | Function                            | Returns                                                                  |
|---|---|---|
| `POST /v1/score/risk`     | `score_risk` in `app/routes/score.py` | `{risk_score, category, components, version}`                          |
| `POST /v1/score/cluster`  | `score_cluster` in `app/routes/score.py` | `{cluster_id, distance, model_name, model_version}`                  |
| `POST /v1/admin/reload-model` | `reload_model` in `app/routes/admin.py` | `{status, model_version, source}` — forces `ClusterPredictor.reload()` |

Both `/score/*` accept a free-form `FeatureVector` (missing keys default to
0, matching SQL `COALESCE` semantics). Both go through `AuthMiddleware`
and require a bearer JWT in production. `/admin/reload-model` uses a
separate **`X-API-Key`** header (`settings.api_admin_key`) and fails
closed if no key is configured.

The routers are mounted via `include_versioned_router` in
[`app/versioning.py`](../app/versioning.py), so the same handlers also
serve the legacy paths `/score/*` and `/admin/*` with
`Deprecation: true`, `Sunset: Sat, 31 Oct 2026 23:59:59 GMT`, and
`Link: </v1>; rel="successor-version"` response headers.

---

## 4. Evaluation Phase

Evaluation runs at **four** different cadences, each catching a different
class of failure:

| When                       | What we check                                                | Where the code lives                                    |
|---|---|---|
| Every training run         | Required tenant coverage; silhouette across K                | `ml/train_clustering.py::_validate_tenant_coverage`, `fit_clustering` |
| Every commit / CI          | Risk-score SQL ≡ Python; promotion-gate decision logic       | `tests/test_feature_computation.py`, `tests/test_promotion.py`, `tests/test_drift.py` |
| Every batch scoring run    | Cluster sizes; model version stamped on every row; PSI drift | `ml/batch_scoring.py`, `ml/drift.py`                    |
| Every retrain run          | Candidate silhouette vs Production silhouette + tolerance     | `ml/promotion.py::decide_promotion`, `retrain_with_gate`|

### 4.1 Tenant coverage gate — `_validate_tenant_coverage`

Already documented in §2.4 — it's part of the modeling phase's pre-flight
check, but its purpose is evaluation: it catches an entire class of
silent regressions before any clustering math runs.

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

| Range          | Interpretation                                                                       |
|---|---|
| ≥ 0.50         | Strong, well-separated clusters                                                       |
| 0.25 – 0.50    | Reasonable structure                                                                  |
| 0.00 – 0.25    | Weak / overlapping — borderline                                                        |
| < 0            | Wrong K (points are on average closer to a different cluster than their own)          |

The K with the highest silhouette is the one shipped. It is logged as the
MLflow metric `silhouette` on every run, visible in the MLflow UI's
**Compare runs** view.

### 4.3 Cluster-size sanity check

`metadata.json` and MLflow both record
`cluster_sizes = {0: n0, 1: n1, ...}` (logged as
`cluster_<i>_size` metrics on the run). A healthy run produces roughly
comparable cluster sizes — no cluster <1% of the population. A degenerate
run typically shows one giant "everything-else" cluster and 2–3
micro-clusters; the silhouette will catch this most of the time, but the
size dict is the human-readable confirmation.

### 4.4 SQL ≡ Python parity test

[`tests/test_feature_computation.py`](../tests/test_feature_computation.py)
contains the two integration tests that pin the model/dashboard contract:

- **`test_risk_predictor_matches_persisted_fact`** — pulls up to 25 rows
  from `marts.fact_device_risk_score` filtered to the **same
  `model_version`** the predictor is currently loaded with, re-runs
  `RiskPredictor.predict()` on the matching features in
  `marts.v_ml_features_full`, and asserts `|py − sql| ≤ 0.1` per row.
  The tolerance accommodates IF `decision_function` numerics; tenants
  the loaded artifact doesn't know are counted in `skipped_tenant_rows`
  rather than failing the test — same posture as the API's 503 for
  unmodeled tenants. Filtering by model version makes this robust:
  the test must not fail just because the API is currently serving a
  newer registry version than the one that produced those rows.
- **`test_cluster_predictor_matches_persisted_assignment`** — pulls up to
  25 rows from `marts.fact_device_cluster_assignment` filtered to the
  **same `model_version`** the predictor is currently loaded with, re-runs
  `ClusterPredictor.predict()` on the matching features in
  `marts.v_ml_features_full`, and asserts the cluster id matches. Same
  version-filter trick as the risk test.

Both tests skip cleanly when Postgres is unreachable so the unit tier
still passes in any environment.

### 4.5 Model-version stamping (post-hoc evaluation)

Every row written to `marts.fact_device_cluster_assignment` **and**
`marts.fact_device_risk_score` carries the `model_version` and
`model_source` that produced it. So we can:

- Backfill comparisons: "for July 2026, how did v3 vs v4 assign tenant 235?"
- Detect stale partitions: "any month where `model_version` ≠ current
  Production?"
- Investigate user complaints: "why did device X's cluster change?" — the
  answer is in the version column, not lost to history. Same applies to
  risk scores: a sudden jump from 22 → 78 for the same device under the
  same calendar month is either real-world behaviour change or a model
  refit — the version stamp tells you which.
- Parity tests (§4.4) filter by version, so a fresh Production model
  doesn't invalidate facts produced under the previous one.

### 4.6 PSI drift detection — `ml/drift.py`

After every batch scoring run, the Prefect flow can call
[`detect_drift_for_months`](../src/accent_fleet/ml/drift.py) to compute
the **Population Stability Index** between two windows of
`marts.v_ml_features_full`:

- **reference window**: the `DEFAULT_REFERENCE_WINDOW_MONTHS = 6`
  calendar months immediately preceding the earliest touched month
  (anchored to `min(current_months)`, not "today", so a historical
  backfill measures against its own contemporary baseline).
- **current window**: the months the batch flow just (re)scored.

Per feature, PSI is computed over `PSI_N_BINS = 10` quantile bins
derived from the reference:

```
PSI = Σ_i (p_current[i] - p_reference[i]) * ln(p_current[i] / p_reference[i])
```

with `PSI_EPSILON = 1e-6` flooring empty bins to keep `log()` finite.

Industry rule-of-thumb thresholds (used by SAS, FICO, banking model
monitoring):

| PSI           | Interpretation                                       |
|---|---|
| < 0.10        | Stable                                                |
| 0.10 – 0.25   | Moderate change                                       |
| ≥ 0.25        | Significant drift (the configured alert threshold)    |

The flagging happens via `PSI_ALERT_THRESHOLD = 0.25`. Drift is
**informational** — the Prefect task that wraps `detect_drift_for_months`
never raises. A flow that drift-checks itself shouldn't fail because the
model is suddenly stale; it should keep loading data and surface the
problem so humans can decide whether to retrain.

Edge cases that return `PSI = 0` (rather than `NaN`) — so downstream
comparisons stay trivial:

- Empty reference or current sample.
- Reference distribution is a single repeated value (no valid bin edges).

All of these are pinned by [`tests/test_drift.py`](../tests/test_drift.py)
(`test_psi_identical_distributions_is_near_zero`,
`test_psi_large_shift_exceeds_alert_threshold`,
`test_psi_empty_inputs_returns_zero`,
`test_psi_constant_reference_returns_zero`,
`test_compare_frames_flags_only_drifted_features`).

#### 4.6.1 Score drift — `compute_score_drift`

Feature drift catches input-side shifts before they hurt the model.
**Score drift** catches output-side shifts after the model has spoken.
[`compute_score_drift`](../src/accent_fleet/ml/drift.py) takes the same
6-month reference / current-window split and computes PSI directly over
the `risk_score` column of `marts.fact_device_risk_score`:

- **reference window**: the 6 months immediately preceding the earliest
  scored month (same anchor logic as feature drift — `derive_reference_window`).
- **current window**: the months the batch flow just scored.

Returns `None` (not `0.0`) for two edge cases the promotion gate cares about:

- Empty current window — no scored months → no signal.
- Empty reference window — cold start, we've never scored before → the
  gate can't decide on PSI yet. Returning `None` lets the gate treat
  "PSI unknown" distinctly from "PSI = 0 ⇒ stable".

When PSI is finite and the value exceeds `PSI_ALERT_THRESHOLD = 0.25`,
the risk promotion gate (§4.7.2) holds. The orchestrator behaviour and
the SQL plumbing are pinned by five tests in `test_drift.py`
(`test_compute_score_drift_empty_current_months_returns_none`,
`test_compute_score_drift_no_reference_rows_returns_none`,
`test_compute_score_drift_returns_low_psi_on_identical_distributions`,
`test_compute_score_drift_returns_high_psi_on_shift`,
`test_compute_score_drift_queries_reference_then_current` — that last
one asserts the function issues *two* SQL queries in the right order
and that the current window is excluded from the reference).

### 4.7 Gated promotion — `ml/promotion.py`

Both models share the same two-step shape: train → register, then
gate-check before the registered version is allowed to take Production
traffic. The plain `train_clustering.run(promote=True)` /
`train_risk.run(promote=True)` paths unconditionally transition the
freshly-registered version to Production — fine for the very first
model, dangerous once a Production model exists: a regression ships
straight to the API.

The gate decision functions (`decide_promotion`, `decide_risk_promotion`)
are **pure functions** — no I/O — so they're trivially testable and the
full decision tree lives in
[`tests/test_promotion.py`](../tests/test_promotion.py) (17 tests
across both gates).

The wrappers around them (`retrain_with_gate`, `retrain_risk_with_gate`)
are **fail-soft** at the MLflow boundary:

- If MLflow isn't installed → `get_current_production_*()` returns
  `None` → cold-start branch (allow promotion).
- If `mlflow.transition_model_version_stage` fails →
  `_transition_to_production` returns `(False, reason)` and the result's
  `reason` is appended with the failure message, so a "gate passed but
  transition failed" case is loud in logs and metrics.

Local joblib artifacts are written **before** registry registration so
the API's disk-fallback path always has something to load even if MLflow
is unavailable.

#### 4.7.1 Silhouette gate — clustering

`scripts/retrain_monthly.py` calls `retrain_flow` (which wraps
`retrain_with_gate`) and applies a **silhouette-based gate** before
promotion:

```
promote IFF candidate_silhouette >= current_silhouette - tolerance
```

with `DEFAULT_SILHOUETTE_TOLERANCE = 0.02` (clustering literature: ±0.01
is noise, ±0.02 is meaningful but small, ±0.05+ is real). The tolerance
is configurable per run via `--tolerance`.

The full decision tree:

| Case                                      | Outcome                                       | Reason string                                                                                                |
|---|---|---|
| No current Production model               | **promote** (cold start)                      | `no_current_production_model`                                                                                |
| candidate ≥ current − tolerance           | **promote**                                   | `candidate <c> >= floor <f> (current <cur> - tol <t>)`                                                       |
| candidate < current − tolerance           | **hold**                                      | `silhouette regression: candidate <c> < floor <f> (current <cur> - tol <t>)`                                 |
| Exactly at floor (candidate == floor)     | **promote** (inclusive boundary)              | promotion reason as above                                                                                    |
| `tolerance = 0`                           | **strict ≥** non-regression                   | promotion reason as above                                                                                    |

#### 4.7.2 Stability gate — risk

The risk model is unsupervised — there's no held-out label and no
silhouette to point at — so we can't promote on a single quality scalar.
Instead, `scripts/retrain_risk_monthly.py` calls `retrain_risk_with_gate`
which applies a **distribution-stability gate** across three channels:

```
promote IFF
  Δ%critical_share ≤ critical_tolerance_pp
  AND Δ%high_share  ≤ high_tolerance_pp
  AND (score_drift_psi is None OR score_drift_psi < PSI_ALERT_THRESHOLD)
```

with defaults `critical_tolerance_pp = 5.0`, `high_tolerance_pp = 5.0`,
`PSI_ALERT_THRESHOLD = 0.25` (all configurable in
`config/pipeline.yaml::risk_promotion`).

The intuition: a model that suddenly classifies twice as many devices
"critical" is either right (real-world fleets got dramatically worse,
unlikely month-on-month) or wrong (regression in the IF artifact, bad
features, retraining artefact). Either way you do NOT silently promote it
— hold for review.

Δ-shares are computed against the previous Production artifact's persisted
`score_share` dict (the per-band shares averaged across tenants at
training time, stored in the bundled artifact). Score drift PSI is the
output of `compute_score_drift` (§4.6.1) over the most recent scored
window vs. its 6-month reference.

The full decision tree:

| Case                                                     | Outcome                                                       |
|---|---|
| No current Production model                              | **promote** (cold start)                                       |
| All three channels within tolerance                      | **promote**                                                    |
| Δ%critical > critical_tolerance_pp                       | **hold** — reason cites both shares and the tolerance         |
| Δ%high > high_tolerance_pp                               | **hold**                                                       |
| score_drift_psi ≥ 0.25                                   | **hold**                                                       |
| `score_drift_psi is None`                                | counted as "no PSI veto" (cold start for the score column)     |
| Multiple channels violate                                | **hold** — reason concatenates every violation                |
| Per-call tolerance override                              | passed through verbatim, no clamping                          |

All cases above are pinned by the 10 risk-side tests in
[`tests/test_promotion.py`](../tests/test_promotion.py)
(`test_risk_cold_start_promotes_unconditionally`,
`test_risk_within_tolerance_promotes`,
`test_risk_critical_shift_holds`,
`test_risk_high_shift_holds`,
`test_risk_psi_above_threshold_holds`,
`test_risk_psi_none_treated_as_no_veto`,
`test_risk_multiple_violations_concatenates_reasons`,
`test_risk_per_call_tolerance_override`,
`test_risk_decision_is_frozen`,
`test_risk_default_tolerances_match_yaml`).

The 7 silhouette-gate cases above are pinned by the same file
(`test_cold_start_promotes_unconditionally`,
`test_strict_improvement_promotes`,
`test_within_tolerance_regression_still_promotes`,
`test_outside_tolerance_regression_holds`,
`test_boundary_exactly_at_floor_promotes`,
`test_zero_tolerance_demands_strict_non_regression`, plus reason-string
assertions).

### 4.8 Notebook-based evaluation

The CRISP-DM evaluation phase also has a notebook tier
([`notebooks/05_evaluation/`](../notebooks/05_evaluation)):

- `01_cluster_quality.ipynb` — per-cluster z-score heatmap, used to
  re-label `web/src/lib/clusters.ts` after a retrain (cluster IDs are
  not semantically stable across re-trainings).
- `02_risk_score_validation.ipynb` — per-tenant distribution analysis
  of the IF risk score and category bands, sanity checks against
  `fact_device_risk_score` to confirm the persisted bands match the
  artifact's thresholds. Visualises the score-share dict the promotion
  gate keys off.
- `03_stability_and_fairness.ipynb` — coverage, edge cases, per-tenant
  drift on both models.

These are read-only diagnostics; the production gates live in code.

---

## 5. Retraining cadence

Both models have the same three-tier surface — ad-hoc CLI for first run,
gated CLI for monthly retrains, supercronic profile for scheduled
execution.

### 5.1 Clustering

| Cadence                | How                                                                                                                                                                                                                  |
|---|---|
| **Ad-hoc / first run** | `python scripts/train_clustering.py [--month-from YYYY-MM] [--no-promote]`. Auto-promotes by default (no gate); pass `--no-promote` to register without transitioning, then promote via the MLflow UI after review. |
| **Gated retrain**      | `python scripts/retrain_monthly.py [--month-from YYYY-MM] [--tolerance 0.02]`. Calls `retrain_flow → task_retrain_with_gate → retrain_with_gate`, applies the silhouette gate, never raises (returns the result dict). |
| **Scheduled monthly**  | `docker compose --profile scheduler up -d retrain-scheduler`. The supercronic schedule (`docker/retrain.crontab`) fires **every Monday at 04:00 UTC**, then `docker/scripts/run_monthly_retrain.sh` gates execution to the **first Monday of each month** in shell.   |
| **Host-cron alternative** | A vanilla cron line wrapping the same script — see the header of `scripts/retrain_monthly.py` for the snippet.                                                                                                  |

### 5.2 Risk

| Cadence                | How                                                                                                                                                                                                                  |
|---|---|
| **Ad-hoc / first run** | `python scripts/train_risk_score.py [--month-from YYYY-MM] [--no-promote]`. Trains one IF model per tenant in the cohort, bundles them into a single artifact, registers as `device-risk-score` in MLflow. |
| **Gated retrain**      | `python scripts/retrain_risk_monthly.py [--month-from YYYY-MM] [--critical-tolerance-pp 5 --high-tolerance-pp 5]`. Calls `retrain_risk_flow → task_retrain_risk_with_gate → retrain_risk_with_gate`, applies the stability gate (§4.7.2), never raises. |
| **Scheduled monthly**  | Same `retrain-scheduler` profile as clustering — `docker/retrain.crontab` fires both scripts; `docker/scripts/run_monthly_risk_retrain.sh` shares the first-Monday-of-month shell gate.                              |

### 5.3 Exit codes (both scripts)

- `0` — gate passed and promotion succeeded.
- `0` — gate held (regression detected; not a CLI error, operator decides).
- `1` — retraining or MLflow plumbing failed before the gate could decide.

After a successful promotion, hit `POST /v1/admin/reload-model` (with
`X-API-Key`) on the API so the running process picks up the new
artifacts without a restart — both `ClusterPredictor.reload()` and
`RiskPredictor.reload()` are thread-safe and reload independently.

---

## 6. End-to-end flow — putting it together

```
                       ┌─────────────────────────────────────────────┐
                       │           marts.v_ml_features_full          │
                       │   one row per (tenant, device, year_month)  │
                       │   13 features (shared by both models)       │
                       │   + all 35 baseline cols (audit/drill-down) │
                       └────────────────────┬────────────────────────┘
                                            │
              ┌─────────────────────────────┼──────────────────────────────┐
              │                             │                              │
              ▼                             ▼                              ▼
   ┌──────────────────────────┐ ┌──────────────────────────┐  ┌──────────────────────────┐
   │  RISK (IsolationForest)  │ │  CLUSTERING (KMeans)     │  │  BATCH SCORING (ETL)     │
   │                          │ │                          │  │                          │
   │  ONE model per tenant    │ │  StandardScaler          │  │  ml/batch_scoring.py     │
   │  ↓                       │ │  ↓                       │  │  loads both predictors   │
   │  StandardScaler (tenant) │ │  KMeans(n_init=10,       │  │  once, vectorised        │
   │  ↓                       │ │     random_state=42)     │  │  .transform/.predict     │
   │  IF(n_est=200,           │ │  ↓                       │  │  over month              │
   │     contamination='auto')│ │  silhouette over K=3..6  │  │  ↓                       │
   │  ↓                       │ │  ↓                       │  │  DELETE + INSERT into:   │
   │  -decision_function →    │ │  best (kmeans, scaler)   │  │   fact_device_cluster_   │
   │   raw anomaly score      │ │  ↓                       │  │     assignment           │
   │  ↓                       │ │  joblib + MLflow         │  │   fact_device_risk_score │
   │  rescale to [0,100]      │ │  registry (gate-checked) │  │  ↓                       │
   │  ↓                       │ │                          │  │  PSI feature drift +     │
   │  per-tenant quantile     │ │                          │  │  score drift (info)      │
   │  thresholds (50/80/95)   │ │                          │  │                          │
   │  → low/mod/high/critical │ │                          │  │                          │
   │                          │ │                          │  │                          │
   │  bundled artifact:       │ │                          │  │                          │
   │   {235: {scaler, model,  │ │                          │  │                          │
   │    raw_min/max, thresh,  │ │                          │  │                          │
   │    score_share}, ...}    │ │                          │  │                          │
   └────────────┬─────────────┘ └────────────┬─────────────┘  └──────────────────────────┘
                │                            │
                │ at API runtime             │ at API runtime
                ▼                            ▼
   ┌────────────────────────────┐ ┌──────────────────────────────┐
   │   RiskPredictor (lazy)     │ │   ClusterPredictor (lazy)    │
   │                            │ │                              │
   │   load from MLflow→joblib  │ │   load from MLflow → joblib  │
   │   fallback → 503           │ │   fallback → 503             │
   │   503 if tenant_id missing │ │                              │
   │                            │ │                              │
   │   POST /v1/score/risk      │ │   POST /v1/score/cluster     │
   │     {risk_score, category, │ │     {cluster_id, distance,   │
   │      components,           │ │      model_version}          │
   │      model_version}        │ │                              │
   │                            │ │                              │
   │   POST /v1/admin/          │ │   POST /v1/admin/            │
   │     reload-model →         │ │     reload-model →           │
   │     reload() (thread-safe) │ │     reload() (thread-safe)   │
   └────────────────────────────┘ └──────────────────────────────┘
```

---

## 7. Cheat sheet — common questions

**Q: What's the clustering model's input?**
The 13 features in `FEATURES` (`ml/train_clustering.py:40-54`), pulled
from `marts.v_ml_features_full`, filtered to device-months with
`total_distance_km ≥ 100` and `total_ignition_on_minutes > 0`. Missing
values default to 0.

**Q: What's the clustering model's output?**
`(cluster_id ∈ {0..K-1}, distance_to_centroid: float)`. K is chosen
automatically per training run from `{3, 4, 5, 6}` (typically 4 or 5).

**Q: What's the risk score's input?**
The same 13 features `FEATURES` use for clustering, plus a `tenant_id`
(required — risk is per-tenant). The API loads the IF model for that
tenant from the bundled artifact, scales the features with that
tenant's `StandardScaler`, and runs `-decision_function`. Tenants the
loaded artifact doesn't know return **HTTP 503** with reason
`tenant_model_missing` rather than silently scoring against a
neighbour's model.

**Q: What's the risk score's output?**
`(risk_score ∈ [0, 100], category ∈ {low, moderate, high, critical},
components: {feature_name: z_score}, model_version: str)`. `components`
is the per-feature standardised value (z-score after the tenant's
scaler) — not a weight contribution, since IF doesn't expose weights.
The category bands are the per-tenant quantile thresholds (50/80/95) of
the training distribution.

**Q: How is risk "validated"?**
Four layers: (1) required-tenant coverage gate at training time (same
cohort gate as clustering); (2) per-tenant `min_rows = 200` gate inside
`fit_one_tenant` skips quiet tenants instead of crashing the run;
(3) **stability-gated promotion** — `decide_risk_promotion` holds when
Δ%critical or Δ%high exceeds 5pp, or when score-PSI ≥ 0.25;
(4) feature PSI + score PSI on every batch scoring run, alerting when
either ≥ 0.25.

**Q: How is clustering "validated"?**
Four layers: (1) required-tenant coverage gate at training time;
(2) silhouette score for K selection (logged to MLflow on every run);
(3) silhouette-gated promotion (`decide_promotion`, tolerance 0.02) for
monthly retrains; (4) PSI drift on every batch scoring run, alerting
when feature PSI ≥ 0.25.

**Q: How often are the models retrained?**
Manually on demand via `python scripts/train_clustering.py` /
`python scripts/train_risk_score.py`, gated manually via
`python scripts/retrain_monthly.py` / `python scripts/retrain_risk_monthly.py`,
or automatically on the **first Monday of each month at 04:00 UTC**
when the `retrain-scheduler` compose profile is active (both models
retrain on the same schedule).

**Q: How do you roll back a bad model version?**
MLflow UI → transition the previous version back to `Production`
(it'll auto-archive the bad one) → call `POST /v1/admin/reload-model`
on the API. The `model_version` column in
`fact_device_cluster_assignment` / `fact_device_risk_score` lets you
find which months need re-scoring.

**Q: Cluster IDs renumbered after a retrain — what now?**
The numeric IDs are arbitrary; the personas mapped in
`web/src/lib/clusters.ts` are not stable across retrainings. Open
`notebooks/05_evaluation/01_cluster_quality.ipynb`, inspect the z-score
heatmap, and re-edit `clusters.ts` so each `cluster_id` points at the
right persona again.

---

## 8. Pointers

### Entrypoints

- Clustering training — `scripts/train_clustering.py`
- Clustering gated retrain — `scripts/retrain_monthly.py`
- Risk training — `scripts/train_risk_score.py`
- Risk gated retrain — `scripts/retrain_risk_monthly.py`

### Library code

- Clustering training — `src/accent_fleet/ml/train_clustering.py`
- Risk training — `src/accent_fleet/ml/train_risk.py`
- Promotion gates (both models) — `src/accent_fleet/ml/promotion.py`
- Drift monitor (feature + score PSI) — `src/accent_fleet/ml/drift.py`
- Inference (`ClusterPredictor`, `RiskPredictor`) — `src/accent_fleet/ml/inference.py`
- Batch scoring (cluster + risk) — `src/accent_fleet/ml/batch_scoring.py`
- Prefect flow — `src/accent_fleet/pipeline/flow_batch.py`

### API

- Routes — `app/routes/score.py`, `app/routes/admin.py`
- Versioning policy — `app/versioning.py`
- Schemas — `app/schemas/score.py`

### SQL

- Risk score fact (new) — `sql/28_fact_device_risk_score.sql`
- Risk score compat view — `sql/21_v_device_risk_profile.sql`
- Cluster assignment fact — `sql/27_fact_device_cluster_assignment.sql`
- Feature contract — `sql/26_v_ml_features_full.sql`
- Mart refresh — `sql/41_refresh_marts.sql`

### Config

- IF risk-model hyperparams — `config/feature_definitions.yaml::risk_score_model`
- Tenant cohort + promotion tolerances — `config/pipeline.yaml`

### Tests

- Per-tenant IF training math — `tests/test_train_risk.py`
- RiskPredictor inference — `tests/test_inference.py`
- Promotion gates (both models) — `tests/test_promotion.py`
- Drift (feature PSI + score PSI) — `tests/test_drift.py`
- Batch scoring (cluster + risk) — `tests/test_batch_scoring.py`
- API score routes — `tests/test_api_score.py`
- SQL ≡ Python parity — `tests/test_feature_computation.py`

### Ops

- Retrain crontab — `docker/retrain.crontab`
- Notebooks — `notebooks/04_modeling/`, `notebooks/05_evaluation/`

### Companion docs

- Data-prep companion — `docs/DATA_PREPARATION.md`
- Project overview — `docs/HOW_IT_WORKS.md`
