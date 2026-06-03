# How the Modeling & Evaluation Works — Isolation Forest + K-Means

> A self-contained, plain-language walkthrough of the two machine-learning
> models in Accent Fleet Analytics: **what** they do, **why** they were
> chosen, **how** they are trained, and **how** we decide whether a freshly
> trained model is good enough to ship.
>
> This is the teaching version. For the exhaustive engineering reference see
> [`docs/MODELING_AND_EVALUATION.md`](MODELING_AND_EVALUATION.md); for the
> thesis-oriented walkthrough see [`report/modeling_and_evaluation.md`](../report/modeling_and_evaluation.md).
> All facts below were verified against `src/accent_fleet/ml/`.

---

## 1. Two models, two questions

Accent Fleet Analytics is **not one big model**. It is **two independent
unsupervised models** that read the *same* monthly feature table but answer
different questions:

| Model | Algorithm | Question it answers | Output |
|---|---|---|---|
| **Device Risk Score** | **Isolation Forest**, one per tenant | *"How unusual is this device's driving for this fleet?"* | `risk_score ∈ [0,100]` + category (`low` / `moderate` / `high` / `critical`) + per-feature z-scores |
| **Device-Behavior Clustering** | **K-Means**, one global model | *"Which behavioural archetype does this device belong to?"* | `cluster_id ∈ {0..K-1}` + distance to its centroid |

### Why unsupervised?

We have **no ground-truth crash/incident labels** — the telematics data
never tells us "this driver was dangerous". So a supervised classifier is
impossible. Instead we use two unsupervised techniques that extract
operational signal from the data's *structure*:

- **Anomaly detection** (Isolation Forest) — finds device-months that are
  statistical outliers relative to their own fleet.
- **Clustering** (K-Means) — groups device-months into behavioural
  archetypes so each device gets a human-readable persona.

The honest caveat we surface in the UI: **"high risk" means *unusual for
this fleet*, not *provably dangerous*.** That's why the API returns the
per-feature z-scores alongside the score — a fleet manager can see *why* a
device flagged, instead of trusting a black-box number.

### Why is risk per-tenant but clustering global?

- **Risk is fit per tenant** because "normal" driving differs wildly between
  fleets. A delivery van fleet and a school-bus fleet don't share a baseline.
  A single global anomaly model would just flag whichever tenant is smallest,
  because its data sits on the tails of the combined distribution.
- **Clustering is one global model** so every tenant shares the same
  vocabulary ("cluster 2 = high-overspeed urban") and the model sees enough
  rows (~2,000 device-months) to learn stable centroids.

---

## 2. The shared input: one feature table, 13 features

Both models read from **`marts.v_ml_features_full`** — one row per
`(tenant_id, device_id, year_month)` (a "device-month"). Both use the **same
13 features**, chosen to span four behavioural axes:

| Axis | Features |
|---|---|
| **Overspeed** | `overspeed_per_100km`, `avg_speed_over_limit`, `high_speed_trip_ratio`, `speed_alert_per_100km` |
| **Harsh driving** | `harsh_brake_per_100km`, `harsh_accel_per_100km`, `harsh_corner_per_100km` |
| **Engine / efficiency** | `monthly_idle_ratio`, `high_rpm_minutes_per_day` |
| **Exposure / pattern** | `night_trip_ratio`, `rush_hour_trip_ratio`, `stddev_trip_distance`, `short_trip_ratio` |

Missing values become `0` (matches the SQL view's `COALESCE`). The list is
declared as a tuple in both `train_clustering.py::FEATURES` and
`train_risk.py::FEATURES`, kept in sync with
`config/feature_definitions.yaml`.

### Activity gates — who is worth scoring?

Before any model math runs, low-signal device-months are filtered out:

| Gate | Rule | Where |
|---|---|---|
| Activity filter (both models) | `year_month >= month_from` AND `total_distance_km >= 100` AND `total_ignition_on_minutes > 0` | `load_training_frame` |
| K-Means minimum frame size | `len(df) >= 50` | `fit_clustering` |
| Risk per-tenant floor | tenant needs `>= 50` rows, else **skipped** | `fit_one_tenant` |

A device that barely drove has no behavioural signal — including it would
just create a degenerate "barely-driven" cluster or a noisy risk number.

### Tenant-coverage pre-flight check

`config/pipeline.yaml::modeling` lists `expected_tenants` and
`required_tenants`. Before training, `_validate_tenant_coverage` runs:

- A missing **expected** tenant → warning, training continues.
- A missing **required** tenant (`7486`) → **raises `ValueError`, aborts.**

Tenant 7486 is telemetry-first: its rows only appear after
`scripts/reconstruct_telemetry_trips.py` runs. If that step silently breaks,
7486 vanishes and the clusters drift to fit the other four tenants — so the
guard fails loudly and tells the operator exactly which script to re-run.

---

## 3. Model 1 — K-Means clustering (global)

**CLI:** `python scripts/train_clustering.py [--month-from 2025-01] [--no-promote]`
**Code:** `src/accent_fleet/ml/train_clustering.py`

### How it trains, step by step

```
load_training_frame(month_from)          # activity-filtered device-months
        ↓
_validate_tenant_coverage(df)            # fail fast if 7486 missing
        ↓
fit_clustering(df):                      # needs >= 50 rows
    X      = df[FEATURES].fillna(0)
    scaler = StandardScaler().fit(X)     # <-- crucial, see below
    Xs     = scaler.transform(X)
    for k in {3, 4, 5, 6}:
        km  = KMeans(n_clusters=k, n_init=10, random_state=42).fit(Xs)
        sil = silhouette_score(Xs, km.labels_)
        keep (km, k) with the highest silhouette
        ↓
save_local(...)                          # models/clustering/*.joblib + metadata.json
log_to_mlflow(..., promote=True)         # register device-clustering vN -> Production
```

### Why each choice

| Choice | Why |
|---|---|
| **`StandardScaler` first** | K-Means uses Euclidean distance. Without scaling, a large-magnitude feature like `high_rpm_minutes_per_day` would dominate small ratios like `night_trip_ratio`. Scaling puts every feature on equal footing. |
| **K-Means** (not DBSCAN / hierarchical) | We want a fixed, small number of interpretable personas, and we need to score a *new* device in microseconds. DBSCAN can't predict on new points natively; hierarchical clustering scales poorly. |
| **`n_init=10`** | K-Means converges to a *local* optimum that depends on its random start. Running 10 k-means++ initializations and keeping the lowest-inertia fit guards against a bad seed. |
| **`random_state=42`** | Pinned everywhere → same data in, same labels out. Essential for reproducibility and the parity tests. |
| **`K ∈ {3,4,5,6}`** | Below 3 the silhouette is meaningless; above 6 it gets noisy at our row counts. The K with the best silhouette wins (typically 3–5). |
| **One global model, bundled** | Shared cross-tenant vocabulary + the artifact `{kmeans, scaler, feature_order}` is logged as **one** object, so the API can never pair a scaler from one run with a K-Means from another. |

### What it produces

- `models/clustering/kmeans_v1.joblib`, `scaler_v1.joblib`, `metadata.json`
- An MLflow run logging the `silhouette` metric + each `cluster_<i>_size`
- A registered model `device-clustering` at stage `Production`

### How a prediction is made

```python
scaled = scaler.transform(feature_vector)
cluster_id = kmeans.predict(scaled)            # which centroid is closest
distance   = kmeans.transform(scaled)[cluster] # how far from that centroid
```

---

## 4. Model 2 — Isolation Forest risk score (per tenant)

**CLI:** `python scripts/train_risk_score.py [--month-from 2025-01] [--no-promote]`
**Code:** `src/accent_fleet/ml/train_risk.py`

### What an Isolation Forest does (intuition)

An Isolation Forest builds many random binary trees by repeatedly picking a
random feature and a random split point. **Anomalies get isolated in very
few splits** (they sit alone in sparse regions); normal points need many
splits to separate. The average path length to isolate a point becomes its
anomaly score. No assumption of a bell curve, no labels needed.

### How it trains, step by step

```
load_training_frame(month_from)          # same filter as clustering
        ↓
_validate_tenant_coverage(df)            # 7486 required
        ↓
fit_risk_model(df):
    for each tenant in df.groupby("tenant_id"):
        if rows < 50:  skip (recorded in skipped_tenants)
        X      = sub[FEATURES].fillna(0)
        scaler = StandardScaler().fit(X)                 # per tenant
        Xs     = scaler.transform(X)
        iso    = IsolationForest(n_estimators=200,
                                 contamination="auto",
                                 random_state=42).fit(Xs)
        raw    = -iso.decision_function(Xs)              # higher = more anomalous
        scores = rescale_raw_to_0_100(raw, raw.min(), raw.max())
        t_m,t_h,t_c = quantile_thresholds(scores, (0.50, 0.80, 0.95))
        labels      = categorize_scores(scores, thresholds)
        store {scaler, model, raw_min/max, thresholds, score_share, ...}
        ↓
save_local(...)                          # models/risk_score/risk_v1.joblib + metadata.json
log_to_mlflow(..., promote=True)         # register device-risk-score vN -> Production
```

### From raw score to a 0–100 number with bands

1. **Raw anomaly score** = `-decision_function(X)` — unbounded and unitless.
   The minus sign flips it so **higher = more anomalous**.
2. **Rescale to [0, 100]** using the *fit-time* min/max (`rescale_raw_to_0_100`).
   Live values outside that window are **clipped**, so a wildly anomalous row
   reads 100, never 137 — keeping the API contract `risk_score ∈ [0,100]`.
3. **Per-tenant quantile bands** (`categorize_scores`):
   - `low` = below the 50th percentile
   - `moderate` = 50th–80th
   - `high` = 80th–95th
   - `critical` = top 5%

   Banding on *each tenant's own quantiles* means "critical" always means
   "top 5% within your fleet" — a fleet where everyone drives fast doesn't
   get its whole population flagged critical.

### Why each choice

| Choice | Why |
|---|---|
| **Isolation Forest** (not One-Class SVM / LOF / autoencoder) | Best match for "unusual within this fleet" without OCSVM's parametric assumptions, LOF's O(n²) memory, or a deep model's hunger for labels. Returns a *smooth* score (not just a yes/no), which the dashboard bands need. |
| **`n_estimators=200`, `random_state=42`** | Reproducible; 200 trees is the stable sweet spot. |
| **`contamination="auto"`** | We don't know the true fraction of "anomalous" device-months. Sklearn derives the offset from the training distribution; the bands come from quantiles downstream anyway. |
| **Per-tenant fit** | Each fleet's "normal" is genuinely different; lets the promotion gate compare each tenant against its own history. |
| **Per-tenant `StandardScaler`** | Isolation Forest is scale-invariant in theory, but we *return per-feature z-scores* as "components", and those need a fitted scaler. Per-tenant means a z-score reads "1.5σ above *this* fleet's mean idle ratio". |
| **One bundled artifact** (all tenants in one dict) | Rollback restores every tenant's model together — never a v3 scaler paired with a v4 forest. |
| **No hand-tuned weights** | The forest learns the structure itself. The "components" the API returns are honest z-scores, not invented weight contributions. |

### What it produces

- `models/risk_score/risk_v1.joblib` (one dict bundling every tenant's
  scaler + forest + bounds + thresholds + `score_share`) and `metadata.json`
- An MLflow run logging `overall_share_{category}` + per-tenant share metrics
- A registered model `device-risk-score` at stage `Production`

### How a prediction is made

```python
entry  = artifact["tenants"][tenant_id]    # 503 if this tenant has no model
scaled = entry["scaler"].transform(vec)
raw    = -entry["model"].decision_function(scaled)
score  = rescale_raw_to_0_100(raw, entry["raw_min"], entry["raw_max"])
band   = categorize_scores(score, entry["thresholds"])
components = (vec - feature_means) / feature_stds   # per-feature z-scores
```

`tenant_id` is **required** — a vector with no tenant can't be routed. A
request for an unknown tenant returns **HTTP 503** (`tenant_model_missing`),
which the dashboard shows as "feature not ready for this tenant" rather than
a generic error.

---

## 5. Where the models run

Each model runs in **two places that must agree numerically**:

- **In-database (batch):** `ml/batch_scoring.py` scores the whole month
  vectorised (~1000× faster than per-row), then writes
  `fact_device_cluster_assignment` and `fact_device_risk_score` via
  **delete-then-insert in one transaction**. Dashboards read these
  pre-computed facts — no per-row API call. Every row stamps `model_version`
  + `model_source` for auditing.
- **In-process (online):** `ClusterPredictor` / `RiskPredictor`
  (`ml/inference.py`) score ad-hoc vectors for the FastAPI endpoints
  `POST /v1/score/cluster` and `POST /v1/score/risk`.

Both predictors load lazily: **MLflow registry → local joblib → unloaded**.
If unloaded, `/health` still returns 200 but the score endpoint returns 503
(so the API can deploy before the first training run).

---

## 6. Evaluation — how we judge a model

Evaluation runs at four cadences, each catching a different failure class:

| When | What we check |
|---|---|
| Every training run | Tenant coverage; silhouette across K |
| Every CI commit | SQL ≡ Python parity; promotion-gate logic |
| Every batch scoring run | Cluster sizes; model-version stamping; PSI drift |
| Every monthly retrain | Silhouette gate (clustering) / stability gate (risk) |

### 6.1 Silhouette score — how K-Means is judged

For each candidate K, the **silhouette** of a point is:

```
silhouette(i) = (b(i) − a(i)) / max(a(i), b(i))
```

where `a(i)` = mean distance to points in its *own* cluster, `b(i)` = mean
distance to the *nearest other* cluster. The overall score is the mean over
all points, in `[-1, +1]`:

| Range | Meaning |
|---|---|
| ≥ 0.50 | Strong, well-separated |
| 0.25 – 0.50 | Reasonable structure |
| 0.00 – 0.25 | Weak / overlapping |
| < 0 | Wrong K |

The K with the highest silhouette ships. (Real fleet data is overlapping, so
scores around 0.22–0.27 are normal — see the per-tenant table in
`notebooks/05_evaluation/README.md`.) Plus a **cluster-size sanity check**:
no healthy run has a cluster <1% of the population.

### 6.2 How the unsupervised risk score is validated

There's no held-out label, so risk is validated three ways:

1. **Internal consistency** (`02_risk_score_validation.ipynb`): high-band
   device-months should have systematically worse feature means than
   low-band ones. They do — e.g. for tenant 7486, high-band rows show ~4.5×
   more overspeeds, ~5× more harsh events, ~18× more high-RPM minutes.
2. **External (maintenance) validation:** correlate risk with
   `fact_maintenance` events. Currently **tenant-limited** (ground truth
   concentrated in tenant 7486), so treated as *directional* evidence only —
   the dashboard footer discloses this honestly.
3. **Stability** (`03_stability_and_fairness.ipynb`): month-over-month band
   churn stays under ~16%, and the band-transition matrix is "sticky"
   (P[low→low]≈0.94, P[high→high]≈0.60).

### 6.3 SQL ≡ Python parity

Two integration tests in `tests/test_feature_computation.py` guarantee the
batch facts and the live API agree: they pull rows from each fact table
(filtered to the *same model version* the predictor loaded), re-run the
predictor, and assert the cluster id matches exactly / the risk score matches
within `±0.1`. This is what stops the dashboard and the API from quietly
disagreeing.

### 6.4 PSI drift monitoring

After each batch run, `ml/drift.py` computes the **Population Stability
Index** between a 6-month reference window and the months just scored:

```
PSI = Σ (p_current − p_reference) · ln(p_current / p_reference)
```

| PSI | Meaning |
|---|---|
| < 0.10 | Stable |
| 0.10 – 0.25 | Moderate change |
| ≥ 0.25 | Significant drift (alert threshold) |

- **Feature drift** (`detect_drift_for_months`) watches the inputs.
- **Score drift** (`compute_score_drift`) watches the `risk_score` output.

Drift is **informational** — it never crashes the pipeline; it surfaces the
problem so a human decides whether to retrain.

---

## 7. Gated promotion — the safety net before shipping

The first model of each kind promotes unconditionally (cold start). Once a
Production model exists, a **gate** (`ml/promotion.py`, pure functions, fully
unit-tested) decides whether the new candidate is allowed to replace it.

### 7.1 Clustering — silhouette gate

```
promote IFF candidate_silhouette ≥ current_silhouette − 0.02
```

A small tolerance (0.02) absorbs run-to-run silhouette noise from re-fitting
K-Means on a newer data slice. A bigger drop ⇒ **hold for review**.

### 7.2 Risk — stability gate (three channels)

```
promote IFF |Δ%critical| ≤ 5pp
        AND |Δ%high|     ≤ 5pp
        AND (score_PSI is None OR score_PSI < 0.25)
```

The intuition: a model that suddenly doubles the share of "critical" devices
is almost certainly broken (real fleets don't change that fast). So we
**hold** it for a human instead of silently shipping it.

Both gates are **fail-soft**: if MLflow is down, local joblib artifacts are
still written so the API's fallback path always has something to load, and
the failure is logged loudly rather than crashing the retrain.

### Retraining cadence

| | Clustering | Risk |
|---|---|---|
| First run | `train_clustering.py` | `train_risk_score.py` |
| Gated retrain | `retrain_monthly.py` | `retrain_risk_monthly.py` |
| Scheduled | first Monday of each month, 04:00 UTC (`retrain-scheduler` compose profile) | same scheduler |

After a successful promotion, call `POST /v1/admin/reload-model` so the
running API picks up the new artifacts without a restart.

---

## 8. One-paragraph summary

The system runs **two unsupervised models** on a shared 13-feature
device-month table. **K-Means** (one global model, K chosen by silhouette)
assigns each device a behavioural *persona*; a **per-tenant Isolation
Forest** assigns each device a 0–100 *anomaly risk score* with low/moderate/
high/critical bands set from each fleet's own quantiles. Both run in batch
(writing fact tables) and online (FastAPI), kept numerically identical by
parity tests. Quality is judged by silhouette (clustering) and
internal-consistency + stability (risk), monitored continuously with PSI
drift, and every retrain must clear a **promotion gate** — silhouette
non-regression for clustering, category-mix + score-PSI stability for risk —
before it can take Production traffic.

---

## 9. Where to look in the code

| Thing | File |
|---|---|
| K-Means training | `src/accent_fleet/ml/train_clustering.py` |
| Isolation Forest training | `src/accent_fleet/ml/train_risk.py` |
| Promotion gates (both) | `src/accent_fleet/ml/promotion.py` |
| Drift (feature + score PSI) | `src/accent_fleet/ml/drift.py` |
| Inference (both predictors) | `src/accent_fleet/ml/inference.py` |
| Batch scoring | `src/accent_fleet/ml/batch_scoring.py` |
| Hyperparameters | `config/feature_definitions.yaml`, `config/pipeline.yaml` |
| Evaluation notebooks | `notebooks/05_evaluation/` |
| Tests | `tests/test_train_*.py`, `tests/test_promotion.py`, `tests/test_drift.py`, `tests/test_feature_computation.py` |
