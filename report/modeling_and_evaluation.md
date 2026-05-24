# Modeling & Evaluation Phase — Accent Fleet Analytics

> Report-ready walkthrough of the **Modeling** and **Evaluation** phases of
> the CRISP-DM cycle as they actually run in the v0.6 codebase. Companion
> source for `report/chapters/chapter5_modeling.tex`.
>
> Canonical engineering reference: [`docs/MODELING_AND_EVALUATION.md`](../docs/MODELING_AND_EVALUATION.md).

---

## 0. CRISP-DM scope of this phase

In CRISP-DM, **Modeling** is *"select techniques, generate test design, build
the model, assess it"*; **Evaluation** is *"evaluate results against business
goals, review the process, determine next steps"*. In Accent Fleet
Analytics this phase consumes the feature mart produced by Data Preparation
(`marts.v_ml_features_full`, grain = `(tenant_id, device_id, year_month)`)
and writes back two fact tables (`fact_device_cluster_assignment`,
`fact_device_risk_score`) plus two MLflow registries (`device-clustering`,
`device-risk-score`).

| CRISP-DM sub-task | Where it lives in the repo |
|---|---|
| Select technique | `ml/train_clustering.py` (KMeans), `ml/train_risk.py` (per-tenant Isolation Forest) |
| Generate test design | `tests/test_train_*.py`, `tests/test_feature_computation.py`, `tests/test_inference.py` |
| Build the model | `scripts/train_clustering.py`, `scripts/train_risk_score.py` (CLIs); `scripts/retrain_*_monthly.py` (gated) |
| Assess the model | Silhouette (clustering), per-tenant share + PSI (risk), SQL ≡ Python parity |
| Approve model | MLflow Production stage + local joblib fallback |
| Review / next steps | `ml/promotion.py` (two gates), `ml/drift.py` (feature + score PSI) |

---

## 1. The two models — design decision

Accent Fleet Analytics is **not one big model**. It is **two independent
unsupervised ML systems** that share the same feature mart but answer
different questions:

| Model | Type | Question it answers | Output |
|---|---|---|---|
| **Device Risk Score** | Per-tenant Isolation Forest | *"How unusual is this device's driving for this tenant's fleet?"* | `risk_score ∈ [0,100]` + category (`low`/`moderate`/`high`/`critical`) + per-feature z-score components |
| **Device-Behavior Clustering** | Global KMeans | *"Which behavioural archetype does this device belong to?"* | `cluster_id ∈ {0..K-1}` + Euclidean distance to centroid |

Both are unsupervised — we have **no ground-truth crash labels** in
Tunisian telematics, so a supervised classifier is impossible. Anomaly
detection + clustering extract operational signal anyway, with the honest
caveat that *"high score"* means *unusual for the tenant*, not *causally
dangerous*. The dashboard surfaces per-feature z-scores alongside the
score so a fleet manager sees *why* a device flagged anomalous instead of
taking the number on faith.

**Design rationale for the split:**

- **Risk is per-tenant** because the "normal" behaviour distribution
  varies wildly between fleets (delivery vans in Sousse don't share a
  "normal" with school buses in Tunis). A global Isolation Forest would
  mostly flag the smaller tenants as anomalous because their data lives
  on the tails.
- **Clustering is global** so we get a shared cross-tenant vocabulary
  ("cluster 2 = high-overspeed urban") and enough rows (~2,000 device-
  months) to learn stable centroids.

Each model runs in **two places that must agree numerically**:

- **In-database** — `marts.fact_device_cluster_assignment` and
  `marts.fact_device_risk_score`, written by the Prefect batch flow
  after each marts refresh. Dashboards read pre-computed scores from the
  facts — no per-row API round-trip.
- **In-process** — Python (`ml/inference.py::ClusterPredictor`,
  `ml/inference.py::RiskPredictor`) called by the FastAPI service for
  ad-hoc and online scoring of vectors not yet in the mart.

Numerical parity is pinned by parity tests (see § 6.3).

---

## 2. Modeling — Step 1: Inputs and gates

### 2.1 Feature contract

Both models read from **`marts.v_ml_features_full`** (defined in
`sql/26_v_ml_features_full.sql`). It is a `LEFT JOIN` of:

- `marts.v_ml_features_driver_behavior` — 35 trip-derived feature columns
  (overspeed, alerts, trip volumes, temporal patterns).
- `marts.mart_device_monthly_telemetry` — 22 archive-derived columns
  (harsh events, idling, RPM).

`LEFT JOIN` (not inner) ensures devices with no archive pings still
produce a feature row — telemetry columns just become `0` / `NULL`.

### 2.2 Feature subset — 13 features across 4 axes

Both models use the same 13 columns, picked to span four operational
axes:

| Axis | Features |
|---|---|
| Overspeed behaviour | `overspeed_per_100km`, `avg_speed_over_limit`, `high_speed_trip_ratio`, `speed_alert_per_100km` |
| Harsh driving | `harsh_brake_per_100km`, `harsh_accel_per_100km`, `harsh_corner_per_100km` |
| Engine / efficiency | `monthly_idle_ratio`, `high_rpm_minutes_per_day` |
| Exposure / pattern | `night_trip_ratio`, `rush_hour_trip_ratio`, `stddev_trip_distance`, `short_trip_ratio` |

Missing values → 0 (matches SQL `COALESCE` semantics). Canonical names
live in `config/feature_definitions.yaml`; both `train_clustering.py::FEATURES`
and `train_risk.py::FEATURES` re-declare the tuple so other modules
import the constant without round-tripping through YAML. A unit test
(`test_risk_defaults_match_yaml_documented_values`) pins the IF
hyper-parameters and gate tolerances to YAML so a careless edit fails
fast.

### 2.3 Activity gates

| Gate | Where | Condition |
|---|---|---|
| Clustering training filter | `train_clustering.py::load_training_frame` | `year_month >= :month_from` AND `total_distance_km >= 100` AND `total_ignition_on_minutes > 0` |
| Risk per-tenant floor | `train_risk.py::fit_risk_model` | Same activity filter + `min_rows_per_tenant = 50`; below ⇒ `skipped_tenants` |
| Minimum frame size for KMeans | `train_clustering.py::fit_clustering` | `len(df) >= MIN_ROWS = 50` |
| Driver-behaviour view floor | `sql/v_ml_features_driver_behavior.sql` | `total_trips >= 5` (per month, pre-aggregate) |

Devices below the thresholds are dropped — they don't have enough signal
to cluster or score meaningfully and would collapse into degenerate
"barely-driven" outputs.

### 2.4 Tenant cohort validation — pre-flight check

`config/pipeline.yaml::modeling` declares which tenants the training run
must see:

```yaml
modeling:
  expected_tenants: [235, 238, 264, 1787, 7486]
  required_tenants: [7486]
```

`_validate_tenant_coverage(df)` runs **before any model math**:

- Missing an *expected* tenant → warning log, training continues.
- Missing a *required* tenant → raises `ValueError`, aborts.

Tenant **7486** is telemetry-first in the source system; its rows only
land in the feature view after `scripts/reconstruct_telemetry_trips.py`
runs. If that step silently breaks, the activity-filtered frame loses
7486 and the new clusters drift to fit the remaining four tenants. The
error message names exactly which fix-up script to run. Pinned by
`test_required_modeling_tenant_missing_raises`.

---

## 3. Modeling — Step 2: Clustering pipeline (KMeans)

CLI entry point: `python scripts/train_clustering.py [--month-from 2025-01] [--no-promote]`
Library: `src/accent_fleet/ml/train_clustering.py::run()`

### 3.1 Algorithm — step by step

```
load_training_frame(month_from)
    ↓ DataFrame (n_device_months × 13 features + ids)
    [activity-filtered]
_validate_tenant_coverage(df)        # § 2.4 — fail-fast if 7486 missing
    ↓
fit_clustering(df)                   # requires len(df) >= MIN_ROWS (50)
    ├── X       = df[FEATURES].fillna(0).to_numpy(dtype=float)
    ├── scaler  = StandardScaler().fit(X)
    ├── Xs      = scaler.transform(X)
    └── For each k in range(3, 7):
            km   = KMeans(n_clusters=k, n_init=10, random_state=42).fit(Xs)
            sil  = silhouette_score(Xs, km.labels_)
            keep (km, k) if sil > best_so_far
    → returns (best_kmeans, scaler, TrainResult)
    ↓
save_local(...)                      # models/clustering/{kmeans_v1,scaler_v1}.joblib + metadata.json
log_to_mlflow(..., promote=True)     # registers device-clustering vN, transitions to Production
```

### 3.2 Algorithm choices — why each one

| Choice | Why |
|---|---|
| `StandardScaler` | KMeans uses Euclidean distance — without scaling, large-magnitude features (`high_rpm_minutes_per_day`) would dominate small ones (ratios). |
| `KMeans(n_init=10)` | KMeans converges to a local optimum; 10 different k-means++ seeds and keeping the lowest-inertia fit is the standard guard. |
| `random_state=42` | Pinned everywhere (K selection + KMeans). Same features in → same labels out. Critical for parity tests and reproducing investigations. |
| `K ∈ {3,4,5,6}` | Silhouette becomes meaningless below 3 and noisy above 6 at our row counts (~few thousand device-months). |
| **Global, not per-tenant** | Gives a shared cross-tenant vocabulary + enough rows for stable centroids. |
| **Single bundled artifact** `{kmeans, scaler, feature_order}` | Atomicity: the API can never load a scaler from one run with a KMeans from another. |

### 3.3 Outputs

| Where | Content |
|---|---|
| `models/clustering/kmeans_v1.joblib` | Pickled fitted `KMeans` |
| `models/clustering/scaler_v1.joblib` | Pickled fitted `StandardScaler` |
| `models/clustering/metadata.json` | `{version, trained_at, k, silhouette, n_rows, feature_order, cluster_sizes, tenants, rows_by_tenant, random_state, training_window}` |
| MLflow run | Params + `silhouette` metric + one `cluster_<i>_size` per cluster + full artifact |
| MLflow Registry | `device-clustering` v`N` at stage `Production` (older versions auto-archived) |

---

## 4. Modeling — Step 3: Risk pipeline (per-tenant Isolation Forest)

**This is the v0.6 piece** — it replaces the v0.5 deterministic six-factor
weighted formula. CLI:
`python scripts/train_risk_score.py [--month-from 2025-01] [--no-promote]`
Library: `src/accent_fleet/ml/train_risk.py::run()`

### 4.1 Algorithm — step by step

```
load_training_frame(month_from)
    ↓ DataFrame (n_device_months × 13 features + ids)
    [same activity filter as clustering]
_validate_tenant_coverage(df)        # 7486 required
    ↓
fit_risk_model(df)
    └── For each tenant in df.groupby("tenant_id"):
            sub = rows for this tenant
            if len(sub) < min_rows_per_tenant (50):
                skipped_tenants[tenant_id] = "below_min_rows (N < 50)"
                continue
            X       = sub[FEATURES].fillna(0).to_numpy(dtype=float)
            scaler  = StandardScaler().fit(X)
            Xs      = scaler.transform(X)
            iso     = IsolationForest(n_estimators=200,
                                       contamination="auto",
                                       random_state=42).fit(Xs)
            raw     = -iso.decision_function(Xs)      # higher = more anomalous
            raw_min, raw_max = raw.min(), raw.max()
            scores  = rescale_raw_to_0_100(raw, raw_min, raw_max)
            t_m, t_h, t_c = quantile_thresholds(scores, (0.50, 0.80, 0.95))
            labels  = categorize_scores(scores, (t_m, t_h, t_c))
            artifact["tenants"][tenant_id] = {
                "scaler": scaler, "model": iso,
                "raw_min": raw_min, "raw_max": raw_max,
                "thresholds": {"moderate": t_m, "high": t_h, "critical": t_c},
                "feature_means": scaler.mean_, "feature_stds": scaler.scale_,
                "n_rows": len(sub),
                "score_share": _share(labels),       # gate input
            }
    ↓
save_local(...)                      # models/risk_score/risk_v1.joblib + metadata.json
log_to_mlflow(..., promote=True)     # registers device-risk-score vN → Production
```

### 4.2 Algorithm choices — why each one

| Choice | Why |
|---|---|
| **Isolation Forest** vs OCSVM / LOF / autoencoder | Closest match to *"unusual within this fleet's distribution"* without OCSVM's parametric assumptions, LOF's O(n²) memory cost, or a deep model's labelled-data appetite. Returns a smooth anomaly score (not a binary label), which is what the dashboard bands need. |
| `n_estimators=200`, `random_state=42` | Same features → same scores every time. 200 trees is the sklearn-default sweet spot — IF stability without paying for a tail of barely-contributing trees. |
| `contamination="auto"` | We don't know the prior probability of *"anomalous"* for a Tunisian fleet. Sklearn picks the offset from the training score distribution — bands come from quantiles downstream anyway. |
| **Per-tenant, not global** | Each tenant's *"normal"* is genuinely different. A global fit on the union would mostly flag the smaller tenants as anomalous because their data lives on the tails. Per-tenant fits also let the promotion gate compare apples-to-apples. |
| **Per-tenant `StandardScaler`** | IF is theoretically scale-invariant, but the API returns per-feature z-scores as **components** — those need a fitted scaler. Per-tenant fitting also keeps components meaningful (*"1.5 σ above this tenant's mean idle ratio"*). |
| **Linear rescale `raw → [0,100]` with fit-time bounds** | The raw `-decision_function` output is unbounded and unitless. Rescaling with fit-time `raw_min` / `raw_max` gives the API a stable dashboard-friendly range. Live values outside the fit window are clipped so a wildly-anomalous row reads as 100, not 137. |
| **Per-tenant quantile thresholds (50 / 80 / 95)** | Each tenant's category split adapts to its own distribution — a fleet where everyone drives fast doesn't have its whole population flagged *"critical"*. Operationally meaningful framing: *"top 5 % within your fleet"*. |
| **Single bundled artifact** `{tenant_id: {scaler, model, raw_min/max, thresholds, score_share}, ...}` | Atomicity: rollback restores every tenant's model together, never a mix. |
| **No per-factor weights** | IF learns the structure itself from the multivariate distribution. The *components* the API returns are z-scores, not weight contributions — honest about what the model exposes. |

### 4.3 Outputs

| Where | Content |
|---|---|
| `models/risk_score/risk_v1.joblib` | Pickled bundled dict (all tenants) |
| `models/risk_score/metadata.json` | `{version, trained_at, model_name, feature_order, tenants, rows_by_tenant, skipped_tenants, tenant_stats, overall_share, n_rows, …}` |
| MLflow run | Params + `overall_share_{cat}` metrics + per-tenant `tenant_{tid}_share_critical/_high/_n_rows` |
| MLflow Registry | `device-risk-score` v`N` at stage `Production` |

---

## 5. Modeling — Step 4: Prediction surfaces

| Predictor | API endpoint | Returns |
|---|---|---|
| `ClusterPredictor` | `POST /v1/score/cluster` | `{cluster_id, distance, model_name, model_version}` |
| `RiskPredictor` | `POST /v1/score/risk` | `{risk_score, category, components, model_version}` |
| (admin) | `POST /v1/admin/reload-model` | Forces `predictor.reload()` after a promotion, no restart needed |

Both predictors load with the same lazy strategy:

1. Try MLflow registry: `models:/device-clustering/Production` (or `device-risk-score`).
2. Fallback to local `models/*/`.joblib`.
3. If both fail, stay *unloaded* — `/health` still returns 200 (so the API
   can be deployed before the first training run); the score endpoint
   returns **HTTP 503** with a *"no model"* message.

**Risk-specific:** `tenant_id` is **required**. A request for a tenant the
loaded artifact doesn't know returns **HTTP 503** with reason
`tenant_model_missing` — the dashboard surfaces this as *"feature not
ready for this tenant"* rather than a generic 5xx alarm.

**Batch scoring** (`ml/batch_scoring.py`) is the same math vectorised over
the whole month — ~1000× faster than per-row calls — then upserted into
the fact tables via **delete-then-insert in one transaction** (so partial
failures leave prior state intact). Every row stamps `model_version`,
`model_source`, `_etl_run_id` for post-hoc auditing.

---

## 6. Evaluation — four cadences

| When | What we check | Where the code lives |
|---|---|---|
| Every training run | Required tenant coverage + silhouette over K | `_validate_tenant_coverage`, `fit_clustering` |
| Every CI / commit | SQL ≡ Python parity + gate decision logic | `tests/test_feature_computation.py`, `tests/test_promotion.py`, `tests/test_drift.py` |
| Every batch scoring run | Cluster sizes + model-version stamping + PSI drift | `ml/batch_scoring.py`, `ml/drift.py` |
| Every retrain run | Silhouette gate (clustering) / stability gate (risk) | `ml/promotion.py` |

### 6.1 Silhouette score — K-selection metric (clustering)

For each K ∈ {3, 4, 5, 6}:

```
silhouette(i) = (b(i) − a(i)) / max(a(i), b(i))
```

where `a(i)` = mean intra-cluster distance, `b(i)` = mean distance to the
nearest *other* cluster. Overall silhouette = mean over all points,
range `[-1, +1]`.

| Range | Interpretation |
|---|---|
| ≥ 0.50 | Strong, well-separated clusters |
| 0.25 – 0.50 | Reasonable structure |
| 0.00 – 0.25 | Weak / overlapping — borderline |
| < 0 | Wrong K (points closer to a different cluster than their own) |

The most recent training run scored **0.2258** at K = 3 — the lower end
of *"reasonable"* for 13-D behavioural data with overlapping driving
styles. The K that maximises silhouette is the one shipped; logged as
MLflow metric `silhouette` on every run.

### 6.2 Cluster-size sanity check

`metadata.json` records `cluster_sizes = {0: n0, 1: n1, ...}` (also logged
as `cluster_<i>_size` metrics). A healthy run has comparable sizes — no
cluster < 1 % of the population. A degenerate run shows one giant
*"everything-else"* cluster and 2–3 micro-clusters; silhouette catches
this most of the time but the size dict is the human-readable
confirmation.

### 6.3 SQL ≡ Python parity tests

Two integration tests in `tests/test_feature_computation.py` pin the
contract that the API and the dashboard agree numerically:

- **`test_cluster_predictor_matches_persisted_assignment`** — pulls 25 rows
  from `fact_device_cluster_assignment` *filtered to the same
  `model_version` the predictor currently loads*, re-runs
  `ClusterPredictor.predict()` on the matching features in
  `v_ml_features_full`, asserts cluster IDs match exactly.
- **`test_risk_predictor_matches_persisted_fact`** — same shape against
  `fact_device_risk_score`, asserts `|py − sql| ≤ 0.1` per row (small
  floor for IF `decision_function` numerics). Tenants the loaded artifact
  doesn't know are skipped, not failed.

Both self-skip when Postgres is unreachable so the unit tier still
passes anywhere.

### 6.4 Model-version stamping (post-hoc auditability)

Every fact row carries `model_version` + `model_source`. So we can:

- Backfill compare: *"for July 2026, how did v3 vs v4 assign tenant 235?"*
- Detect stale partitions: *"any month where `model_version` ≠ current
  Production?"*
- Investigate complaints: *"why did device X's category change from
  `moderate` to `high`?"* — the version column tells you whether it was
  real-world behaviour change or a model refit.
- Parity tests filter by version, so a fresh Production model doesn't
  invalidate historical facts.

### 6.5 PSI feature drift — `ml/drift.py::detect_drift_for_months`

After every batch scoring run, the Prefect flow computes the **Population
Stability Index** between two windows of `v_ml_features_full`:

- **Reference window**: 6 calendar months immediately preceding the
  earliest touched month (anchored to `min(current_months)`, not
  *"today"* — so a historical backfill measures against its contemporary
  baseline).
- **Current window**: the months the batch flow just (re)scored.

Per feature, 10 quantile bins are derived from the reference, then:

```
PSI = Σ_i (p_cur,i − p_ref,i) · ln(p_cur,i / p_ref,i)
```

with `PSI_EPSILON = 1e-6` flooring empty bins to keep `log()` finite.

Banking-monitoring thresholds (used by SAS, FICO):

| PSI | Interpretation |
|---|---|
| < 0.10 | Stable |
| 0.10 – 0.25 | Moderate change |
| ≥ 0.25 | Significant drift (the configured alert threshold) |

Drift is **informational** — the Prefect task that wraps
`detect_drift_for_months` never raises. A flow that drift-checks itself
shouldn't fail because the model is suddenly stale; it should keep
loading data and surface the problem so humans can decide whether to
retrain.

Edge cases that return `PSI = 0` (rather than `NaN`) — so downstream
comparisons stay trivial: empty reference or current sample, and a
reference distribution that is a single repeated value. All pinned by 5
tests in `tests/test_drift.py`.

### 6.6 PSI score drift — `compute_score_drift` (v0.6 addition)

Feature drift catches input-side shifts *before* they hurt the model.
**Score drift** catches output-side shifts *after* the model has spoken.
Same 6-month window logic, but PSI is computed directly over the
`risk_score` column of `fact_device_risk_score`.

Returns `None` (not `0.0`) for two edge cases the promotion gate cares
about:

- Empty current window — no scored months → no signal.
- Empty reference window — cold start, never scored before → the gate
  can't decide on PSI yet. Returning `None` lets the gate distinguish
  *"PSI unknown"* from *"PSI = 0 ⇒ stable"*.

When PSI is finite and ≥ 0.25, the **risk promotion gate** (§ 6.7.2)
holds. Pinned by 5 tests in `tests/test_drift.py` — including
`test_compute_score_drift_queries_reference_then_current` which asserts
the function issues *two* SQL queries in the right order and that the
current window is excluded from the reference.

### 6.7 Gated promotion — `ml/promotion.py`

Both models share the same shape: train → register → gate-check before
the new version takes Production traffic. The decision functions
(`decide_promotion`, `decide_risk_promotion`) are **pure** — no I/O — so
17 tests in `tests/test_promotion.py` cover every branch. The wrappers
(`retrain_with_gate`, `retrain_risk_with_gate`) are **fail-soft** at the
MLflow boundary:

- If MLflow isn't installed → `get_current_production_*()` returns `None`
  → cold-start branch (allow promotion).
- If `mlflow.transition_model_version_stage` fails →
  `_transition_to_production` returns `(False, reason)` and the result's
  `reason` is appended with the failure message, so a *"gate passed but
  transition failed"* case is loud in logs and metrics.

Local joblib artifacts are written **before** registry registration so
the API's disk-fallback path always has something to load even if MLflow
is unavailable.

#### 6.7.1 Silhouette gate — clustering

```
promote IFF candidate_silhouette ≥ current_silhouette − tolerance
```

with `DEFAULT_SILHOUETTE_TOLERANCE = 0.02` (clustering literature:
± 0.01 is noise, ± 0.02 is meaningful but small, ± 0.05+ is real). The
tolerance is configurable per run via `--tolerance`.

| Case | Outcome | Reason string |
|---|---|---|
| No current Production model | **promote** (cold start) | `no_current_production_model` |
| candidate ≥ current − tolerance | **promote** | `candidate <c> >= floor <f> (current <cur> - tol <t>)` |
| candidate < current − tolerance | **hold** | `silhouette regression: candidate <c> < floor <f> (current <cur> - tol <t>)` |
| Exactly at floor (candidate == floor) | **promote** (inclusive boundary) | promotion reason as above |
| `tolerance = 0` | **strict ≥** non-regression | promotion reason as above |

#### 6.7.2 Stability gate — risk

The risk model is unsupervised — there is no held-out label and no
silhouette to point at — so we cannot promote on a single quality scalar.
Instead, the stability gate combines **three channels**:

```
promote IFF
  Δ %critical_share ≤ critical_tolerance_pp           (default 5 pp)
  AND Δ %high_share  ≤ high_tolerance_pp              (default 5 pp)
  AND (score_psi is None OR score_psi < 0.25)
```

Δ-shares are computed against the previous Production artifact's
persisted `score_share` dict (per-band shares averaged across tenants at
training time, stored *in the bundle*). Score PSI comes from
`compute_score_drift` (§ 6.6).

**Intuition:** a model that suddenly classifies twice as many devices
*"critical"* is either right (real-world fleets got dramatically worse —
vanishingly unlikely month-on-month) or wrong (regression in the IF
artifact, bad features, retraining artefact). Either way, do NOT
silently promote it — hold for human review.

| Case | Outcome |
|---|---|
| No current Production model | **promote** (cold start) |
| All three channels within tolerance | **promote** |
| Δ %critical > 5 pp | **hold** — reason cites both shares + tolerance |
| Δ %high > 5 pp | **hold** |
| score_psi ≥ 0.25 | **hold** |
| `score_psi is None` | counted as *"no PSI veto"* (cold start for the score column) |
| Multiple channels violate | **hold** — reasons concatenated |
| Per-call tolerance override | passed through verbatim, no clamping |

10 risk-side tests in `tests/test_promotion.py` pin every branch.

### 6.8 Notebook-based diagnostics

Read-only diagnostics in `notebooks/05_evaluation/`:

- `01_cluster_quality.ipynb` — per-cluster z-score heatmap, used to
  re-label `web/src/lib/clusters.ts` after a retrain (cluster IDs are
  not semantically stable across re-trainings).
- `02_risk_score_validation.ipynb` — per-tenant distribution analysis of
  the IF risk score and category bands, sanity checks against
  `fact_device_risk_score` confirming persisted bands match artifact
  thresholds. Visualises the `score_share` dict the promotion gate keys
  off.
- `03_stability_and_fairness.ipynb` — coverage, edge cases, per-tenant
  drift on both models.

These are read-only diagnostics; the production gates live in code.

---

## 7. End-to-end flow

```
                       ┌─────────────────────────────────────────────┐
                       │           marts.v_ml_features_full          │
                       │   one row per (tenant, device, year_month)  │
                       │   13 features (shared by both models)       │
                       │   + audit columns (all 35 baseline cols)    │
                       └────────────────────┬────────────────────────┘
                                            │
              ┌─────────────────────────────┼──────────────────────────────┐
              ▼                             ▼                              ▼
   ┌──────────────────────────┐ ┌──────────────────────────┐  ┌──────────────────────────┐
   │  RISK (IsolationForest)  │ │  CLUSTERING (KMeans)     │  │  BATCH SCORING (ETL)     │
   │  ONE model per tenant    │ │  StandardScaler          │  │  loads both predictors,  │
   │  ↓                       │ │  ↓                       │  │  vectorised over months  │
   │  StandardScaler (tenant) │ │  KMeans(n_init=10,       │  │  ↓                       │
   │  ↓                       │ │     random_state=42)     │  │  DELETE+INSERT in 1 tx:  │
   │  IF(n_est=200,           │ │  ↓                       │  │   fact_device_cluster_   │
   │     contamination='auto')│ │  silhouette over K=3..6  │  │     assignment           │
   │  ↓                       │ │  ↓                       │  │   fact_device_risk_score │
   │  −decision_function →    │ │  best (kmeans, scaler)   │  │  ↓                       │
   │   raw anomaly score      │ │  ↓                       │  │  PSI feature drift +     │
   │  ↓                       │ │  joblib + MLflow         │  │  PSI score drift (info)  │
   │  rescale [0,100]         │ │  registry (gate-checked) │  │                          │
   │  per-tenant quantiles    │ │                          │  │                          │
   │  → low/mod/high/critical │ │                          │  │                          │
   │  bundled artifact dict   │ │                          │  │                          │
   └────────────┬─────────────┘ └────────────┬─────────────┘  └──────────────────────────┘
                │ at API runtime             │ at API runtime
                ▼                            ▼
   ┌────────────────────────────┐ ┌──────────────────────────────┐
   │   RiskPredictor (lazy)     │ │   ClusterPredictor (lazy)    │
   │   MLflow → joblib → 503    │ │   MLflow → joblib → 503      │
   │   503 if tenant_id missing │ │                              │
   │   POST /v1/score/risk      │ │   POST /v1/score/cluster     │
   │   POST /v1/admin/reload    │ │   POST /v1/admin/reload      │
   └────────────────────────────┘ └──────────────────────────────┘
```

---

## 8. Retraining cadence

Both models have the same three-tier surface — ad-hoc CLI for the first
run, gated CLI for monthly retrains, supercronic profile for scheduled
execution.

| Cadence | Clustering | Risk |
|---|---|---|
| **Ad-hoc / first run** | `python scripts/train_clustering.py` | `python scripts/train_risk_score.py` |
| **Gated retrain** | `python scripts/retrain_monthly.py --tolerance 0.02` (silhouette gate) | `python scripts/retrain_risk_monthly.py --critical-tolerance-pp 5 --high-tolerance-pp 5` (stability gate) |
| **Scheduled monthly** | `docker compose --profile scheduler up -d retrain-scheduler` — supercronic fires every Mon 04:00 UTC; shell-gates to **first Monday of each month** | Same scheduler container fires both scripts |

**Exit codes** (both scripts):

- `0` — gate passed and promotion succeeded.
- `0` — gate held (regression detected; operator decides — not a CLI error).
- `1` — retraining or MLflow plumbing failed before the gate could decide.

After a successful promotion, hit `POST /v1/admin/reload-model` (with
`X-API-Key`) on the API so the running process picks up the new
artifacts without a restart. Both `ClusterPredictor.reload()` and
`RiskPredictor.reload()` are thread-safe and reload independently.

---

## 9. Reproducibility checklist (for the jury)

Every result in this chapter can be reproduced by:

1. **Same data** — `marts.v_ml_features_full` is a deterministic view;
   row contents are pinned by `model_version` + `_etl_run_id` columns in
   the fact tables.
2. **Same hyper-parameters** — `random_state=42` everywhere,
   `n_estimators=200`, `n_init=10`, `K ∈ {3,4,5,6}`,
   `contamination="auto"` — all in code + `config/feature_definitions.yaml`.
3. **Same artifact** — MLflow Model Registry stages every promoted
   version + local joblib fallback. Rollback = transition previous
   version back to Production + `POST /v1/admin/reload-model`.
4. **Same evaluation logic** — pure-function gates (`decide_promotion`,
   `decide_risk_promotion`) with 17 unit tests covering every branch.
5. **Same drift answer** — `compute_psi` + `compute_score_drift` mocked
   at the pandas boundary for unit tests; the same code runs in
   production against real Postgres.

---

## 10. Cross-references

### 10.1 Code entry points

- Clustering training — `scripts/train_clustering.py`
- Clustering gated retrain — `scripts/retrain_monthly.py`
- Risk training — `scripts/train_risk_score.py`
- Risk gated retrain — `scripts/retrain_risk_monthly.py`

### 10.2 Library code

- Clustering training — `src/accent_fleet/ml/train_clustering.py`
- Risk training — `src/accent_fleet/ml/train_risk.py`
- Promotion gates (both models) — `src/accent_fleet/ml/promotion.py`
- Drift monitor (feature + score PSI) — `src/accent_fleet/ml/drift.py`
- Inference (`ClusterPredictor`, `RiskPredictor`) — `src/accent_fleet/ml/inference.py`
- Batch scoring (cluster + risk) — `src/accent_fleet/ml/batch_scoring.py`
- Prefect flow — `src/accent_fleet/pipeline/flow_batch.py`

### 10.3 SQL

- Risk score fact (new in v0.6) — `sql/28_fact_device_risk_score.sql`
- Risk score compat view — `sql/21_v_device_risk_profile.sql`
- Cluster assignment fact — `sql/27_fact_device_cluster_assignment.sql`
- Feature contract — `sql/26_v_ml_features_full.sql`
- Mart refresh — `sql/41_refresh_marts.sql`

### 10.4 Config

- IF risk-model hyper-parameters — `config/feature_definitions.yaml::risk_score_model`
- Tenant cohort + promotion tolerances — `config/pipeline.yaml`

### 10.5 Tests

- Per-tenant IF training math — `tests/test_train_risk.py`
- `RiskPredictor` inference — `tests/test_inference.py`
- Promotion gates (both models) — `tests/test_promotion.py`
- Drift (feature PSI + score PSI) — `tests/test_drift.py`
- Batch scoring (cluster + risk) — `tests/test_batch_scoring.py`
- API score routes — `tests/test_api_score.py`
- SQL ≡ Python parity — `tests/test_feature_computation.py`

### 10.6 Companion documents

- Engineering deep dive — [`docs/MODELING_AND_EVALUATION.md`](../docs/MODELING_AND_EVALUATION.md)
- Data-preparation companion — [`docs/DATA_PREPARATION.md`](../docs/DATA_PREPARATION.md)
- Project overview — [`docs/HOW_IT_WORKS.md`](../docs/HOW_IT_WORKS.md)
