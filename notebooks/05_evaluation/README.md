# M10 · Evaluation (CRISP-DM Phase 5) — Verdict

**Date:** 2026-05-07
**Project:** Device Behavior Scoring & Risk Classification (P1)
**Models evaluated:** per-tenant K-Means (notebook `04_modeling/01`) and per-tenant Isolation Forest (`04_modeling/02`)

---

## 1. Notebooks in this folder

| File | Role |
| --- | --- |
| `01_cluster_quality.ipynb` | Internal-validity (silhouette, Davies-Bouldin, Calinski-Harabasz), cluster-size balance, auto-named persona profiles, z-score heatmap. |
| `02_risk_score_validation.ipynb` | External validation against `warehouse.fact_maintenance` (concurrent + predictive + top-decile lift) **and** internal-consistency fallback when ground truth is unavailable. |
| `03_stability_and_fairness.ipynb` | Month-to-month assignment churn, risk-band transition matrix, per-tenant band distribution, scoring coverage. |

All three notebooks are self-contained: they re-fit the models from `marts.v_ml_features_full` (deterministic, `random_state=42`) so they do not depend on the parquet artifacts from `04_modeling/`.

The notebook generator is `scripts/build_eval_notebooks.py`. Re-running it overwrites the three `.ipynb` files; the JSON is round-trip-validated with `nbformat`.

---

## 2. Headline numbers (smoke-tested live against the database)

### 2.1 Cluster quality (`01_cluster_quality.ipynb`)

| tenant | n | k | silhouette | Davies-Bouldin | Calinski-Harabasz |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 235  | 613 | 3 | 0.355 | 1.730 | 102.9 |
| 238  | 339 | 6 | 0.237 | 1.165 |  72.2 |
| 264  | 354 | 6 | 0.174 | 1.587 |  52.5 |
| 1787 | 417 | 6 | 0.278 | 1.220 |  81.9 |

- All four tenants meet the silhouette gate (>= 0.15) and the `k >= 3` gate.
- **Tenant 235 fails the size-balance gate**: cluster 0 holds 82.4% of rows. The other two clusters (4.9%, 12.7%) are real but the model is essentially a "normal vs two outlier groups" split rather than three balanced personas.
- Tenants 238 / 264 / 1787 have at most 50% in any single cluster — balanced.

### 2.2 Risk score validation (`02_risk_score_validation.ipynb`)

**External (maintenance-based) validation: BLOCKED.**

Maintenance ground truth is in tenant **7486 only** (136 events, 2025-01..2026-02). The four modeled tenants — **235, 238, 264, 1787** — have **zero** rows in `warehouse.fact_maintenance`. Tenant 7486 in turn has zero rows in `fact_trip` and therefore zero rows in `marts.v_ml_features_full`. The two populations are disjoint.

This is a **source-data coverage gap**, not a model failure. We surface it explicitly in cell §2a of notebook 02.

**Internal-consistency validation: PASSES.** Per-tenant ratio of unsafe-feature mean (high band ÷ low band):

| tenant | features w/ ratio>1 (of 7) | features w/ ratio>=2 | mean ratio |
| ---: | ---: | ---: | ---: |
| 235  | 6 | 5 |  18.79 |
| 238  | 6 | 4 | 143.69 |
| 264  | 5 | 4 |  19.93 |
| 1787 | 6 | 5 |   9.02 |

Every tenant clears the gate (>= 5 features w/ ratio>1, >= 2 w/ ratio>=2). Concrete sample (tenant 235): high-band rows have 33x more overspeeds, 14x more harsh brakes, 23x more harsh accels, 32x more harsh corners, 10x more high-RPM minutes than low-band rows in the same tenant cohort. The score is identifying genuinely-aggressive device-months even without labels.

### 2.3 Stability + fairness (`03_stability_and_fairness.ipynb`)

| tenant | n_pairs | cluster churn | band churn |
| ---: | ---: | ---: | ---: |
| 235  | 507 | 0.055 | 0.116 |
| 238  | 279 | 0.140 | 0.108 |
| 264  | 295 | 0.075 | 0.153 |
| 1787 | 326 | 0.129 | 0.107 |

Risk-band month-over-month transition matrix (row-normalised):

|       | low | medium | high |
| ----- | ---: | ---: | ---: |
| **low**    | 0.935 | 0.063 | 0.002 |
| **medium** | 0.292 | 0.639 | 0.069 |
| **high**   | 0.050 | 0.350 | 0.600 |

- Cluster churn 5.5%-14% — well under the 60% gate.
- Band churn 10.7%-15.3% — well under the 50% gate.
- Diagonal: P[low->low]=0.94, P[high->high]=0.60 — both >= 0.5.
- Per-tenant band shares: max is 82.9% (tenant 238 in low band). With `contamination='auto'` on Isolation Forest a low-band share of ~80% is by design, not a defect. No tenant is collapsed to >90%.

---

## 3. Go / No-go per artifact

| Artifact | Verdict | Reason |
| --- | --- | --- |
| **Per-tenant cluster labels (235, 238, 264, 1787)** | SHIP for tenants **238 / 264 / 1787**; **gate** tenant **235** behind a "k=3 binary outlier flag, not three personas" caveat. | Silhouette + persona gates pass for all four; size-balance fails for 235 (82% in cluster 0). |
| **Risk score (Isolation Forest, [0,1])** | SHIP **provisionally** for all four tenants with a dashboard footer disclosing the maintenance-coverage gap. | Internal-consistency gate clears strongly across every tenant; stability gates clear; external validation blocked by source-data overlap, not by the model. |
| **Risk band (low / medium / high)** | SHIP. | Transition diagonal is sticky (P[stay] >= 0.6 for low and high); churn under 16% per tenant. |

**Mandatory dashboard footer text** (paste-ready, do not soften):

> Risk scores were validated against feature-space anomalies. Outcome backtesting against maintenance events is blocked by source data coverage in `staging.maintenance` for the modeled tenants and is pending integration of an incident or claims feed.

---

## 4. What this evaluation cannot tell you (and what to do about it)

1. **Whether high-risk device-months actually cause maintenance events.** No overlap. Action: ingest `staging.sinistre` once it has rows, or extend `fact_maintenance` ingestion to cover the modeled tenants.
2. **Whether the score generalises to new tenants.** Each model is fit on its own tenant — we have no held-out tenant test. Action: when a fifth tenant is added, fit the model on it and re-run notebook 02 §7 to confirm the internal-consistency gate holds without any per-tenant tuning.
3. **Whether the personas are stable across re-fits with different random states.** The current notebooks use a fixed `random_state=42`. Action (low priority): a small sensitivity sweep over 5 seeds, expecting persona names to remain in the same family.

---

## 5. Hand-off to `06_deployment/`

Inputs the deployment phase can rely on:

- `data/ml/device_clusters.parquet` — `(tenant_id, device_id, year_month, cluster)`. Re-generated by running `04_modeling/01`.
- `data/ml/device_risk_scores.parquet` — `(tenant_id, device_id, year_month, risk_score, risk_band)`. Re-generated by running `04_modeling/02`.
- These are the **only two** model outputs that should be wired into a dashboard. The persona names from `01_cluster_quality.ipynb` are descriptive labels, not a third artifact — render them at the BI layer.

Deployment must:

- Pin the dashboard footer disclosing the maintenance-coverage gap (text in §3 above).
- For tenant 235, hide the cluster column or label it "outlier flag" — do not surface three personas.
- Re-run all three evaluation notebooks every time the modeling window advances by a month and fail the deploy if any gate that previously passed flips to fail.
