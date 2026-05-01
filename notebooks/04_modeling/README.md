# M9 · Modeling (CRISP-DM Phase 4) — Device Behavior Scoring

> **Renamed from "Driver Behavior Scoring" on 2026-04-30.**
> See `REFACTOR_RATIONALE.md` §6 and the EDA plan
> `~/.claude/plans/cheerful-herding-crown.md` for the rationale.

---

## What changed and why

The original project description proposed a **supervised** scoring model
trained against overspeed/alert frequency as proxy labels, with a success
criterion of "risk score correlates >0.7 with historical overspeed".

The EDA against the live warehouse on 2026-04-30 found that this path is
**not viable** on the current data:

1. **Driver attribution covers 1.9% of devices** (12 of 633). The unit
   of analysis is therefore the **device-month**, not the driver.
2. **Harsh-event activity and overspeed activity are inversely
   correlated** in the modeling window (3.5× lower overspeed in
   harsh-active devices). Using either as a proxy label argues the
   model against itself. The >0.7 correlation goal is structurally
   unreachable.
3. **Archive-derived signals (harsh / RPM / idle) are 99% absent before
   2025-01.** The realistic modeling window is 16 months (2025-01 to
   2026-04), ~5,000 device-month training rows, not "millions".
4. **Tenants 264 (overspeed-rich) and 1787 (harsh-rich)** are different
   operating regimes. A global model averages them out.

## Current direction

Two unsupervised, per-tenant notebooks. Each enforces:
- `WHERE year_month >= '2025-01'` (F3)
- per-tenant fitting (F4)
- device-month grain (F5)
- no proxy labels (F6)

| Notebook | Approach | Output | Project goal addressed |
|---|---|---|---|
| `01_device_behavior_clustering.ipynb` | StandardScaler → KMeans (k chosen by silhouette across 3..6) per tenant; PCA(2) for plotting only | `data/ml/device_clusters.parquet` with per-(tenant, device, month) cluster label + `cluster_profile` table per tenant | "Clusters reveal at least 3 distinct profiles" |
| `02_anomaly_risk_score.ipynb` | StandardScaler → Isolation Forest per tenant; raw decision_function rescaled to [0,1] | `data/ml/device_risk_scores.parquet` with per-(tenant, device, month) `risk_score` and `risk_band ∈ {low, medium, high}` | Operational risk score for ranking devices |

## Optional / future work (not implemented)

| Approach | Why not now |
|---|---|
| Supervised classification | Requires either a labelled incident feed (don't have one) or a coherent proxy label (data shows none exist). Re-evaluate if a workshop/incident table is added to source. |
| Deep sequence models on raw telemetry | Overkill for ~5k training rows. Reconsider once the 2025+ window has ≥3 years of accumulated data. |
| Risk-weight tuning of `feature_definitions.yaml` | Requires labelled month to optimize against. Defer until incidents exist. |

## Inputs that already exist and should be reused

- `marts.v_ml_features_full` — the **frozen** ML feature contract.
- `config/feature_definitions.yaml` — canonical feature list (still useful as documentation; no longer used as a model).
- `src/accent_fleet/features/registry.py` — typed accessor for the YAML.
