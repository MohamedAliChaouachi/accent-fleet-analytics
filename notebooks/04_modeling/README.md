# M9 · Modeling (CRISP-DM Phase 4) — to be populated

This folder will hold modeling notebooks once the warehouse is green (all milestones M1–M7 passing) and the feature EDA in `03_feature_engineering/01_explore_ml_features.ipynb` is complete.

## Candidate approaches

Pick after EDA; not before.

| Approach | Notebook (planned) | Target | Input features |
|---|---|---|---|
| K-Means clustering | `01_kmeans_driver_segments.ipynb` | Unsupervised segments (safe / moderate / aggressive) | `marts.v_ml_features_driver_behavior` — scale + PCA first |
| Isolation Forest | `02_isolation_forest_anomaly.ipynb` | Flag anomalous (tenant, device, month) rows | Same feature table |
| Risk-score tuning | `03_tune_risk_weights.ipynb` | Optimize the 6 weights in `config/feature_definitions.yaml` against a labeled month | View `v_device_risk_profile` |
| Supervised classification | `04_supervised_risk.ipynb` | Predict next-month incident (if labels exist) | Monthly features + lag features |

## Before you start any of these

1. Confirm `marts.mart_device_monthly_behavior` has ≥ 3 months of data per device for most devices.
2. Write down (in this README) which question you are answering and what "better" means for the chosen algorithm.
3. Decide how you will split train/validation/test — by month (temporal split) is usually right for fleet data.

## Inputs that already exist and should be reused

- `config/feature_definitions.yaml` — the canonical feature list (avoid re-inventing).
- `src/accent_fleet/features/registry.py` — typed accessor for the YAML.
- `src/accent_fleet/features/risk_score.py` — the baseline weighted model.
