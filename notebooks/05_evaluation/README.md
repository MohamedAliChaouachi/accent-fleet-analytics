# M10 · Evaluation (CRISP-DM Phase 5) — to be populated

Populated after the modeling notebooks in `04_modeling/` produce artifacts.

## What this phase must answer

1. **Technical metrics.** For clustering: silhouette, Davies-Bouldin, Calinski-Harabasz; cluster stability across months. For anomaly detection: precision@k against known incidents. For supervised: ROC-AUC, PR-AUC, calibration. For risk-score tuning: correlation of `risk_score` with downstream incidents.
2. **Business KPIs.** Do devices in the top-10% risk bucket actually have more incidents, more maintenance events, higher fuel costs? This requires joining predictions back to `staging.maintenance`, `staging.offense`, `staging.sinistre`, etc.
3. **Fairness across tenants.** Does the model perform equally across the 5 tenants, or does one tenant dominate?
4. **Stability over time.** Retrain monthly — do segment assignments flip erratically? (They should not.)

## Deliverable format

Each evaluation notebook ends with a short written verdict in markdown:

> **Go / No-go:** …
> **Next action:** …

Do not merge a model into deployment without that verdict.
