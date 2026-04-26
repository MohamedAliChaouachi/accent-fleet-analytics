# Accent Fleet Analytics — CRISP-DM Phase 3 (Refactored)

Stream-ready data preparation pipeline for **Project P1: Driver Behavior Scoring & Risk Classification**.

> This is the refactored Phase 3 deliverable. It replaces the batch-only SQL-script pipeline from v1 with an incremental, watermark-driven, testable Python + SQL pipeline that is ready for streaming ingestion.
>
> For the why of the refactor, see [`REFACTOR_RATIONALE.md`](./REFACTOR_RATIONALE.md).
> For the original Phase 1+2 business/data understanding, see the parent project docs.

---

## Quick start

The canonical development workflow is **notebook-first**. Scripts remain the production entry point, but every step is also available as a standalone notebook so you can see and inspect each CRISP-DM phase on its own.

```bash
# 1. Environment
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env               # then edit DB credentials

# 2. Launch Jupyter and run the notebooks in order — see notebooks/ layout below
jupyter lab

# 3. Once the warehouse is fully built, schedule incremental runs on the Azure VM
python scripts/run_batch.py --mode incremental   # driven by cron, see notebooks/06_deployment/
```

Stream mode (`scripts/run_streaming.py`, `pipeline/flow_stream.py`, `ingestion/stream_source.py`) is currently **deferred** and lives under `_deferred/` folders. It is preserved, not deleted, and will be reactivated later.

---

## Architecture

```
  staging (bronze)           warehouse (silver)              marts (gold)
  ─────────────────          ──────────────────              ─────────────────────
  path                ┐      dim_tenant                ┌──► mart_device_monthly_behavior   (ML)
  stop                │      dim_vehicle               │    mart_device_monthly_telemetry  (ML)
  rep_overspeed       │      dim_device                ├──► mart_fleet_daily               (BI day)
  rep_activity_daily  │      dim_driver                │    mart_vehicle_monthly           (BI vehicle)
  notification        │      dim_date / dim_hour_band  │    mart_tenant_monthly_summary    (BI rollup)
  archive             │      bridge_device_driver      │
  vehicule / device   ├──►   fact_trip                 │    Views — ML
  driver / assignment │      fact_overspeed            │      v_device_risk_profile
  maintenance         │      fact_stop                 │      v_ml_features
  offense / sinistre  │      fact_speed_notification ──┤      v_ml_features_full
  reparation          │      fact_daily_activity       │      v_fleet_risk_dashboard
  document / fueling  │      fact_harsh_event          │
                      │      fact_telemetry_daily      │    Views — BI
                      │      fact_notification         │      v_executive_dashboard
                      │      fact_maintenance          │      v_operational_dashboard
                      │      fact_maintenance_line     │      v_maintenance_dashboard
                      │      fact_fueling              │
                      └►     etl_watermark ◄────── drives incremental loads
                             etl_run_log
                             quarantine_rejected
```

Two consumer projects feed off the same warehouse:

- **Project 1 — Driver Behavior Scoring (ML).** Uses the device-grain monthly marts.
- **Project 2 — Fleet BI Dashboard.** Uses the day-/vehicle-/tenant-grain BI marts and views.

Every arrow is an **incremental, idempotent, watermark-driven transformation**. Running the pipeline twice on the same window produces the same output; running it once extends the watermark forward.

---

## Repository layout

```
accent-fleet-analytics/
├── config/                # YAML: cleaning rules, feature defs, pipeline params
│   ├── cleaning_rules.yaml
│   ├── feature_definitions.yaml
│   └── pipeline.yaml
├── sql/                   # Idempotent, templated SQL transforms
│   ├── 00_schemas_and_state.sql
│   ├── 01_dim_tenant.sql … 05_dim_date_hour.sql
│   ├── 10_fact_trip_incremental.sql … 14_fact_daily_activity_incr.sql
│   ├── 07_bridge_device_driver_load.sql           # NEW (driver↔device SCD)
│   ├── 15_fact_harsh_event_incremental.sql        # archive accelerometer
│   ├── 16_fact_telemetry_daily_incr.sql           # archive aggregates
│   ├── 17_fact_notification_incr.sql              # NEW (BI: full notifications)
│   ├── 18_fact_maintenance_incr.sql               # NEW (BI: maintenance header)
│   ├── 19_fact_maintenance_line_incr.sql          # NEW (BI: offense/sinistre/reparation)
│   ├── 20_mart_device_monthly_behavior.sql
│   ├── 21_v_device_risk_profile.sql
│   ├── 22_v_ml_features.sql
│   ├── 23_v_fleet_risk_dashboard.sql
│   ├── 24_fact_fueling_incr.sql                   # NEW (BI: fueling events)
│   ├── 25_mart_device_monthly_telemetry.sql       # archive-side mart
│   ├── 26_v_ml_features_full.sql                  # unified ML view
│   ├── 30_mart_fleet_daily.sql                    # NEW (BI mart: day-grain)
│   ├── 31_mart_vehicle_monthly.sql                # NEW (BI mart: vehicle-month)
│   ├── 32_mart_tenant_monthly_summary.sql         # NEW (BI mart: tenant-month)
│   ├── 33_v_executive_dashboard.sql               # NEW (BI view: exec)
│   ├── 34_v_operational_dashboard.sql             # NEW (BI view: ops)
│   ├── 35_v_maintenance_dashboard.sql             # NEW (BI view: maintenance)
│   └── 99_validation_suite.sql
├── src/accent_fleet/      # The Python package (reused by both scripts and notebooks)
│   ├── config.py          # Pydantic settings + YAML loading
│   ├── db/                # SQLAlchemy engine, watermark, SQL loader
│   ├── contracts/         # Pydantic row-level schemas
│   ├── cleaning/          # Config-driven cleaning rule engine
│   ├── transforms/        # Dim / fact / feature transforms
│   ├── ingestion/         # Batch source (stream source in _deferred/)
│   ├── pipeline/          # Prefect batch flow (stream flow in _deferred/)
│   ├── features/          # Feature registry + risk score
│   └── monitoring/        # Row counts, freshness, null-rate checks
├── notebooks/             # CRISP-DM-aligned notebooks (see below)
│   ├── 00_setup/                      # M0-M1: env check, DDL
│   ├── 01_data_understanding/         # M2: Phase-2 replay, cleaning exploration
│   ├── 02_data_preparation/           # M3-M7: Phase-3 heart
│   │   ├── cleaning/                  # M4: rule-engine preview
│   │   ├── dimensions/                # M3: load dims
│   │   ├── facts/                     # M5: one notebook per fact backfill
│   │   ├── marts/                     # M6: mart + views
│   │   └── validation/                # M7: DQ suite
│   ├── 03_feature_engineering/        # M8: EDA bridging to modeling
│   ├── 04_modeling/                   # M9: placeholder (Phase 4)
│   ├── 05_evaluation/                 # M10: placeholder (Phase 5)
│   ├── 06_deployment/                 # M11: cron recipe + incremental demo
│   └── _legacy/                       # earlier demo notebooks, kept for reference
├── scripts/               # Batch entry point (stream entry point in _deferred/)
├── tests/                 # Pytest suite
├── docker-compose.yml     # Local Postgres + optional Redpanda
├── pyproject.toml
├── requirements.txt
└── .env.example
```

---

## Running in VS Code

The repo ships with `.vscode/` settings for Python, Jupyter, and SQL. Open the folder and VS Code will prompt to install the recommended extensions. The `launch.json` includes three debug targets: `Bootstrap`, `Incremental Batch`, and `Streaming`.

Notebooks live in `notebooks/` and are wired to use the same `accent_fleet` package as the scripts — you can import any pipeline component into a cell for interactive development.

---

## Testing

```bash
pytest                              # full suite
pytest tests/test_cleaning_rules.py -v   # a single module
pytest -k "incremental"             # by keyword
```

All 7 cleaning rules (C1–C7) have dedicated unit tests. The incremental-semantics test proves that two sequential runs produce identical output (idempotency).

---

## Pipeline modes

| Mode | Trigger | Window | Use case |
|---|---|---|---|
| `bootstrap` | Manual, once | — | Create schemas + state tables + **full-refresh dimensions** + mart DDL + views. *Not just DDL — also loads 5 dimension tables.* |
| `backfill` | Manual, once | Full history | Process the 54.7 M archive rows / 7.4 M trips already in staging. Decomposed per-fact in `notebooks/02_data_preparation/facts/`. |
| `incremental` | Cron every 5 min | `now - overlap → now` | Normal operating mode. Pulls new rows since last watermark. |
| `stream` | *Deferred* | — | Future mode when IoT events stream directly. Code in `_deferred/`. |

---

## Milestones (map notebooks to CRISP-DM phases)

| Milestone | Notebook(s) | CRISP-DM |
|---|---|---|
| M0 | `00_setup/00_environment_check.ipynb` | Setup |
| M1 | `00_setup/01_create_schemas.ipynb` | Phase 3 (DDL) |
| M2 | `01_data_understanding/01_staging_profile.ipynb`, `…/02_cleaning_rules_exploration.ipynb` | Phase 2 replay |
| M3 | `02_data_preparation/dimensions/01_load_dimensions.ipynb` | Phase 3 |
| M4 | `02_data_preparation/cleaning/01_apply_cleaning_rules_preview.ipynb` | Phase 3 |
| M5 | `02_data_preparation/facts/01…05_*.ipynb` (one per fact) | Phase 3 |
| M5b | `02_data_preparation/facts/06_load_fact_harsh_event.ipynb`, `…/07_load_fact_telemetry_daily.ipynb` — archive-derived facts | Phase 3 |
| M6 | `02_data_preparation/marts/01…02_*.ipynb` | Phase 3 |
| M6c | `02_data_preparation/marts/03_build_mart_device_monthly_telemetry.ipynb` | Phase 3 |
| M6d | `02_data_preparation/marts/04_build_unified_ml_view.ipynb` (frozen ML contract) | Phase 3 |
| M7 | `02_data_preparation/validation/01_run_validation_suite.ipynb` | Phase 3 |
| M8 | `03_feature_engineering/01_explore_ml_features.ipynb` | Bridge to Phase 4 |
| M8a | `03_feature_engineering/02_handle_missing_values.ipynb` | Phase 3 (clean) |
| M8b | `03_feature_engineering/03_handle_outliers.ipynb` | Phase 3 (clean) |
| M8c | `03_feature_engineering/04_data_encoding.ipynb` (OHE / Ordinal / Target-Guided) | Phase 3 (transform) |
| M8d | `03_feature_engineering/05_handle_imbalanced_dataset.ipynb` (SMOTE) | Phase 3 (transform) |
| M8e | `03_feature_engineering/06_build_ml_dataset.ipynb` — exit gate, produces X/y parquet + preprocess.joblib | Phase 3 (final) |
| M9 | `04_modeling/README.md` (placeholder) | Phase 4 |
| M10 | `05_evaluation/README.md` (placeholder) | Phase 5 |
| M11 | `06_deployment/01_incremental_mode_demo.ipynb`, `02_scheduled_runs.md` | Phase 6 |

Run them in order — each notebook asserts its exit criterion in the last cell.

---

## What to read next

1. [`REFACTOR_RATIONALE.md`](./REFACTOR_RATIONALE.md) — why this design, mapped to v1.
2. [`notebooks/_template.ipynb`](./notebooks/_template.ipynb) — the 4-section notebook template.
3. [`notebooks/00_setup/00_environment_check.ipynb`](./notebooks/00_setup/00_environment_check.ipynb) — start here.
4. [`notebooks/06_deployment/02_scheduled_runs.md`](./notebooks/06_deployment/02_scheduled_runs.md) — cron recipe for the Azure VM.
