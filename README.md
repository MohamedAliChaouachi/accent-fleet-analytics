# Accent Fleet Analytics — CRISP-DM Phase 3 (Refactored)

Stream-ready data preparation pipeline for **Project P1: Driver Behavior Scoring & Risk Classification**.

> This is the refactored Phase 3 deliverable. It replaces the batch-only SQL-script pipeline from v1 with an incremental, watermark-driven, testable Python + SQL pipeline that is ready for streaming ingestion.
>
> For the why of the refactor, see [`REFACTOR_RATIONALE.md`](./REFACTOR_RATIONALE.md).
> For the original Phase 1+2 business/data understanding, see the parent project docs.

---

## Quick start

```bash
# 1. Environment
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env               # then edit DB credentials

# 2. One-time DW + marts bootstrap (creates schemas, watermark table, dims)
python scripts/run_batch.py --mode bootstrap

# 3. Back-fill historical staging data into warehouse + marts
python scripts/run_batch.py --mode backfill

# 4. From here on: run every 5 minutes (cron / Prefect / systemd timer)
python scripts/run_batch.py --mode incremental

# Future: stream mode (requires Kafka / Redpanda)
python scripts/run_streaming.py
```

Or explore interactively with the notebooks under `notebooks/`.

---

## Architecture

```
  staging (bronze)           warehouse (silver)          marts (gold)
  ─────────────────          ──────────────────          ──────────────
  path                ┐      dim_tenant                  mart_device_
  stop                │      dim_vehicle                 monthly_behavior
  rep_overspeed       ├──► { dim_device      } ──────►  v_device_risk_profile
  notification        │      dim_driver                  v_ml_features_
  rep_activity_daily  │      dim_date                     driver_behavior
  vehicule / device   │      dim_hour_band                v_fleet_risk_dashboard
  driver / assignment ┘      fact_trip
                             fact_overspeed
                             fact_stop
                             fact_speed_notification
                             fact_daily_activity
                             etl_watermark ◄───── drives incremental loads
                             etl_run_log
                             quarantine_rejected
```

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
│   ├── 20_mart_device_monthly_behavior.sql
│   ├── 21_v_device_risk_profile.sql
│   ├── 22_v_ml_features.sql
│   ├── 23_v_fleet_risk_dashboard.sql
│   └── 99_validation_suite.sql
├── src/accent_fleet/      # The Python package
│   ├── config.py          # Pydantic settings + YAML loading
│   ├── db/                # SQLAlchemy engine, watermark, SQL loader
│   ├── contracts/         # Pydantic row-level schemas
│   ├── cleaning/          # Config-driven cleaning rule engine
│   ├── transforms/        # Dim / fact / feature transforms
│   ├── ingestion/         # Batch + stream sources (same interface)
│   ├── pipeline/          # Prefect flows (batch, streaming)
│   ├── features/          # Feature registry + risk score
│   └── monitoring/        # Row counts, freshness, null-rate checks
├── notebooks/             # Jupyter: profile → load → validate → demo
├── scripts/               # Entrypoints for CI / cron
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
| `bootstrap` | Manual, once | — | Create schemas, watermark table, static dimensions (date, hour_band). |
| `backfill` | Manual, once | Full history | Process the 54.7 M archive rows / 7.4 M trips already in staging. |
| `incremental` | Cron / Prefect every 5 min | `now - overlap → now` | Normal operating mode. Pulls new rows since last watermark. |
| `stream` | Kafka / CDC trigger | Per-message | Future mode when IoT events stream directly. Same transforms. |

---

## What to read next

1. [`REFACTOR_RATIONALE.md`](./REFACTOR_RATIONALE.md) — why this design, mapped to v1.
2. [`notebooks/01_data_profiling.ipynb`](./notebooks/01_data_profiling.ipynb) — live profile of staging.
3. [`notebooks/05_feature_engineering.ipynb`](./notebooks/05_feature_engineering.ipynb) — walk through the 35 features.
4. [`notebooks/06_stream_simulation.ipynb`](./notebooks/06_stream_simulation.ipynb) — replay path events to prove streaming works.
