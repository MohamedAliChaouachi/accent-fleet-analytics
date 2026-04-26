# CRISP-DM Phase 3 — Refactor Rationale

**Project:** P1 Driver Behavior Scoring & Risk Classification — Accent Fleet Analytics
**Refactor Date:** April 2026
**Refactor Scope:** Replace the batch-only, monolithic T&L pipeline with a stream-ready, incremental architecture.

---

## 1. Why refactor?

The v1 Phase 3 design (documented in `CRISP_DM_Phase3_Data_Preparation_Driver_Behavior_2v1.pdf`) described a valid star-schema star + feature mart, but it was built around three assumptions that are no longer true:

| v1 assumption | Reality after this refactor |
|---|---|
| Staging is a **one-time dump** that the team loads manually. | Staging will be fed **continuously** by IoT telematics. New records arrive every few seconds. |
| Transformations run as **monolithic SQL scripts**, 15–45 min each, with full-table rewrites. | The warehouse and marts must stay **fresh within minutes**, not 45-min batches. |
| The marts are **recomputed from scratch** on every run. | Recomputing 7.4M trips + 12M stops every run is untenable once data is live. Loads must be **incremental** and **idempotent**. |
| Python / pandas is only used at modelling time (Phase 4). | Python is used **from Phase 3 onward** — the pipeline itself is a Python application, with SQL as a runtime language but not the orchestration layer. |

The goal of this refactor is to keep every business rule and feature from v1, but express them as **incremental, watermark-driven, idempotent transformations** that the same code path can run in two modes:

- **Batch / back-fill** — process the full staging history once (already-present 54.7 M rows).
- **Micro-batch / streaming** — process only what has arrived since the last watermark, on a 5-minute schedule or on CDC trigger.

---

## 2. Architectural principles

The refactor follows seven principles. Every design decision below flows from them.

1. **Medallion layering.** `staging` = bronze (raw, immutable), `warehouse` = silver (cleaned, conformed, star schema), `marts` = gold (denormalized features for ML / BI). This maps cleanly onto the three schemas already in the database.
2. **Event-time semantics.** Every row has a canonical event-time column (`begin_path_time`, `stop_start`, `created_at`, `activity_start_time`). Watermarks and windowing use event-time, never wall-clock / processing-time.
3. **Watermark-driven incrementalism.** A single table `warehouse.etl_watermark` tracks `(layer, table_name, last_event_time, last_run_at)`. Every downstream transform reads only rows `WHERE event_time > watermark - overlap` to handle late-arriving data.
4. **Idempotency via MERGE / upsert.** Every fact and mart load uses `INSERT ... ON CONFLICT DO UPDATE`. Re-running the same window is a no-op on already-landed rows — essential for stream retries and back-fills.
5. **Schema contracts.** Every boundary (staging → warehouse, warehouse → marts, pipeline → external consumer) is guarded by a pydantic v2 model. A telemetry row that violates the contract is routed to a quarantine table, not silently dropped.
6. **Config-driven cleaning rules.** The seven C1-C7 rules from v1 live in `config/cleaning_rules.yaml`, not inside SQL. The rule engine reads YAML and applies rules uniformly in batch and streaming mode. New rules = config edit + unit test, no SQL change.
7. **Same code, two runtimes.** The ingestion layer exposes an abstract `Source` protocol. Two concrete implementations exist today (`BatchStagingSource`, `StreamKafkaSource`); the transform and load code is unchanged between them. When Accent stands up Kafka/Redpanda, only the source class swaps.

---

## 3. What changes concretely vs. v1

### 3.1 Schemas

| v1 | Refactored |
|---|---|
| 6 dimensions, 5 facts, 1 bridge (same) | 6 dimensions, 5 facts, 1 bridge **+** `warehouse.etl_watermark`, `warehouse.etl_run_log`, `warehouse.quarantine_rejected` |
| Fact tables have only business columns | Fact tables additionally carry `_loaded_at TIMESTAMPTZ`, `_source_checksum CHAR(32)`, `_etl_run_id BIGINT` for lineage & idempotency |
| No surrogate keys on facts | Composite natural key `(tenant_id, device_id, begin_path_time)` enforced `UNIQUE` so `ON CONFLICT` works |
| `mart_device_monthly_behavior` fully rebuilt every run | Same table, but rows are **upserted month-by-month**; only months touched by newly-landed facts are recomputed |

### 3.2 Execution model

| v1 | Refactored |
|---|---|
| Four SQL scripts, run in sequence, 15–45 min | Prefect flow composed of small tasks; runs in seconds on a 5-min incremental window |
| No re-run safety — duplicate rows if script rerun | Safe to re-run any window; deterministic output |
| No lineage except comments in SQL | Every row traces to `_etl_run_id` → `etl_run_log` entry → source query + parameters |

### 3.3 Cleaning rules

The seven rules from v1 are preserved verbatim in **business logic** but migrated from inline SQL to a declarative YAML catalog (`config/cleaning_rules.yaml`). This gives us:

- Unit tests per rule (`tests/test_cleaning_rules.py`).
- Rule-by-rule metrics logged to `warehouse.etl_run_log.rejected_by_rule` (a JSONB column).
- The ability to toggle rules on/off via config for A/B experiments during Phase 4 modelling.

### 3.4 Features

All 35 features from v1 Feature Groups 1–6 are retained, computed in `src/accent_fleet/transforms/features.py` using **Polars** instead of pure SQL. Why Polars?

- Lazy evaluation — the planner pushes predicates down to the scan, which matters when a 5-min window still touches a few thousand rows.
- The same expressions can later be lifted into Arrow-native streaming frameworks (DataFusion, Polars Streaming) with zero rewrite.
- Python-first, which means Phase 4 modelling code can share feature computation with the pipeline — no drift between "feature table" and "what the model actually sees".

### 3.5 Risk scoring

`v_device_risk_profile` is retained **as a view** over the feature mart so the weighted formula stays transparent and auditable. But the Python module `src/accent_fleet/features/risk_score.py` mirrors it exactly, so online scoring (Phase 6 deployment) can reuse the same coefficients without re-querying Postgres.

---

## 4. Streaming runway — what this unlocks for Phase 6

When Accent is ready to ingest IoT events in real time (i.e., before the device writes to staging, or by streaming staging changes via Debezium CDC), the refactored pipeline accommodates it with three changes:

1. Replace `BatchStagingSource` with `StreamKafkaSource` (stub already provided in `src/accent_fleet/ingestion/stream_source.py`).
2. Change the Prefect trigger from `schedule=every_5_min` to `trigger=on_kafka_message`.
3. Install `timescaledb` and convert `warehouse.fact_trip`, `warehouse.fact_stop`, and `warehouse.fact_overspeed` to **hypertables** (scripts already prepared under `sql/upgrade_timescaledb/` — execution left to infra team).

The transformation code, cleaning rules, feature definitions, and risk-score formula do not change. This is the core value of the refactor.

---

## 5. Traceability to v1 deliverables

Every v1 deliverable is present in the refactored repo:

| v1 artifact | Refactored location |
|---|---|
| `01_dw_dimensions.sql` | `sql/01_dim_tenant.sql` … `sql/05_dim_date_hour.sql` (split by dimension) |
| `02_dw_facts.sql` | `sql/10_fact_trip_incremental.sql` … `sql/14_fact_daily_activity_incr.sql` |
| `03_data_marts.sql` | `sql/20_mart_device_monthly_behavior.sql` + views `sql/21_*.sql` through `sql/23_*.sql` |
| `04_validation.sql` | `sql/99_validation_suite.sql` + `src/accent_fleet/monitoring/quality.py` |
| Cleaning rules C1–C7 | `config/cleaning_rules.yaml` + `src/accent_fleet/cleaning/rules.py` |
| Feature Groups 1–6 | `config/feature_definitions.yaml` + `src/accent_fleet/transforms/features.py` |
| Risk scoring formula | `src/accent_fleet/features/risk_score.py` + `sql/21_v_device_risk_profile.sql` |
| Data lineage table | `warehouse.etl_run_log` + `warehouse.etl_watermark` |

Nothing is lost. Everything is made incremental, testable, and stream-ready.

---

## 5b. April 2026 — BI dashboard expansion (Project 2)

**Trigger:** the warehouse will feed not only the ML pipeline (Project 1: Driver Behavior Scoring) but also a future BI dashboard. The BI layer needs every operationally meaningful staging table cleaned and persisted in the warehouse, not just the trip/overspeed/stop subset that the ML mart needs.

### Scope (curated BI, not raw mirror)

We deliberately did **not** mirror all ~110 staging tables. Most are configuration, junction, lookup, or admin noise (`alert_config*`, `cal_*`, `user*`, `authority`, `point_client_*`, etc.) and add zero BI value. We brought across only true business facts and dimension extensions:

| Source | Target | SQL | Watermark | Strategy |
|---|---|---|---|---|
| `staging.notification` (all rows) | `warehouse.fact_notification` | sql/17 | `created_at` | UPSERT on (tenant, notif_id) |
| `staging.maintenance` | `warehouse.fact_maintenance` | sql/18 | `date_operation` | UPSERT on (tenant, id_maintenance) |
| `staging.offense` ∪ `sinistre` ∪ `reparation` | `warehouse.fact_maintenance_line` | sql/19 | parent's `date_operation` | DELETE-INSERT-on-window |
| `staging.document` ⨝ `staging.fueling` (`doc_type='Fueling'`) | `warehouse.fact_fueling` | sql/24 | `document.date_operation` | DELETE-INSERT-on-window |
| `staging.assignment` → bridge | `warehouse.bridge_device_driver` | sql/07 | n/a (full reload, ~12 rows) | TRUNCATE+INSERT |

Design decisions:

- **`fact_notification` is a SUPERSET, not a replacement, of `fact_speed_notification`.** The ML feature contract depends on the speed-only table, so we keep it untouched. The new fact buckets `description` into nine `alert_category` values for BI grouping.
- **Maintenance line items are a single union table, not three sub-facts.** `offense`, `sinistre`, `reparation` all FK on `id_maintenance` and have no own date or PK — modelling them as one `fact_maintenance_line` with a `line_type` discriminator is cleaner than three near-empty tables. DELETE-INSERT-on-window is used because `reference_unique` is sparse and non-unique.
- **`fact_fueling` joins document + fueling.** `staging.fueling.date` is fully NULL; the only reliable timestamp is `staging.document.date_operation`. We filter `doc_type='Fueling'` and exclude `'FuelingMonthly'` aggregates.
- **Skipped from this refactor:** `staging.alert` (superseded by `notification`), `staging.mileage` (overlaps `fact_trip`), `staging.events` (low BI value without lookup tables), `staging.fiche_vehicule` and `staging.leasing` (both empty as of profile — defer until populated), `staging.mission` (empty).

### BI marts (gold layer)

Three new marts power the dashboard, with strict dependency order:

| Mart | Grain | Driven by | SQL |
|---|---|---|---|
| `marts.mart_fleet_daily` | (tenant_id, fleet_date) | `:touched_dates[]` | sql/30 |
| `marts.mart_vehicle_monthly` | (tenant_id, vehicle_id, year_month) | `:touched_months[]` | sql/31 |
| `marts.mart_tenant_monthly_summary` | (tenant_id, year_month) | rolls up the above two | sql/32 |

`mart_fleet_daily` is the only **day-grain** mart. It needs a new `touched_dates_from_windows()` helper (added to `src/accent_fleet/transforms/facts.py`) — symmetrical to the existing `touched_months_from_windows()`.

### BI dashboard views

Three `CREATE OR REPLACE VIEW`s, idempotent on every bootstrap:

| View | Purpose | SQL |
|---|---|---|
| `marts.v_executive_dashboard` | Tenant-month KPIs + MoM deltas + 3-mo rolling | sql/33 |
| `marts.v_operational_dashboard` | Daily KPIs + per-100km ratios + 7d rolling | sql/34 |
| `marts.v_maintenance_dashboard` | Vehicle-month maintenance leaderboard with cost rank | sql/35 |

### Orchestration changes

- `FACT_SQL` registry (`src/accent_fleet/transforms/facts.py`) now lists all 11 facts (was 7).
- New tasks in `flow_batch.py`: `task_recompute_fleet_daily`, `task_recompute_vehicle_monthly`, `task_recompute_tenant_summary`.
- `bootstrap_flow` ensures DDL for the three new marts and the three new views.
- `incremental_flow` recomputes all five marts in one window: ML marts on touched-months, BI day-mart on touched-dates, then the tenant rollup last (depends on the others).
- `pipeline.yaml` now declares 11 sources (was 7) and the three new mart entries.
- `dim_driver` loader file (sql/04) defines the bridge schema; sql/07 populates it after dim_vehicle and dim_device are loaded.

### Notebooks added

- `02_data_preparation/facts/08_load_fact_notification.ipynb`
- `02_data_preparation/facts/09_load_fact_maintenance.ipynb` (loads header + lines in order)
- `02_data_preparation/facts/10_load_fact_fueling.ipynb`
- `02_data_preparation/marts/05_build_bi_marts.ipynb`
- `02_data_preparation/marts/06_build_bi_views.ipynb`

All follow the established 4-section template (Setup / Inputs / Execute / Inspect).

---

## 6. Open items for Phase 4 hand-off

- `marts.v_ml_features_driver_behavior` is the single contract Phase 4 (modelling) consumes. Its column list is **frozen** for the duration of modelling to avoid breaking feature drift.
- The `warehouse.quarantine_rejected` table should be reviewed weekly during Phase 4 — if rule C4 (fuel overflow) is rejecting >2 % of rows, the rule threshold needs tuning before it distorts model training data.
- TimescaleDB migration is **recommended but not required** for Phase 4 modelling. It becomes required once live streaming begins in Phase 6.
