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

## 6. Open items for Phase 4 hand-off

- `marts.v_ml_features_driver_behavior` is the single contract Phase 4 (modelling) consumes. Its column list is **frozen** for the duration of modelling to avoid breaking feature drift.
- The `warehouse.quarantine_rejected` table should be reviewed weekly during Phase 4 — if rule C4 (fuel overflow) is rejecting >2 % of rows, the rule threshold needs tuning before it distorts model training data.
- TimescaleDB migration is **recommended but not required** for Phase 4 modelling. It becomes required once live streaming begins in Phase 6.
