-- =============================================================================
-- 00_schemas_and_state.sql
-- =============================================================================
-- Bootstrap script. Idempotent: safe to run multiple times.
-- Creates the three schemas (bronze/silver/gold), the watermark state table
-- that drives every incremental load, and the run-log + quarantine tables
-- that give every row full lineage.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS marts;

-- -----------------------------------------------------------------------------
-- etl_watermark: the single source of truth for "what have I processed?"
-- -----------------------------------------------------------------------------
-- One row per (layer, table_name). last_event_time is the max event-time
-- successfully loaded. Incremental loads read:
--     WHERE event_time > last_event_time - overlap
-- and after a successful load write back the new max.
--
-- This decouples watermark from wall-clock, so re-runs, back-fills, and
-- replays all behave consistently.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.etl_watermark (
  layer             TEXT NOT NULL,                 -- warehouse | marts
  table_name        TEXT NOT NULL,                 -- fact_trip, fact_stop, ...
  last_event_time   TIMESTAMP,                     -- event-time, NULL before first run
  last_run_at       TIMESTAMPTZ,                   -- processing-time
  last_etl_run_id   BIGINT,                        -- FK-like to etl_run_log
  rows_loaded_total BIGINT NOT NULL DEFAULT 0,
  notes             TEXT,
  PRIMARY KEY (layer, table_name)
);

-- -----------------------------------------------------------------------------
-- etl_run_log: one row per Prefect flow run. Full lineage.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.etl_run_log (
  run_id             BIGSERIAL PRIMARY KEY,
  mode               TEXT NOT NULL,                 -- bootstrap | backfill | incremental | stream
  started_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at        TIMESTAMPTZ,
  status             TEXT NOT NULL DEFAULT 'running',   -- running | success | failed
  window_start       TIMESTAMP,                     -- event-time window start
  window_end         TIMESTAMP,
  rows_read          BIGINT,
  rows_loaded        BIGINT,
  rows_rejected      BIGINT,
  rejected_by_rule   JSONB,                         -- { "C2": 12, "C3": 3, ... }
  error_message      TEXT,
  git_sha            TEXT,                          -- populated by CI
  config_hash        CHAR(32)                       -- MD5 of rules+features YAML
);

CREATE INDEX IF NOT EXISTS idx_etl_run_log_started
  ON warehouse.etl_run_log (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_etl_run_log_status
  ON warehouse.etl_run_log (status) WHERE status <> 'success';

-- -----------------------------------------------------------------------------
-- quarantine_rejected: rows that failed a "reject" cleaning rule.
-- -----------------------------------------------------------------------------
-- JSONB payload lets us keep a single table for all source tables. Readers
-- in Phase 4 modelling can use this to audit whether rule thresholds are too
-- aggressive (e.g., rejecting >5% of a tenants).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.quarantine_rejected (
  quarantine_id      BIGSERIAL PRIMARY KEY,
  etl_run_id         BIGINT NOT NULL REFERENCES warehouse.etl_run_log(run_id),
  source_table       TEXT NOT NULL,                 -- path, stop, ...
  rule_id            TEXT NOT NULL,                 -- C1, C2, ...
  tenant_id          INTEGER,
  device_id          BIGINT,
  event_time         TIMESTAMP,
  raw_row            JSONB NOT NULL,                -- the rejected source row
  rejected_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quarantine_rule
  ON warehouse.quarantine_rejected (rule_id, rejected_at DESC);
CREATE INDEX IF NOT EXISTS idx_quarantine_device
  ON warehouse.quarantine_rejected (tenant_id, device_id, event_time);

-- -----------------------------------------------------------------------------
-- Seed the watermark table with the tables the pipeline watches. Idempotent
-- via ON CONFLICT DO NOTHING — re-running bootstrap never resets progress.
-- -----------------------------------------------------------------------------
INSERT INTO warehouse.etl_watermark (layer, table_name, last_event_time, notes)
VALUES
  ('warehouse', 'dim_tenant',              NULL, 'Full-scan dim; low volume, no watermark needed'),
  ('warehouse', 'dim_vehicle',             NULL, 'Full-scan dim; refreshed on bootstrap'),
  ('warehouse', 'dim_device',              NULL, 'Full-scan dim; refreshed on bootstrap'),
  ('warehouse', 'dim_driver',              NULL, 'Full-scan dim; refreshed on bootstrap'),
  ('warehouse', 'dim_date',                NULL, 'Static generated calendar'),
  ('warehouse', 'dim_hour_band',           NULL, 'Static 24-row table'),
  ('warehouse', 'fact_trip',               NULL, 'Incremental on begin_path_time'),
  ('warehouse', 'fact_overspeed',          NULL, 'Incremental on begin_path_time'),
  ('warehouse', 'fact_stop',               NULL, 'Incremental on stop_start'),
  ('warehouse', 'fact_speed_notification', NULL, 'Incremental on created_at, SPEED% only'),
  ('warehouse', 'fact_daily_activity',     NULL, 'Incremental on activity_start_time'),
  ('warehouse', 'fact_harsh_event',        NULL, 'Incremental on date (staging.archive); accelerometer-derived events'),
  ('warehouse', 'fact_telemetry_daily',    NULL, 'Incremental on date::DATE (staging.archive); per-day aggregates'),
  ('marts',     'mart_device_monthly_behavior', NULL, 'Recomputed for touched months only'),
  ('marts',     'mart_device_monthly_telemetry', NULL, 'Recomputed for touched months only; archive-side companion mart')
ON CONFLICT (layer, table_name) DO NOTHING;
