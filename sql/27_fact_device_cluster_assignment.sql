-- =============================================================================
-- 27_fact_device_cluster_assignment.sql
-- =============================================================================
-- One row per (tenant, device, year_month) recording the clustering model's
-- output for that partition. Populated by the Python batch scorer
-- (src/accent_fleet/ml/batch_scoring.py) after marts are refreshed in the
-- incremental flow.
--
-- This is intentionally DDL-only: the upsert lives in Python because it must
-- run the fitted KMeans + StandardScaler from MLflow / disk. Keeping the
-- table SQL-managed (here) and the row population Python-managed (in
-- batch_scoring) keeps the contract explicit:
--
--   - Schema migrations -> change this file.
--   - Scoring logic / model swap -> change batch_scoring.py.
--
-- The dashboard reads from this table directly so it does not pay the cost
-- of calling the API per row. The API path (POST /score/cluster) is only
-- used for ad-hoc what-if scoring.
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.fact_device_cluster_assignment (
  tenant_id              INTEGER          NOT NULL,
  device_id              BIGINT           NOT NULL,
  year_month             CHAR(7)          NOT NULL,

  -- Model output
  cluster_id             INTEGER          NOT NULL,
  distance_to_centroid   DOUBLE PRECISION NOT NULL,

  -- Provenance: which model produced this row, when, under which ETL run.
  -- model_version is whatever the predictor reports (e.g. "3", "local:v1",
  -- or "unknown"); we don't try to parse it.
  model_version          TEXT             NOT NULL,
  model_source           TEXT             NOT NULL,
  scored_at              TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
  _etl_run_id            BIGINT,

  PRIMARY KEY (tenant_id, device_id, year_month)
);

-- Cluster-cohort queries on the dashboard: "show me every device in cluster 2
-- this month". Without this index they would scan the whole table.
CREATE INDEX IF NOT EXISTS idx_fact_cluster_assignment_cluster
  ON marts.fact_device_cluster_assignment (cluster_id, year_month);

-- Tenant-month filter is the default dashboard sidebar.
CREATE INDEX IF NOT EXISTS idx_fact_cluster_assignment_tenant_month
  ON marts.fact_device_cluster_assignment (tenant_id, year_month);

-- Lineage lookup: "which assignments came from a given ETL run?" — used by
-- the validation suite (V9) and for debugging stale rows after a model swap.
CREATE INDEX IF NOT EXISTS idx_fact_cluster_assignment_run
  ON marts.fact_device_cluster_assignment (_etl_run_id);
