-- =============================================================================
-- 28_fact_device_risk_score.sql
-- =============================================================================
-- One row per (tenant, device, year_month) recording the per-tenant
-- Isolation Forest risk-score model's output for that partition. Populated by
-- the Python batch scorer (``score_risk_partitions`` in
-- src/accent_fleet/ml/batch_scoring.py) after marts are refreshed in the
-- incremental flow.
--
-- DDL-only on purpose: the upsert lives in Python because it must run the
-- fitted per-tenant IsolationForest + StandardScaler bundle from MLflow /
-- disk. Keeping the table SQL-managed (here) and the row population
-- Python-managed (in batch_scoring) keeps the contract explicit:
--
--   - Schema migrations         -> change this file.
--   - Scoring logic / model swap -> change batch_scoring.py.
--
-- This table is the source of truth for the risk score after v0.6:
--
--   - marts.v_device_risk_profile  is a compat VIEW that joins this fact
--                                  with mart_device_monthly_behavior so
--                                  downstream consumers keep working.
--   - marts.fact_device_risk_profile is the materialised snapshot read by
--                                  the /devices/* endpoints; its refresh
--                                  procedure also reads from this fact.
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.fact_device_risk_score (
  tenant_id              INTEGER          NOT NULL,
  device_id              BIGINT           NOT NULL,
  year_month             CHAR(7)          NOT NULL,

  -- Model output. risk_score is the rescaled anomaly score in [0, 100]
  -- with one decimal of precision; the per-tenant raw_min/raw_max are
  -- baked into the artifact so the scaling is reproducible across runs.
  risk_score             NUMERIC(5, 1)    NOT NULL,
  risk_category          TEXT             NOT NULL
    CHECK (risk_category IN ('low', 'moderate', 'high', 'critical')),

  -- Provenance: same shape as fact_device_cluster_assignment so the
  -- dashboard's "model version" badge can be sourced uniformly.
  model_version          TEXT             NOT NULL,
  model_source           TEXT             NOT NULL,
  scored_at              TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
  _etl_run_id            BIGINT,

  PRIMARY KEY (tenant_id, device_id, year_month)
);

-- Per-tenant-month sidebar filter on the dashboard. Same shape as the
-- equivalent index on fact_device_cluster_assignment.
CREATE INDEX IF NOT EXISTS idx_fact_device_risk_score_tenant_month
  ON marts.fact_device_risk_score (tenant_id, year_month);

-- Risk-category cohort: "show me every critical device this month".
CREATE INDEX IF NOT EXISTS idx_fact_device_risk_score_category_month
  ON marts.fact_device_risk_score (risk_category, year_month);

-- Per-device timeline (used by /v1/devices/{id}/risk-timeline if/when it
-- lands; today the materialised fact below is the primary read path).
CREATE INDEX IF NOT EXISTS idx_fact_device_risk_score_device_month
  ON marts.fact_device_risk_score (device_id, year_month DESC);

-- Lineage lookup: same convention as the cluster fact.
CREATE INDEX IF NOT EXISTS idx_fact_device_risk_score_run
  ON marts.fact_device_risk_score (_etl_run_id);
