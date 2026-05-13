-- =============================================================================
-- 40_retention.sql
-- =============================================================================
-- Bounded retention for high-cardinality operational tables.
--
-- Why this exists:
--   - warehouse.etl_run_log gets ~288 rows/day from the 5-min incremental flow.
--     Within a year that's >100 K rows; the dashboard's freshness query (and
--     anyone debugging a run) ends up scanning rows nobody will ever look at.
--   - warehouse.quarantine_rejected can balloon if a tenant ships dirty data;
--     dropping rejected rows older than the audit window is safe because the
--     run-log row that pointed at them is also gone.
--   - marts.fact_device_cluster_assignment retains one row per
--     (tenant, device, month). Without a cap, a multi-year fleet grows the
--     dashboard's cluster-cohort queries linearly. We keep N months of history,
--     which is what the dashboard's "What-if" comparison ever asks for.
--
-- This file is parameterised so the caller chooses the retention windows.
-- The Prefect task `task_apply_retention` in pipeline/flow_batch.py invokes
-- it from the incremental flow after marts are recomputed.
--
-- Run order matters: quarantine_rejected has an FK to etl_run_log(run_id),
-- so we delete the dependent rows first.
--
-- Parameters (bound via SQLAlchemy named binds):
--   :etl_run_log_retention_days     -- e.g. 90
--   :quarantine_retention_days      -- e.g. 30
--   :cluster_assignment_retention_months  -- e.g. 12
-- =============================================================================

-- ----- quarantine_rejected -------------------------------------------------
-- Dependent rows first. Successful runs older than the cutoff get dropped
-- alongside their etl_run_log row below. Failed runs are NOT pruned even if
-- old — they're the breadcrumb trail for retroactive incident analysis.
DELETE FROM warehouse.quarantine_rejected
 WHERE etl_run_id IN (
   SELECT run_id
     FROM warehouse.etl_run_log
    WHERE status = 'success'
      AND started_at < NOW() - (:quarantine_retention_days || ' days')::INTERVAL
 );

-- ----- etl_run_log ---------------------------------------------------------
-- Successful runs older than the cutoff are pruned. We keep failed runs
-- forever (small in absolute terms, valuable for post-mortems).
DELETE FROM warehouse.etl_run_log
 WHERE status = 'success'
   AND started_at < NOW() - (:etl_run_log_retention_days || ' days')::INTERVAL;

-- ----- fact_device_cluster_assignment --------------------------------------
-- year_month is CHAR(7) 'YYYY-MM'. We compute the cutoff month in Python and
-- pass it as text; the comparison is string-safe because the format is
-- monotonic.
DELETE FROM marts.fact_device_cluster_assignment
 WHERE year_month < :cluster_assignment_cutoff_month;
