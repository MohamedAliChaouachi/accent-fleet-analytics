-- =====================================================================
--  06_metadata_analysis.sql
--  Operational metadata: table sizes, last analyse, dead tuples, vacuum status
-- =====================================================================

-- Table size and row estimate
SELECT
    schemaname,
    relname        AS table_name,
    n_live_tup     AS live_rows,
    n_dead_tup     AS dead_rows,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) AS total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||relname))       AS table_size,
    last_analyze,
    last_autoanalyze,
    last_vacuum,
    last_autovacuum
FROM pg_stat_user_tables
WHERE schemaname IN ('staging', 'warehouse', 'marts')
ORDER BY pg_total_relation_size(schemaname||'.'||relname) DESC;

-- Index usage and bloat indicators
SELECT
    schemaname,
    relname  AS table_name,
    indexrelname AS index_name,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_all_indexes
WHERE schemaname IN ('staging', 'warehouse', 'marts')
ORDER BY pg_relation_size(indexrelid) DESC;

-- Pipeline run log statistics over the last 24 hours
SELECT
    mode,
    status,
    COUNT(*) AS runs,
    MIN(started_at) AS first_run,
    MAX(started_at) AS last_run,
    AVG(EXTRACT(EPOCH FROM (ended_at - started_at)))::numeric(10,2) AS avg_duration_seconds,
    SUM(rows_processed) AS total_rows_processed
FROM warehouse.etl_run_log
WHERE started_at > NOW() - INTERVAL '24 hours'
GROUP BY mode, status
ORDER BY mode, status;

-- Watermark age (how stale is the warehouse?)
SELECT
    source_table,
    target_table,
    last_event_ts,
    NOW() - last_event_ts AS lag
FROM warehouse.etl_watermark
ORDER BY lag DESC;

-- Quarantine summary: rejected rows per rule per day (last 7 days)
SELECT
    DATE(rejected_at) AS day,
    rule_id,
    COUNT(*) AS rejected_rows
FROM warehouse.quarantine_rejected
WHERE rejected_at > NOW() - INTERVAL '7 days'
GROUP BY day, rule_id
ORDER BY day DESC, rejected_rows DESC;
