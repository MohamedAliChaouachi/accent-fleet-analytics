-- =============================================================================
-- 41_fact_device_risk_profile.sql
-- =============================================================================
-- Materialized snapshot of marts.v_device_risk_profile.
--
-- The underlying view does a window/aggregate over the whole
-- mart_device_monthly_behavior table, then joins to fact_device_risk_score
-- and filters. When the API queries `... WHERE device_id = ?`, the engine
-- can't push the WHERE below the window in general, so every request
-- rescans the source. Bench at 2026-05-12:
--   p95 = 500ms single-shot, 1693ms at concurrency 16  (target <= 300ms)
--
-- Snapshotting the view into a real table with PK + per-device index turns
-- the endpoint into an index lookup. The refresh is cheap because it only
-- materializes the gate-passing devices (trips_3m >= 10) and runs once per
-- incremental flow after marts are refreshed AND the IF batch scorer has
-- (re)populated marts.fact_device_risk_score.
--
-- v0.6 change: the underlying view now sources risk_score / risk_category
-- from fact_device_risk_score (Isolation Forest output) instead of the
-- old in-line weighted formula. The columns that downstream BI consumes
-- are unchanged so the dashboard keeps working without rewrites; the new
-- model_version / model_source / scored_at columns are added so the
-- /devices/{id}/profile endpoint can render a "scored by model vN on T"
-- badge.
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.fact_device_risk_profile (
    tenant_id              INT              NOT NULL,
    device_id              BIGINT           NOT NULL,
    latest_month           CHAR(7)          NOT NULL,
    trips_3m               BIGINT,
    distance_3m            DOUBLE PRECISION,
    overspeed_3m           BIGINT,
    severe_overspeed_3m    BIGINT,
    alerts_3m              BIGINT,
    risk_score             INTEGER,
    risk_category          TEXT,
    model_version          TEXT,
    model_source           TEXT,
    scored_at              TIMESTAMPTZ,
    refreshed_at           TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, device_id)
);

-- Idempotent column adds for stacks that pre-date v0.6 — keeps the
-- migration zero-downtime: redeploying these statements never errors
-- on a fresh DB nor on an upgraded one.
ALTER TABLE marts.fact_device_risk_profile
    ADD COLUMN IF NOT EXISTS model_version TEXT;
ALTER TABLE marts.fact_device_risk_profile
    ADD COLUMN IF NOT EXISTS model_source TEXT;
ALTER TABLE marts.fact_device_risk_profile
    ADD COLUMN IF NOT EXISTS scored_at TIMESTAMPTZ;

-- Single-column index for the /devices/{device_id}/profile lookup path,
-- which queries by device_id alone (no tenant filter on that endpoint).
CREATE INDEX IF NOT EXISTS ix_fact_device_risk_profile_device_id
    ON marts.fact_device_risk_profile (device_id);

-- Risk-score index for the /devices/top-risk endpoint (ORDER BY risk_score DESC).
CREATE INDEX IF NOT EXISTS ix_fact_device_risk_profile_risk_score
    ON marts.fact_device_risk_profile (risk_score DESC NULLS LAST);

-- ---------------------------------------------------------------------------
-- Refresh procedure.
--
-- TRUNCATE + INSERT in a single transaction so readers either see the
-- previous snapshot or the new one, never a half-empty table. Snapshot
-- size is small (one row per gate-passing device, currently <100k rows),
-- so the truncate-and-reload cost is dominated by the view computation
-- itself — which we'd have to pay either way.
--
-- v0.6 contract: this procedure can ONLY produce useful rows after the
-- batch scorer has populated marts.fact_device_risk_score for the touched
-- months. The Prefect flow orchestrates the order
--   refresh marts → score_risk_partitions → refresh_fact_device_risk_profile
-- so this invariant holds end-to-end.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE marts.refresh_fact_device_risk_profile()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE marts.fact_device_risk_profile;

    INSERT INTO marts.fact_device_risk_profile (
        tenant_id, device_id, latest_month,
        trips_3m, distance_3m, overspeed_3m, severe_overspeed_3m, alerts_3m,
        risk_score, risk_category,
        model_version, model_source, scored_at
    )
    SELECT
        tenant_id, device_id, latest_month,
        trips_3m, distance_3m, overspeed_3m, severe_overspeed_3m, alerts_3m,
        risk_score, risk_category,
        model_version, model_source, scored_at
    FROM marts.v_device_risk_profile;
END;
$$;
