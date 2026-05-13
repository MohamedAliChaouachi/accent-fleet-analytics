-- =============================================================================
-- 41_fact_device_risk_profile.sql
-- =============================================================================
-- Materialized snapshot of marts.v_device_risk_profile.
--
-- The underlying view does a ROW_NUMBER() window over the whole
-- mart_device_monthly_behavior table, then filters by rn <= 3. When the API
-- queries `... WHERE device_id = ?`, the window can't be pushed below the
-- WHERE, so every request rescans the source. Bench at 2026-05-12:
--   p95 = 500ms single-shot, 1693ms at concurrency 16  (target <= 300ms)
--
-- Snapshotting the view into a real table with PK + per-device index turns
-- the endpoint into an index lookup. The refresh is cheap because it only
-- materializes the gate-passing devices (trips_3m >= 10), and runs once per
-- incremental flow after marts are refreshed.
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.fact_device_risk_profile (
    tenant_id              INT          NOT NULL,
    device_id              BIGINT       NOT NULL,
    latest_month           CHAR(7)      NOT NULL,
    trips_3m               BIGINT,
    distance_3m            DOUBLE PRECISION,
    overspeed_3m           BIGINT,
    severe_overspeed_3m    BIGINT,
    alerts_3m              BIGINT,
    risk_score             INTEGER,
    risk_category          TEXT,
    refreshed_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, device_id)
);

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
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE marts.refresh_fact_device_risk_profile()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE marts.fact_device_risk_profile;

    INSERT INTO marts.fact_device_risk_profile (
        tenant_id, device_id, latest_month,
        trips_3m, distance_3m, overspeed_3m, severe_overspeed_3m, alerts_3m,
        risk_score, risk_category
    )
    SELECT
        tenant_id, device_id, latest_month,
        trips_3m, distance_3m, overspeed_3m, severe_overspeed_3m, alerts_3m,
        risk_score, risk_category
    FROM marts.v_device_risk_profile;
END;
$$;
