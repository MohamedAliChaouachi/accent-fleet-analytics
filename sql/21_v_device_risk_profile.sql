-- =============================================================================
-- 21_v_device_risk_profile.sql
-- =============================================================================
-- Rolling 3-month risk profile per device.
--
-- BEFORE v0.6: the risk score was computed in-line from a deterministic
--              weighted formula that mirrored src/accent_fleet/features/
--              risk_score.py exactly.
--
-- AFTER v0.6:  the risk score is produced by the per-tenant Isolation Forest
--              and persisted in marts.fact_device_risk_score by the Python
--              batch scorer. This view is now a COMPAT layer: it joins that
--              fact onto the rolling-3-month aggregates from
--              mart_device_monthly_behavior so existing downstream consumers
--              (sql/23_v_fleet_risk_dashboard.sql, sql/41_fact_device_risk_
--              profile.sql, the BI dashboard) keep working with no changes.
--
-- "Latest month with a score" semantics: for each device we take its most
-- recent (tenant_id, device_id, year_month) row in fact_device_risk_score
-- and join that month's behavior aggregates plus the 3-month rolling sums.
-- Devices below the activity gate (trips_3m >= 10) are filtered out, same
-- as the legacy formula view.
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_device_risk_profile AS
WITH
latest_score AS (
  -- One row per device: the most recently scored (tenant, device, year_month).
  -- DISTINCT ON is the Postgres-idiomatic way to "argmax" a column per group.
  SELECT DISTINCT ON (tenant_id, device_id)
    tenant_id,
    device_id,
    year_month         AS latest_month,
    risk_score,
    risk_category,
    model_version,
    model_source,
    scored_at
  FROM marts.fact_device_risk_score
  ORDER BY tenant_id, device_id, year_month DESC
),
latest_3m AS (
  -- The 3 most recent year_months that have data for each device, used to
  -- compute the rolling 3m aggregates below. We anchor to the same set of
  -- months the model was scored against by joining on (tenant, device).
  SELECT
    tenant_id, device_id,
    year_month,
    ROW_NUMBER() OVER (
      PARTITION BY tenant_id, device_id
      ORDER BY year_month DESC
    ) AS rn
  FROM marts.mart_device_monthly_behavior
),
rolling AS (
  SELECT
    m.tenant_id, m.device_id,
    SUM(m.total_trips)                             AS trips_3m,
    SUM(m.total_distance_km)                       AS distance_3m,
    SUM(m.overspeed_count)                         AS overspeed_3m,
    SUM(m.overspeed_severity_high
        + m.overspeed_severity_extreme)            AS severe_overspeed_3m,
    SUM(m.speed_alert_count)                       AS alerts_3m
  FROM marts.mart_device_monthly_behavior m
  JOIN latest_3m l USING (tenant_id, device_id, year_month)
  WHERE l.rn <= 3
  GROUP BY m.tenant_id, m.device_id
)
SELECT
  ls.tenant_id,
  ls.device_id,
  ls.latest_month,
  r.trips_3m,
  r.distance_3m,
  r.overspeed_3m,
  r.severe_overspeed_3m,
  r.alerts_3m,
  -- risk_score is kept as INTEGER for backward compatibility with the legacy
  -- formula view's column type. The IF model emits NUMERIC(5,1); rounding
  -- to integer here is the same semantic as the pre-v0.6 view.
  ROUND(ls.risk_score)::INTEGER AS risk_score,
  ls.risk_category,
  ls.model_version,
  ls.model_source,
  ls.scored_at
FROM latest_score ls
JOIN rolling r USING (tenant_id, device_id)
-- Activity gate: same threshold as the legacy view. Devices below it never
-- got a score in the formula world either, so omitting them preserves the
-- consumer contract.
WHERE r.trips_3m >= 10;
