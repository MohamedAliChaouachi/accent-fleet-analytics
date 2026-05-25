-- =============================================================================
-- 38_v_predictive_alerts_dashboard.sql
-- =============================================================================
-- Predictive alerts BI view (v2.0). Synthesises a unified alert stream from
-- 6 distinct heuristics, each yielding rows in a shared schema:
--
--   alert_type            description
--   --------------------  -----------------------------------------------------
--   high_risk_prediction  device's most recent IsolationForest score is
--                         category 'high' or 'critical'
--   maintenance_due       days since last maintenance > 90
--   fuel_anomaly          last-month fuel litres > 1.5 × prior-month
--   inactive_device       no trip in last 7 days but device is in dim_device
--   cluster_migration     device's cluster_id changed between consecutive months
--   harsh_event_spike     last 7 days harsh count > 2 × prior 28-day average
--
-- Why a view (not a table):
--   Alerts are derivable. Persisting them means we then need an update
--   loop to flip status → 'acknowledged'/'resolved'. v2.0 ships read-only
--   alerts; an ack/resolve table can be added later as a join layer.
--
-- Dashboard usage:
--   SELECT * FROM marts.v_predictive_alerts_dashboard
--    WHERE tenant_id = :t AND severity IN ('high', 'critical')
--    ORDER BY detected_at DESC;
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_predictive_alerts_dashboard AS
WITH
-- ---- 1. High-Risk Prediction ----
-- Pulls the *latest* score per device. If that category is high/critical we
-- treat it as an active prediction the operations team should look at.
latest_risk AS (
  SELECT DISTINCT ON (tenant_id, device_id)
    tenant_id,
    device_id,
    year_month,
    risk_score,
    risk_category,
    scored_at
  FROM marts.fact_device_risk_score
  ORDER BY tenant_id, device_id, year_month DESC
),
risk_alerts AS (
  SELECT
    tenant_id,
    device_id,
    'high_risk_prediction'::TEXT                                AS alert_type,
    CASE WHEN risk_category = 'critical' THEN 'critical'
         ELSE 'high' END                                        AS severity,
    scored_at                                                   AS detected_at,
    'Device predicted '
      || risk_category
      || ' risk (score '
      || risk_score::TEXT
      || ')'                                                    AS alert_message,
    risk_score::TEXT                                            AS alert_value
  FROM latest_risk
  WHERE risk_category IN ('high', 'critical')
),
-- ---- 2. Maintenance Due ----
-- "Days since last maintenance" per vehicle, then propagated to all devices
-- on that vehicle via dim_device.vehicle_sk → dim_vehicle.
last_maintenance AS (
  SELECT
    fm.tenant_id,
    fm.vehicle_id,
    MAX(fm.maintenance_date)                                    AS last_maintenance_date
  FROM warehouse.fact_maintenance fm
  GROUP BY fm.tenant_id, fm.vehicle_id
),
maintenance_alerts AS (
  SELECT
    dd.tenant_id,
    dd.device_id,
    'maintenance_due'::TEXT                                     AS alert_type,
    CASE WHEN (CURRENT_DATE - lm.last_maintenance_date) > 180 THEN 'critical'
         WHEN (CURRENT_DATE - lm.last_maintenance_date) > 120 THEN 'high'
         ELSE 'medium' END                                      AS severity,
    (lm.last_maintenance_date + INTERVAL '90 days')::TIMESTAMPTZ AS detected_at,
    'Maintenance overdue by '
      || ((CURRENT_DATE - lm.last_maintenance_date) - 90)::TEXT
      || ' days'                                                AS alert_message,
    (CURRENT_DATE - lm.last_maintenance_date)::TEXT             AS alert_value
  FROM warehouse.dim_device dd
  JOIN warehouse.dim_vehicle dv USING (vehicle_sk)
  JOIN last_maintenance lm
    ON lm.tenant_id  = dd.tenant_id
   AND lm.vehicle_id = dv.vehicule_id
  WHERE (CURRENT_DATE - lm.last_maintenance_date) > 90
),
-- ---- 3. Fuel Anomaly ----
-- Compares this month's fuel litres to last month's for the same vehicle.
-- > 1.5× is the spec's threshold. Vehicles below 50 litres/month are excluded
-- so a small absolute spike on a parked vehicle doesn't fire.
fuel_lag AS (
  SELECT
    mv.tenant_id,
    mv.vehicle_id,
    mv.year_month,
    mv.fuel_litres                                             AS fuel_litres_this,
    LAG(mv.fuel_litres) OVER (
      PARTITION BY mv.tenant_id, mv.vehicle_id
      ORDER BY mv.year_month
    )                                                          AS fuel_litres_prior
  FROM marts.mart_vehicle_monthly mv
),
fuel_alerts AS (
  SELECT
    dd.tenant_id,
    dd.device_id,
    'fuel_anomaly'::TEXT                                       AS alert_type,
    'medium'::TEXT                                             AS severity,
    -- We don't have an exact event timestamp; anchor to last-of-month.
    (TO_DATE(fl.year_month || '-01', 'YYYY-MM-DD')
     + INTERVAL '1 month' - INTERVAL '1 day')::TIMESTAMPTZ      AS detected_at,
    'Fuel use '
      || ROUND(fl.fuel_litres_this / NULLIF(fl.fuel_litres_prior, 0) * 100)::TEXT
      || '% of prior month ('
      || ROUND(fl.fuel_litres_this)::TEXT
      || ' L vs '
      || ROUND(fl.fuel_litres_prior)::TEXT
      || ' L)'                                                  AS alert_message,
    ROUND(fl.fuel_litres_this / NULLIF(fl.fuel_litres_prior, 0) * 100)::TEXT
                                                                AS alert_value
  FROM fuel_lag fl
  JOIN warehouse.dim_vehicle dv
    ON dv.tenant_id = fl.tenant_id AND dv.vehicule_id = fl.vehicle_id
  JOIN warehouse.dim_device dd
    ON dd.vehicle_sk = dv.vehicle_sk
  WHERE fl.fuel_litres_prior >= 50
    AND fl.fuel_litres_this > 1.5 * fl.fuel_litres_prior
),
-- ---- 4. Inactive Device ----
-- No trip in the last 7 days. The dashboard team only cares about devices
-- registered as active (loaded_at within last 90 days as a proxy).
last_trip AS (
  SELECT
    tenant_id,
    device_id,
    MAX(begin_path_time)                                       AS last_trip_time
  FROM warehouse.fact_trip
  GROUP BY tenant_id, device_id
),
inactive_alerts AS (
  SELECT
    dd.tenant_id,
    dd.device_id,
    'inactive_device'::TEXT                                    AS alert_type,
    CASE WHEN COALESCE(lt.last_trip_time, dd._loaded_at)
                < NOW() - INTERVAL '30 days' THEN 'high'
         ELSE 'medium' END                                     AS severity,
    NOW()                                                      AS detected_at,
    'No trip in '
      || EXTRACT(DAY FROM NOW() -
           COALESCE(lt.last_trip_time, dd._loaded_at))::TEXT
      || ' days'                                                AS alert_message,
    EXTRACT(DAY FROM NOW() -
      COALESCE(lt.last_trip_time, dd._loaded_at))::TEXT         AS alert_value
  FROM warehouse.dim_device dd
  LEFT JOIN last_trip lt
    ON lt.tenant_id = dd.tenant_id AND lt.device_id = dd.device_id
  WHERE COALESCE(lt.last_trip_time, dd._loaded_at) < NOW() - INTERVAL '7 days'
    -- Don't fire on devices that were never active to begin with.
    AND dd._loaded_at > NOW() - INTERVAL '180 days'
),
-- ---- 5. Cluster Migration ----
-- Device moved to a different cluster between consecutive months — implies
-- a behavior change worth investigating.
cluster_change AS (
  SELECT
    tenant_id,
    device_id,
    year_month,
    cluster_id,
    LAG(cluster_id) OVER (
      PARTITION BY tenant_id, device_id ORDER BY year_month
    )                                                          AS prior_cluster_id,
    scored_at
  FROM marts.fact_device_cluster_assignment
),
cluster_alerts AS (
  SELECT
    tenant_id,
    device_id,
    'cluster_migration'::TEXT                                  AS alert_type,
    'low'::TEXT                                                AS severity,
    scored_at                                                  AS detected_at,
    'Behavior cluster shifted from '
      || prior_cluster_id::TEXT
      || ' to '
      || cluster_id::TEXT                                       AS alert_message,
    cluster_id::TEXT                                           AS alert_value
  FROM cluster_change
  WHERE prior_cluster_id IS NOT NULL
    AND cluster_id <> prior_cluster_id
),
-- ---- 6. Harsh Event Spike ----
-- This week's harsh count compared to the prior 4 weeks' weekly avg.
-- > 200% triggers; rolling windows are anchored to CURRENT_DATE.
harsh_recent AS (
  SELECT
    tenant_id,
    device_id,
    COUNT(*) FILTER (WHERE event_date >= CURRENT_DATE - 7) AS harsh_this_week,
    COUNT(*) FILTER (WHERE event_date >= CURRENT_DATE - 35
                       AND event_date  < CURRENT_DATE - 7)::FLOAT
      / 4                                                    AS harsh_prior_weekly_avg
  FROM warehouse.fact_harsh_event
  WHERE event_date >= CURRENT_DATE - 35
  GROUP BY tenant_id, device_id
),
harsh_alerts AS (
  SELECT
    tenant_id,
    device_id,
    'harsh_event_spike'::TEXT                                 AS alert_type,
    CASE WHEN harsh_this_week > 3 * harsh_prior_weekly_avg THEN 'high'
         ELSE 'medium' END                                    AS severity,
    NOW()                                                     AS detected_at,
    'Harsh events spiked to '
      || harsh_this_week::TEXT
      || ' this week (prior avg '
      || ROUND(harsh_prior_weekly_avg::NUMERIC, 1)::TEXT
      || ')'                                                  AS alert_message,
    harsh_this_week::TEXT                                     AS alert_value
  FROM harsh_recent
  WHERE harsh_prior_weekly_avg > 0
    AND harsh_this_week > 2 * harsh_prior_weekly_avg
    AND harsh_this_week >= 3
),
-- ---- Union all alert categories ----
unioned AS (
  SELECT * FROM risk_alerts
  UNION ALL SELECT * FROM maintenance_alerts
  UNION ALL SELECT * FROM fuel_alerts
  UNION ALL SELECT * FROM inactive_alerts
  UNION ALL SELECT * FROM cluster_alerts
  UNION ALL SELECT * FROM harsh_alerts
)
SELECT
  -- Stable, opaque ID per alert row (hash so two views of the same row
  -- agree and so a downstream ack table can FK to it). MD5 chosen over
  -- the SERIAL pattern because the view is recomputed, not persisted.
  MD5(u.tenant_id::TEXT
      || '|' || u.device_id::TEXT
      || '|' || u.alert_type
      || '|' || u.detected_at::TEXT)                          AS alert_id,
  u.tenant_id,
  u.device_id,
  u.alert_type,
  u.severity,
  u.detected_at,
  u.alert_message,
  u.alert_value,
  -- Default routing — overridden if/when an ack table is added.
  CASE
    WHEN u.alert_type = 'maintenance_due'      THEN 'fleet_manager'
    WHEN u.alert_type = 'high_risk_prediction' THEN 'operations_team'
    WHEN u.alert_type = 'fuel_anomaly'         THEN 'operations_team'
    WHEN u.alert_type = 'inactive_device'      THEN 'device_owner'
    ELSE 'operations_team'
  END                                                         AS assigned_to,
  -- Until an ack table exists, every alert is 'new'. Resolution time is
  -- left NULL so dashboards can SUM/AVG over it without bogus zeros.
  'new'::TEXT                                                 AS status,
  NULL::DOUBLE PRECISION                                      AS resolution_time_hours
FROM unioned u
-- Most-recent first — dashboards generally LIMIT 100 over this.
ORDER BY u.detected_at DESC;
