-- =============================================================================
-- 37_v_safety_scorecard_dashboard.sql
-- =============================================================================
-- Safety-focused BI view (v2.0). One row per (tenant_id, year_month) with
-- normalised safety KPIs (per 1000 km), temporal patterns, and a fleet-wide
-- safety score the insurance / risk team uses to track MoM improvement.
--
-- Normalisation rationale:
--   Raw event counts scale with mileage, so a bigger tenant always "looks
--   worse". Per-1000-km rates put a 50-vehicle fleet and a 5-vehicle fleet
--   on the same axis. 1000 (not 100) so the numbers read like familiar
--   insurance metrics (e.g. "5 events per 1000 km").
--
-- Composite safety_score formula (0-100, higher = safer):
--   100 minus a weighted sum of overspeed/harsh rates capped at 100. The
--   weights mirror the underlying severity bands so a single extreme
--   overspeed has the same impact as five low ones.
-- =============================================================================

CREATE OR REPLACE VIEW marts.v_safety_scorecard_dashboard AS
WITH
device_month AS (
  -- Roll device × month behavior to tenant × month for severity counts and
  -- temporal ratios. Trip-count weighting on ratios so a quiet device with
  -- a 100% night-trip ratio doesn't dominate the fleet average.
  SELECT
    tenant_id,
    year_month,
    SUM(overspeed_count)                                       AS overspeed_count,
    SUM(overspeed_severity_low)                                AS overspeed_low,
    SUM(overspeed_severity_medium)                             AS overspeed_medium,
    SUM(overspeed_severity_high)                               AS overspeed_high,
    SUM(overspeed_severity_extreme)                            AS overspeed_extreme,
    SUM(total_distance_km)                                     AS total_distance_km,
    SUM(total_trips)                                           AS total_trips,
    -- Trip-weighted temporal ratios (% of trips in each window)
    SUM(total_trips * rush_hour_trip_ratio)
      / NULLIF(SUM(total_trips), 0)                            AS rush_hour_trip_ratio,
    SUM(total_trips * night_trip_ratio)
      / NULLIF(SUM(total_trips), 0)                            AS night_trip_ratio,
    SUM(total_trips * weekend_trip_ratio)
      / NULLIF(SUM(total_trips), 0)                            AS weekend_trip_ratio
  FROM marts.mart_device_monthly_behavior
  GROUP BY tenant_id, year_month
),
harsh_month AS (
  -- Brake/accel/corner counts from the raw fact, split by type. We don't
  -- have a pre-aggregated harsh-by-type mart, so this is the cheapest source.
  SELECT
    tenant_id,
    TO_CHAR(event_date, 'YYYY-MM')                             AS year_month,
    COUNT(*)                                                   AS harsh_total,
    COUNT(*) FILTER (WHERE event_type = 'harsh_brake')          AS harsh_brake_count,
    COUNT(*) FILTER (WHERE event_type = 'harsh_accel')          AS harsh_accel_count,
    COUNT(*) FILTER (WHERE event_type = 'harsh_corner')         AS harsh_corner_count,
    -- Weekend share — EXTRACT(DOW) returns 0=Sunday..6=Saturday
    COUNT(*) FILTER (WHERE EXTRACT(DOW FROM event_date) IN (0, 6)) AS weekend_harsh_count
  FROM warehouse.fact_harsh_event
  GROUP BY tenant_id, TO_CHAR(event_date, 'YYYY-MM')
),
risk_month AS (
  -- Per-tenant risk-score aggregates from the IsolationForest output. We
  -- don't have a count of "scored devices" baked into the fact, so we
  -- count rows here and trust the activity-gate the scorer already applied.
  SELECT
    tenant_id,
    year_month,
    AVG(risk_score)                                            AS avg_risk_score,
    COUNT(*)                                                   AS scored_devices,
    COUNT(*) FILTER (WHERE risk_category = 'high')             AS high_risk_devices,
    COUNT(*) FILTER (WHERE risk_category = 'critical')         AS critical_risk_devices,
    COUNT(*) FILTER (WHERE risk_category IN ('high', 'critical')) AS high_or_critical_devices
  FROM marts.fact_device_risk_score
  GROUP BY tenant_id, year_month
),
combined AS (
  SELECT
    s.tenant_id,
    s.year_month,
    s.active_devices,
    s.total_distance_km,
    s.total_trips,
    s.total_overspeed,
    s.total_harsh_events,
    -- device-grain rolled up
    dm.overspeed_low, dm.overspeed_medium, dm.overspeed_high, dm.overspeed_extreme,
    dm.rush_hour_trip_ratio,
    dm.night_trip_ratio,
    dm.weekend_trip_ratio,
    -- harsh fact rolled up
    COALESCE(hm.harsh_brake_count, 0)        AS harsh_brake_count,
    COALESCE(hm.harsh_accel_count, 0)        AS harsh_accel_count,
    COALESCE(hm.harsh_corner_count, 0)       AS harsh_corner_count,
    COALESCE(hm.weekend_harsh_count, 0)      AS weekend_harsh_count,
    -- risk
    COALESCE(rm.avg_risk_score, 0)           AS avg_risk_score,
    COALESCE(rm.scored_devices, 0)           AS scored_devices,
    COALESCE(rm.high_risk_devices, 0)        AS high_risk_devices,
    COALESCE(rm.critical_risk_devices, 0)    AS critical_risk_devices,
    COALESCE(rm.high_or_critical_devices, 0) AS high_or_critical_devices
  FROM marts.mart_tenant_monthly_summary s
  LEFT JOIN device_month dm USING (tenant_id, year_month)
  LEFT JOIN harsh_month  hm USING (tenant_id, year_month)
  LEFT JOIN risk_month   rm USING (tenant_id, year_month)
)
SELECT
  c.tenant_id,
  c.year_month,
  -- ---- Context ----
  c.active_devices,
  c.total_distance_km,
  c.total_trips,
  c.total_overspeed,
  c.total_harsh_events,
  -- ---- Overspeed Metrics (normalised) ----
  CASE WHEN c.total_distance_km > 0
       THEN c.total_overspeed::FLOAT / c.total_distance_km * 1000
       ELSE 0 END                                              AS overspeed_rate_per_1000km,
  -- Severity score: weighted avg per overspeed event (1×low, 2×med, 3×hi, 5×ext)
  CASE WHEN c.total_overspeed > 0
       THEN (COALESCE(c.overspeed_low, 0) * 1
           + COALESCE(c.overspeed_medium, 0) * 2
           + COALESCE(c.overspeed_high, 0) * 3
           + COALESCE(c.overspeed_extreme, 0) * 5)::FLOAT
           / c.total_overspeed
       ELSE 0 END                                              AS overspeed_severity_score,
  COALESCE(
    (c.total_overspeed - LAG(c.total_overspeed) OVER w)
      / NULLIF(LAG(c.total_overspeed) OVER w, 0) * 100,
    0
  )                                                            AS overspeed_trend_mom_pct,
  -- ---- Harsh Events (normalised) ----
  CASE WHEN c.total_distance_km > 0
       THEN c.total_harsh_events::FLOAT / c.total_distance_km * 1000
       ELSE 0 END                                              AS harsh_events_per_1000km,
  c.harsh_brake_count,
  c.harsh_accel_count,
  c.harsh_corner_count,
  CASE WHEN c.total_harsh_events > 0
       THEN c.harsh_brake_count::FLOAT  / c.total_harsh_events
       ELSE 0 END                                              AS harsh_brake_rate,
  CASE WHEN c.total_harsh_events > 0
       THEN c.harsh_accel_count::FLOAT  / c.total_harsh_events
       ELSE 0 END                                              AS harsh_accel_rate,
  CASE WHEN c.total_harsh_events > 0
       THEN c.harsh_corner_count::FLOAT / c.total_harsh_events
       ELSE 0 END                                              AS harsh_corner_rate,
  -- ---- Temporal Risk Patterns ----
  -- We don't tag overspeed events with rush/night flags in the source fact,
  -- so we approximate fleet exposure to rush/night using the trip-ratio
  -- mart. A high ratio + high overspeed rate implies risk concentration in
  -- those windows. (Direct overspeed_rush_hour_pct would need a redesign
  -- of fact_overspeed; out of scope for v2.0.)
  COALESCE(c.rush_hour_trip_ratio, 0) * 100                    AS rush_hour_trip_pct,
  COALESCE(c.night_trip_ratio, 0)     * 100                    AS night_trip_pct,
  CASE WHEN c.total_harsh_events > 0
       THEN c.weekend_harsh_count::FLOAT / c.total_harsh_events * 100
       ELSE 0 END                                              AS weekend_harsh_pct,
  -- ---- Device Risk ----
  c.avg_risk_score,
  c.scored_devices,
  c.high_risk_devices,
  c.critical_risk_devices,
  c.high_or_critical_devices,
  CASE WHEN c.scored_devices > 0
       THEN c.high_or_critical_devices::FLOAT / c.scored_devices * 100
       ELSE 0 END                                              AS high_risk_device_pct,
  -- ---- Composite Safety Score (0-100; higher = safer) ----
  -- Penalise overspeed and harsh rates, with weights calibrated against
  -- the observed data distribution in this fleet:
  --   * overspeed_per_1000km typically ranges 0–60   → weight 0.4
  --   * harsh_per_1000km typically ranges 40–500     → weight 0.1
  -- An earlier draft used 2× / 4× — calibrated for an assumed real-world
  -- ~5 harsh/1000km, which is 50× smaller than the actual rate produced
  -- by the synthetic event generator. Every tenant landed at 0 because
  -- the harsh penalty alone (4× ~150 = ~600) blew past the 100-point
  -- budget. The recalibrated weights produce scores in the 40–90 band
  -- and preserve tenant ranking. Re-tune if the underlying event
  -- generator is later replaced with a real-world feed.
  GREATEST(
    0,
    LEAST(
      100,
      100
        - COALESCE(
            CASE WHEN c.total_distance_km > 0
                 THEN c.total_overspeed::FLOAT / c.total_distance_km * 1000 * 0.4
                 ELSE 0 END, 0)
        - COALESCE(
            CASE WHEN c.total_distance_km > 0
                 THEN c.total_harsh_events::FLOAT / c.total_distance_km * 1000 * 0.1
                 ELSE 0 END, 0)
    )
  )                                                            AS safety_score,
  -- ---- Safety Score MoM Delta (positive = improving) ----
  -- Same formula as safety_score above, minus the previous-month value.
  -- Weights must match the safety_score formula or the trend lies.
  COALESCE(
    GREATEST(0, LEAST(100,
      100
      - CASE WHEN c.total_distance_km > 0
             THEN c.total_overspeed::FLOAT / c.total_distance_km * 1000 * 0.4
             ELSE 0 END
      - CASE WHEN c.total_distance_km > 0
             THEN c.total_harsh_events::FLOAT / c.total_distance_km * 1000 * 0.1
             ELSE 0 END
    )) - LAG(
      GREATEST(0, LEAST(100,
        100
        - CASE WHEN c.total_distance_km > 0
               THEN c.total_overspeed::FLOAT / c.total_distance_km * 1000 * 0.4
               ELSE 0 END
        - CASE WHEN c.total_distance_km > 0
               THEN c.total_harsh_events::FLOAT / c.total_distance_km * 1000 * 0.1
               ELSE 0 END
      ))
    ) OVER w,
    0
  )                                                            AS safety_score_trend
FROM combined c
WINDOW w AS (PARTITION BY c.tenant_id ORDER BY c.year_month);
