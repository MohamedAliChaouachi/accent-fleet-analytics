-- =============================================================================
-- 29_clean_mart_nulls.sql
-- =============================================================================
-- One-shot cleanup. Converts NULL → 0 on already-loaded rows in every mart so
-- the BI dashboard never has to render NULL/empty cells. Future rows are
-- already hardened in sql/20, sql/30, sql/31, sql/32 (COALESCE / ELSE 0).
--
-- Idempotent. Safe to re-run. Filters on `IS NULL` so re-runs are no-ops.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- mart_device_monthly_behavior  (ML mart)
-- ---------------------------------------------------------------------------
UPDATE marts.mart_device_monthly_behavior SET
  stddev_trip_distance     = COALESCE(stddev_trip_distance,     0),
  overspeed_per_100km      = COALESCE(overspeed_per_100km,      0),
  overspeed_per_trip       = COALESCE(overspeed_per_trip,       0),
  avg_speed_over_limit     = COALESCE(avg_speed_over_limit,     0),
  speed_alert_per_100km    = COALESCE(speed_alert_per_100km,    0),
  in_path_stop_ratio       = COALESCE(in_path_stop_ratio,       0),
  stops_per_trip           = COALESCE(stops_per_trip,           0),
  avg_working_hours        = COALESCE(avg_working_hours,        0)
WHERE stddev_trip_distance     IS NULL
   OR overspeed_per_100km      IS NULL
   OR overspeed_per_trip       IS NULL
   OR avg_speed_over_limit     IS NULL
   OR speed_alert_per_100km    IS NULL
   OR in_path_stop_ratio       IS NULL
   OR stops_per_trip           IS NULL
   OR avg_working_hours        IS NULL;

-- ---------------------------------------------------------------------------
-- mart_fleet_daily  (BI day-grain)
-- ---------------------------------------------------------------------------
UPDATE marts.mart_fleet_daily SET
  total_driving_hours = COALESCE(total_driving_hours, 0),
  avg_max_speed_kmh   = COALESCE(avg_max_speed_kmh,   0)
WHERE total_driving_hours IS NULL
   OR avg_max_speed_kmh   IS NULL;

-- ---------------------------------------------------------------------------
-- mart_vehicle_monthly  (BI vehicle-grain)
-- ---------------------------------------------------------------------------
UPDATE marts.mart_vehicle_monthly SET
  total_driving_hours = COALESCE(total_driving_hours, 0),
  trip_fuel_used_l    = COALESCE(trip_fuel_used_l,    0),
  avg_cost_per_litre  = COALESCE(avg_cost_per_litre,  0),
  cost_per_km         = COALESCE(cost_per_km,         0),
  fuel_l_per_100km    = COALESCE(fuel_l_per_100km,    0)
WHERE total_driving_hours IS NULL
   OR trip_fuel_used_l    IS NULL
   OR avg_cost_per_litre  IS NULL
   OR cost_per_km         IS NULL
   OR fuel_l_per_100km    IS NULL;

-- ---------------------------------------------------------------------------
-- mart_tenant_monthly_summary  (BI tenant-grain)
-- ---------------------------------------------------------------------------
UPDATE marts.mart_tenant_monthly_summary SET
  total_driving_hours      = COALESCE(total_driving_hours,      0),
  avg_distance_per_vehicle = COALESCE(avg_distance_per_vehicle, 0),
  cost_per_km              = COALESCE(cost_per_km,              0)
WHERE total_driving_hours      IS NULL
   OR avg_distance_per_vehicle IS NULL
   OR cost_per_km              IS NULL;
