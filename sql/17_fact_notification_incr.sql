-- =============================================================================
-- 17_fact_notification_incr.sql
-- =============================================================================
-- fact_notification: ALL alerts/notifications from staging.notification — not
-- just SPEED%. Built for BI dashboards that need a complete picture of every
-- alert type (geofence, idle, panic, maintenance, route deviation, etc.).
--
-- The existing `fact_speed_notification` (sql/13) is preserved for the
-- baseline ML mart's stable feature contract. This file is a SUPERSET.
--
-- Categorization is derived from `description` so the BI layer can group
-- alerts without parsing strings:
--   speed_alert     : description LIKE 'SPEED%'
--   geofence_alert  : description LIKE 'GEOFENCE%' OR LIKE '%FENCE%'
--   idle_alert      : description LIKE '%IDLE%'
--   panic_alert     : description LIKE '%PANIC%' OR LIKE '%SOS%'
--   maintenance     : description LIKE '%MAINTENANCE%'
--   route_alert     : description LIKE 'ROUTE%' OR LIKE '%DEVIATION%'
--   other           : everything else (still kept for completeness)
--
-- Contract:
--   :window_start, :window_end, :etl_run_id (standard incremental)
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_notification (
  notification_sk    BIGSERIAL PRIMARY KEY,
  notif_id           INTEGER NOT NULL,
  tenant_id          INTEGER NOT NULL,
  device_id          BIGINT NOT NULL,
  created_at         TIMESTAMP NOT NULL,
  notification_date  DATE NOT NULL,
  description        TEXT,                              -- raw alert string
  alert_category     TEXT NOT NULL,                     -- bucketed (see header)
  alert_value        TEXT,                              -- raw payload
  lat                DOUBLE PRECISION,
  lng                DOUBLE PRECISION,
  -- Lineage
  _etl_run_id        BIGINT,
  _loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, notif_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_notification_event_time
  ON warehouse.fact_notification (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_fact_notification_device_month
  ON warehouse.fact_notification (tenant_id, device_id, notification_date);
CREATE INDEX IF NOT EXISTS idx_fact_notification_category
  ON warehouse.fact_notification (alert_category, created_at DESC);

INSERT INTO warehouse.fact_notification (
  notif_id, tenant_id, device_id, created_at, notification_date,
  description, alert_category, alert_value, lat, lng, _etl_run_id
)
SELECT
  n.id,
  n.tenant_id,
  n.device_id,
  n.created_at,
  n.created_at::DATE,
  n.description,
  CASE
    WHEN n.description LIKE 'SPEED%'                                 THEN 'speed_alert'
    WHEN n.description LIKE 'GEOFENCE%' OR n.description LIKE '%FENCE%' THEN 'geofence_alert'
    WHEN n.description LIKE '%IDLE%'                                  THEN 'idle_alert'
    WHEN n.description LIKE '%PANIC%' OR n.description LIKE '%SOS%'   THEN 'panic_alert'
    WHEN n.description LIKE '%MAINTENANCE%' OR n.description LIKE 'MAINT%' THEN 'maintenance_alert'
    WHEN n.description LIKE 'ROUTE%'  OR n.description LIKE '%DEVIATION%' THEN 'route_alert'
    WHEN n.description LIKE '%TOW%'   OR n.description LIKE '%TOWING%' THEN 'tow_alert'
    WHEN n.description LIKE '%BATTERY%' OR n.description LIKE '%POWER%' THEN 'power_alert'
    ELSE 'other'
  END                       AS alert_category,
  n.alert_value,
  n.lat,
  n.lng,
  :etl_run_id
FROM staging.notification n
WHERE n.created_at >= :window_start
  AND n.created_at <  :window_end
  AND n.created_at >= '2019-10-01'::timestamp
  AND EXISTS (
    SELECT 1 FROM warehouse.dim_device dd
    WHERE dd.tenant_id = n.tenant_id AND dd.device_id = n.device_id
  )
ON CONFLICT (tenant_id, notif_id) DO UPDATE
  SET description       = EXCLUDED.description,
      alert_category    = EXCLUDED.alert_category,
      alert_value       = EXCLUDED.alert_value,
      lat               = EXCLUDED.lat,
      lng               = EXCLUDED.lng,
      _etl_run_id       = EXCLUDED._etl_run_id,
      _loaded_at        = NOW();
