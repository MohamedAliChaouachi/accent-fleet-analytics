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
-- Categorization is derived from `name` (the alert kind, e.g. 'SPEED',
-- 'SPEED_NOT_HIGHWAY', 'VIDANGE'). `description` is 100% NULL in the source
-- so the prior derivation produced 'other' for every row. `name` is always
-- populated and uses a small fixed vocabulary:
--   speed_alert        : name LIKE 'SPEED%'   (SPEED, SPEED_HIGHWAY, SPEED_NOT_HIGHWAY)
--   geofence_alert     : name = 'POI' OR name = 'ZONE'
--   maintenance_alert  : name IN ('VIDANGE','DOC')   (oil change, doc reminder)
--   ignition_alert     : name = 'INGNITION_V0'        (sic — source typo)
--   fuel_theft_alert   : name = 'SIPHONAGE'
--   other              : anything else
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
  -- Keep `name` in the description column for downstream readability
  -- (raw `description` is 100% NULL at source).
  n.name                    AS description,
  CASE
    WHEN n.name LIKE 'SPEED%'                THEN 'speed_alert'
    WHEN n.name IN ('POI','ZONE')            THEN 'geofence_alert'
    WHEN n.name IN ('VIDANGE','DOC')         THEN 'maintenance_alert'
    WHEN n.name = 'INGNITION_V0'             THEN 'ignition_alert'
    WHEN n.name = 'SIPHONAGE'                THEN 'fuel_theft_alert'
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
