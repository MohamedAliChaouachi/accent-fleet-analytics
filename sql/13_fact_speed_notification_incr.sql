-- =============================================================================
-- 13_fact_speed_notification_incr.sql
-- =============================================================================
-- Speed alerts from the notification system. Filters to SPEED% only.
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_speed_notification (
  notif_sk     BIGSERIAL PRIMARY KEY,
  notif_id     INTEGER NOT NULL,
  tenant_id    INTEGER NOT NULL,
  device_id    BIGINT NOT NULL,
  created_at   TIMESTAMP NOT NULL,
  alert_type   TEXT,                               -- SPEED | SPEED_HIGHWAY | SPEED_NOT_HIGHWAY
  alert_value  TEXT,
  lat          DOUBLE PRECISION,
  lng          DOUBLE PRECISION,
  _etl_run_id  BIGINT,
  _loaded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, notif_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_speed_notif_event_time
  ON warehouse.fact_speed_notification (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_fact_speed_notif_device
  ON warehouse.fact_speed_notification (tenant_id, device_id, created_at);

INSERT INTO warehouse.fact_speed_notification (
  notif_id, tenant_id, device_id, created_at, alert_type, alert_value, lat, lng, _etl_run_id
)
SELECT
  n.id                AS notif_id,
  n.tenant_id,
  n.device_id,
  n.created_at,
  n.description       AS alert_type,
  n.alert_value,
  n.lat,
  n.lng,
  :etl_run_id
FROM staging.notification n
WHERE n.created_at >= :window_start
  AND n.created_at <  :window_end
  AND n.created_at >= '2019-10-01'::timestamp
  AND n.description LIKE 'SPEED%'                   -- speed-related only
  AND EXISTS (
    SELECT 1 FROM warehouse.dim_device dd
    WHERE dd.tenant_id = n.tenant_id AND dd.device_id = n.device_id
  )
ON CONFLICT (tenant_id, notif_id) DO UPDATE
  SET alert_type  = EXCLUDED.alert_type,
      alert_value = EXCLUDED.alert_value,
      lat         = EXCLUDED.lat,
      lng         = EXCLUDED.lng,
      _etl_run_id = EXCLUDED._etl_run_id,
      _loaded_at  = NOW();
