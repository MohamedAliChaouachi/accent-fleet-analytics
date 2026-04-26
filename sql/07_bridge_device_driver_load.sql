-- =============================================================================
-- 07_bridge_device_driver_load.sql
-- =============================================================================
-- Populate warehouse.bridge_device_driver from staging.assignment.
--
-- staging.assignment grain: (tenant_id, date, driver_driver_id,
-- vehicule_vehicule_id, active). Driver↔vehicle, not driver↔device. We
-- resolve vehicle to device via dim_device.vehicle_sk → dim_vehicle.
--
-- SCD-style validity: per (tenant_id, device_id), each row's valid_from is
-- assignment.date; valid_to is the next assignment.date for that device (or
-- NULL if it is the latest); is_current=TRUE only for that latest row when
-- active=TRUE.
--
-- Volume: ~12 rows total in staging.assignment as of last profile, so this
-- is a full-reload script (TRUNCATE + INSERT). Re-runnable any time.
-- =============================================================================

TRUNCATE TABLE warehouse.bridge_device_driver;

WITH resolved AS (
  -- Map each assignment to a device_id (vehicle → vehicle_sk → device)
  SELECT
    a.tenant_id,
    dd.device_id,
    a.driver_driver_id::BIGINT  AS driver_id,
    a.date                       AS valid_from,
    a.active                     AS active
  FROM staging.assignment a
  JOIN warehouse.dim_vehicle dv
    ON dv.tenant_id  = a.tenant_id
   AND dv.vehicule_id = a.vehicule_vehicule_id::BIGINT
  JOIN warehouse.dim_device dd
    ON dd.vehicle_sk = dv.vehicle_sk
   AND dd.tenant_id  = a.tenant_id
),
ranged AS (
  SELECT
    tenant_id,
    device_id,
    driver_id,
    valid_from,
    LEAD(valid_from) OVER (
      PARTITION BY tenant_id, device_id
      ORDER BY valid_from
    )                                        AS valid_to,
    -- is_current: latest assignment for the device AND active=TRUE
    (ROW_NUMBER() OVER (
       PARTITION BY tenant_id, device_id
       ORDER BY valid_from DESC
     ) = 1)
    AND active                                AS is_current
  FROM resolved
)
INSERT INTO warehouse.bridge_device_driver
  (tenant_id, device_id, driver_id, valid_from, valid_to, is_current)
SELECT tenant_id, device_id, driver_id, valid_from, valid_to, is_current
FROM ranged;

CREATE INDEX IF NOT EXISTS idx_bridge_device_driver_current
  ON warehouse.bridge_device_driver (tenant_id, device_id)
  WHERE is_current;
