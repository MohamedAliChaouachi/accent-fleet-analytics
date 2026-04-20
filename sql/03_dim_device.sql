-- =============================================================================
-- 03_dim_device.sql
-- =============================================================================
-- Device dimension. Enforces C7 via INNER JOIN to dim_vehicle — any device
-- without a valid vehicle linkage is excluded (16 devices per Phase 2 report).
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.dim_device (
  device_sk     BIGSERIAL PRIMARY KEY,
  device_id     BIGINT NOT NULL,
  tenant_id     INTEGER NOT NULL,
  vehicle_sk    BIGINT REFERENCES warehouse.dim_vehicle(vehicle_sk),
  imei          TEXT,
  serial        TEXT,
  _loaded_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, device_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_device_tenant
  ON warehouse.dim_device (tenant_id);
CREATE INDEX IF NOT EXISTS idx_dim_device_vehicle
  ON warehouse.dim_device (vehicle_sk);

INSERT INTO warehouse.dim_device (device_id, tenant_id, vehicle_sk, imei, serial)
SELECT
  d.id_device   AS device_id,
  d.tenant_id,
  dv.vehicle_sk,
  d.imei,
  d.serial
FROM staging.device d
INNER JOIN warehouse.dim_vehicle dv
  ON dv.tenant_id = d.tenant_id
 AND dv.vehicule_id = d.vehicule_id
WHERE d.id_device IS NOT NULL
  AND d.tenant_id IS NOT NULL
ON CONFLICT (tenant_id, device_id) DO UPDATE
  SET vehicle_sk = EXCLUDED.vehicle_sk,
      imei       = EXCLUDED.imei,
      serial     = EXCLUDED.serial,
      _loaded_at = NOW();
