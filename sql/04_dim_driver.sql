-- =============================================================================
-- 04_dim_driver.sql
-- =============================================================================
-- Driver dimension + device->driver bridge (via assignment).
-- Note: only 12 active assignments exist per Phase 2, so device_id remains
-- the primary analysis grain. Driver dim is kept for when data improves.
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.dim_driver (
  driver_sk        BIGSERIAL PRIMARY KEY,
  driver_id        BIGINT NOT NULL,
  tenant_id        INTEGER NOT NULL,
  first_name       TEXT,
  last_name        TEXT,
  is_medically_fit BOOLEAN,
  has_training     BOOLEAN,
  is_safe_driver   BOOLEAN,
  is_authorized    BOOLEAN,
  _loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, driver_id)
);

-- Helper to convert smallint 0/1 -> boolean safely.
CREATE OR REPLACE FUNCTION warehouse.smallint_to_bool(v SMALLINT)
RETURNS BOOLEAN AS $$
  SELECT CASE WHEN v IS NULL THEN NULL WHEN v = 0 THEN FALSE ELSE TRUE END;
$$ LANGUAGE SQL IMMUTABLE;

INSERT INTO warehouse.dim_driver
  (driver_id, tenant_id, first_name, last_name,
   is_medically_fit, has_training, is_safe_driver, is_authorized)
SELECT
  d.id_driver              AS driver_id,
  d.tenant_id,
  NULLIF(TRIM(d.first_name), ''),
  NULLIF(TRIM(d.last_name), ''),
  warehouse.smallint_to_bool(d.is_medically_fit),
  warehouse.smallint_to_bool(d.has_training),
  warehouse.smallint_to_bool(d.is_safe_driver),
  warehouse.smallint_to_bool(d.is_authorized)
FROM staging.driver d
WHERE d.id_driver IS NOT NULL
ON CONFLICT (tenant_id, driver_id) DO UPDATE
  SET first_name       = EXCLUDED.first_name,
      last_name        = EXCLUDED.last_name,
      is_medically_fit = EXCLUDED.is_medically_fit,
      has_training     = EXCLUDED.has_training,
      is_safe_driver   = EXCLUDED.is_safe_driver,
      is_authorized    = EXCLUDED.is_authorized,
      _loaded_at       = NOW();

-- -----------------------------------------------------------------------------
-- Bridge: device <-> driver via active assignment
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.bridge_device_driver (
  tenant_id       INTEGER NOT NULL,
  device_id       BIGINT NOT NULL,
  driver_id       BIGINT NOT NULL,
  valid_from      TIMESTAMP,
  valid_to        TIMESTAMP,
  is_current      BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (tenant_id, device_id, driver_id, valid_from)
);
