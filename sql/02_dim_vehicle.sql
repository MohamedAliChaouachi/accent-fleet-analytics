-- =============================================================================
-- 02_dim_vehicle.sql
-- =============================================================================
-- Vehicle dimension. Normalises make (C7), derives vehicle class, handles
-- max_speed = 0 -> NULL. Idempotent via upsert on natural key.
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.dim_vehicle (
  vehicle_sk         BIGSERIAL PRIMARY KEY,
  tenant_id          INTEGER NOT NULL,
  vehicule_id        BIGINT NOT NULL,
  mark_raw           TEXT,
  mark_clean         TEXT,
  vehicle_class      TEXT,                         -- heavy | medium | light | unknown
  matricule          TEXT,
  max_speed_setting  INTEGER,                      -- NULL if raw was 0
  category           TEXT,
  model              TEXT,
  status             SMALLINT,
  _loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, vehicule_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_vehicle_tenant
  ON warehouse.dim_vehicle (tenant_id);

-- -----------------------------------------------------------------------------
-- Upsert from staging. Normalization dictionary is maintained as a CTE here
-- for SQL-only execution, AND duplicated in config/cleaning_rules.yaml for
-- Python-side use. The two MUST stay in sync — see tests/test_cleaning_rules.py
-- -----------------------------------------------------------------------------
WITH normalized AS (
  SELECT
    v.tenant_id,
    v.vehicule_id,
    v.mark AS mark_raw,
    CASE UPPER(TRIM(v.mark))
      WHEN 'IVECO'         THEN 'Iveco'
      WHEN 'ISUZU'         THEN 'Isuzu'
      WHEN 'SCANIA'        THEN 'Scania'
      WHEN 'RENAULT'       THEN 'Renault'
      WHEN 'MAN'           THEN 'MAN'
      WHEN 'VOLVO'         THEN 'Volvo'
      WHEN 'DACIA'         THEN 'Dacia'
      WHEN 'SUZUKI'        THEN 'Suzuki'
      WHEN 'SUZUKI SWIFT'  THEN 'Suzuki'
      WHEN 'FIAT'          THEN 'Fiat'
      WHEN 'FIAT FIORINO'  THEN 'Fiat'
      WHEN 'MITSUBISHI'    THEN 'Mitsubishi'
      ELSE INITCAP(LOWER(TRIM(v.mark)))
    END AS mark_clean,
    v.matricule,
    CASE WHEN v.max_speed <= 0 THEN NULL ELSE v.max_speed::INTEGER END AS max_speed_setting,
    v.category,
    v.model,
    v.status
  FROM staging.vehicule v
  WHERE v.vehicule_id IS NOT NULL
)
INSERT INTO warehouse.dim_vehicle
  (tenant_id, vehicule_id, mark_raw, mark_clean, vehicle_class,
   matricule, max_speed_setting, category, model, status)
SELECT
  tenant_id,
  vehicule_id,
  mark_raw,
  mark_clean,
  CASE
    WHEN mark_clean IN ('Scania','Iveco','MAN','Volvo')      THEN 'heavy'
    WHEN mark_clean IN ('Isuzu','Renault')                    THEN 'medium'
    WHEN mark_clean IN ('Dacia','Suzuki','Fiat','Mitsubishi') THEN 'light'
    ELSE 'unknown'
  END AS vehicle_class,
  matricule,
  max_speed_setting,
  category,
  model,
  status
FROM normalized
ON CONFLICT (tenant_id, vehicule_id) DO UPDATE
  SET mark_raw          = EXCLUDED.mark_raw,
      mark_clean        = EXCLUDED.mark_clean,
      vehicle_class     = EXCLUDED.vehicle_class,
      matricule         = EXCLUDED.matricule,
      max_speed_setting = EXCLUDED.max_speed_setting,
      category          = EXCLUDED.category,
      model             = EXCLUDED.model,
      status            = EXCLUDED.status,
      _loaded_at        = NOW();
