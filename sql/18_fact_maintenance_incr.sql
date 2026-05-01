-- =============================================================================
-- 18_fact_maintenance_incr.sql
-- =============================================================================
-- fact_maintenance: header-level maintenance/work-order events from
-- staging.maintenance. One row per maintenance operation (id_maintenance).
--
-- Sub-facts (line items) — offense, sinistre, reparation — all FK to
-- id_maintenance and are loaded by sql/19_fact_maintenance_line_incr.sql.
--
-- Schema confirmed against staging.maintenance:
--   tenant_id, id_maintenance (PK), vehicule_id, date_operation,
--   maintenance_type, cost, labor_cost, provider_id, observation,
--   reference_da_dr, actual_repair_time
--
-- Watermark column: date_operation (the business event date).
-- Vehicle (not device) is the natural grain — vehicles are the maintenance
-- subject, and a vehicle may swap devices across its lifetime.
--
-- Cleaning rules applied inline:
--   - cost / labor_cost negative values clamped to NULL (data-quality guard)
--   - vehicule_id must exist in dim_vehicle (referential integrity)
--
-- Contract:
--   :window_start, :window_end, :etl_run_id   (standard incremental)
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_maintenance (
  maintenance_sk        BIGSERIAL PRIMARY KEY,
  maintenance_id        BIGINT NOT NULL,         -- staging.maintenance.id_maintenance
  tenant_id             INTEGER NOT NULL,
  vehicle_id            BIGINT NOT NULL,         -- staging.maintenance.vehicule_id
  provider_id           BIGINT,
  -- Event time
  date_operation        TIMESTAMP NOT NULL,
  maintenance_date      DATE NOT NULL,           -- date_operation::DATE for BI grouping
  actual_repair_time    TIMESTAMP,
  repair_duration_hours DOUBLE PRECISION,        -- actual_repair_time - date_operation, hours
  -- Categorical
  maintenance_type      VARCHAR,
  -- Financials
  cost                  DOUBLE PRECISION,        -- parts/material cost
  labor_cost            DOUBLE PRECISION,
  total_cost            DOUBLE PRECISION,        -- cost + labor_cost (computed at insert)
  -- Free-text
  observation           VARCHAR,
  reference_da_dr       VARCHAR,
  -- Lineage
  _etl_run_id           BIGINT,
  _loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, maintenance_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_maintenance_event_time
  ON warehouse.fact_maintenance (date_operation DESC);
CREATE INDEX IF NOT EXISTS idx_fact_maintenance_vehicle_month
  ON warehouse.fact_maintenance (tenant_id, vehicle_id, maintenance_date);
CREATE INDEX IF NOT EXISTS idx_fact_maintenance_type
  ON warehouse.fact_maintenance (maintenance_type, maintenance_date DESC);

INSERT INTO warehouse.fact_maintenance (
  maintenance_id, tenant_id, vehicle_id, provider_id,
  date_operation, maintenance_date, actual_repair_time, repair_duration_hours,
  maintenance_type, cost, labor_cost, total_cost,
  observation, reference_da_dr, _etl_run_id
)
SELECT
  m.id_maintenance,
  m.tenant_id,
  m.vehicule_id                                            AS vehicle_id,
  m.provider_id,
  m.date_operation,
  m.date_operation::DATE                                   AS maintenance_date,
  m.actual_repair_time,
  CASE
    WHEN m.actual_repair_time IS NOT NULL
     AND m.actual_repair_time >= m.date_operation
    THEN EXTRACT(EPOCH FROM (m.actual_repair_time - m.date_operation)) / 3600.0
    ELSE NULL
  END                                                      AS repair_duration_hours,
  m.maintenance_type,
  CASE WHEN m.cost       < 0 THEN NULL ELSE m.cost       END AS cost,
  CASE WHEN m.labor_cost < 0 THEN NULL ELSE m.labor_cost END AS labor_cost,
  COALESCE(CASE WHEN m.cost       < 0 THEN NULL ELSE m.cost       END, 0)
  + COALESCE(CASE WHEN m.labor_cost < 0 THEN NULL ELSE m.labor_cost END, 0)
                                                           AS total_cost,
  m.observation,
  m.reference_da_dr,
  :etl_run_id
FROM staging.maintenance m
WHERE m.date_operation >= :window_start
  AND m.date_operation <  :window_end
  AND m.date_operation >= '2019-10-01'::timestamp
  AND EXISTS (
    SELECT 1 FROM warehouse.dim_vehicle dv
    WHERE dv.tenant_id  = m.tenant_id
      AND dv.vehicule_id = m.vehicule_id
  )
ON CONFLICT (tenant_id, maintenance_id) DO UPDATE
  SET vehicle_id            = EXCLUDED.vehicle_id,
      provider_id           = EXCLUDED.provider_id,
      date_operation        = EXCLUDED.date_operation,
      maintenance_date      = EXCLUDED.maintenance_date,
      actual_repair_time    = EXCLUDED.actual_repair_time,
      repair_duration_hours = EXCLUDED.repair_duration_hours,
      maintenance_type      = EXCLUDED.maintenance_type,
      cost                  = EXCLUDED.cost,
      labor_cost            = EXCLUDED.labor_cost,
      total_cost            = EXCLUDED.total_cost,
      observation           = EXCLUDED.observation,
      reference_da_dr       = EXCLUDED.reference_da_dr,
      _etl_run_id           = EXCLUDED._etl_run_id,
      _loaded_at            = NOW();
