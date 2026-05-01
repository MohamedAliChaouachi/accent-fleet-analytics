-- =============================================================================
-- 24_fact_fueling_incr.sql
-- =============================================================================
-- fact_fueling: per-fueling-event row joining the polymorphic document header
-- (staging.document WHERE doc_type='Fueling') to the fueling-specific detail
-- (staging.fueling). Header carries date / cost / vehicle / provider; detail
-- carries quantity / odometer / consumption / payment metadata.
--
-- Watermark column: document.date_operation (the fuel-up event date — the
-- only reliable timestamp; staging.fueling.date is fully NULL).
--
-- Natural keys are unreliable (reference_unique is sparse), so this loader
-- uses DELETE-INSERT-on-window with id_document as the parent key.
--
-- Cleaning rules applied inline:
--   - Negative cost / quantity / odometer clamped to NULL
--   - quantity must be > 0 to be considered a real fuel-up (else NULL)
--   - vehicule_id must exist in dim_vehicle
--   - Excludes doc_type='FuelingMonthly' (those are aggregates, not events)
--
-- Contract:
--   :window_start, :window_end, :etl_run_id   (standard incremental)
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_fueling (
  fueling_sk         BIGSERIAL PRIMARY KEY,
  id_document        BIGINT NOT NULL,
  tenant_id          INTEGER NOT NULL,
  vehicle_id         BIGINT NOT NULL,
  provider_id        BIGINT,
  payment_type_id    BIGINT,
  -- Event time
  fueling_at         TIMESTAMP NOT NULL,
  fueling_date       DATE NOT NULL,
  -- Quantities & financials
  quantity_l         DOUBLE PRECISION,             -- litres
  cost_total         DOUBLE PRECISION,             -- from document.cost
  cost_per_litre     DOUBLE PRECISION,             -- cost_total / quantity_l (computed)
  cost_unit          DOUBLE PRECISION,             -- staging.fueling.cost_unit (raw)
  -- Vehicle state at fuel-up
  odometre_km        BIGINT,
  consommation       DOUBLE PRECISION,             -- l/100km if pre-computed by source
  -- Operational
  num_cart_fuel      VARCHAR,
  lieu               VARCHAR,
  observation        VARCHAR,
  reference_unique   VARCHAR,                      -- not unique, kept for trace only
  -- Lineage
  _etl_run_id        BIGINT,
  _loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_fueling_event_time
  ON warehouse.fact_fueling (fueling_at DESC);
CREATE INDEX IF NOT EXISTS idx_fact_fueling_vehicle_month
  ON warehouse.fact_fueling (tenant_id, vehicle_id, fueling_date);
CREATE INDEX IF NOT EXISTS idx_fact_fueling_doc
  ON warehouse.fact_fueling (tenant_id, id_document);

-- -----------------------------------------------------------------------------
-- Step 1 — purge in-window rows for idempotent re-insert.
-- -----------------------------------------------------------------------------
DELETE FROM warehouse.fact_fueling ff
USING staging.document d
WHERE ff.tenant_id   = d.tenant_id
  AND ff.id_document = d.id_document
  AND d.doc_type      = 'Fueling'
  AND d.date_operation >= :window_start
  AND d.date_operation <  :window_end;

-- -----------------------------------------------------------------------------
-- Step 2 — re-insert.
-- -----------------------------------------------------------------------------
INSERT INTO warehouse.fact_fueling (
  id_document, tenant_id, vehicle_id, provider_id, payment_type_id,
  fueling_at, fueling_date,
  quantity_l, cost_total, cost_per_litre, cost_unit,
  odometre_km, consommation,
  num_cart_fuel, lieu, observation, reference_unique, _etl_run_id
)
SELECT
  d.id_document,
  d.tenant_id,
  d.vehicule_id                                           AS vehicle_id,
  d.provider_id,
  f.payment_type_id,
  d.date_operation                                        AS fueling_at,
  d.date_operation::DATE                                  AS fueling_date,
  -- quantity: must be > 0 to be a valid fuel-up
  CASE WHEN f.quantity > 0 THEN f.quantity ELSE NULL END  AS quantity_l,
  CASE WHEN d.cost     >= 0 THEN d.cost   ELSE NULL END   AS cost_total,
  CASE
    WHEN f.quantity > 0 AND d.cost >= 0
    THEN d.cost / f.quantity
    ELSE NULL
  END                                                     AS cost_per_litre,
  CASE WHEN f.cost_unit >= 0 THEN f.cost_unit ELSE NULL END AS cost_unit,
  CASE WHEN f.odometre  > 0 THEN f.odometre  ELSE NULL END AS odometre_km,
  CASE WHEN f.consommation >= 0 THEN f.consommation ELSE NULL END AS consommation,
  f.num_cart_fuel,
  f.lieu,
  f.observation,
  f.reference_unique,
  :etl_run_id
FROM staging.document d
JOIN staging.fueling   f
  ON f.tenant_id   = d.tenant_id
 AND f.id_document = d.id_document
WHERE d.doc_type = 'Fueling'
  AND d.date_operation >= :window_start
  AND d.date_operation <  :window_end
  AND d.date_operation >= '2019-10-01'::timestamp
  AND d.vehicule_id IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM warehouse.dim_vehicle dv
    WHERE dv.tenant_id  = d.tenant_id
      AND dv.vehicule_id = d.vehicule_id
  );
