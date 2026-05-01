-- =============================================================================
-- 19_fact_maintenance_line_incr.sql
-- =============================================================================
-- fact_maintenance_line: line-level detail under fact_maintenance, unioning
-- three staging sub-tables that all FK on staging.maintenance.id_maintenance:
--
--   line_type='offense'    ← staging.offense    (traffic offences charged
--                                                 against a maintenance order)
--   line_type='sinistre'   ← staging.sinistre   (insurance claims / damage)
--   line_type='reparation' ← staging.reparation (repair line items, parts)
--
-- None of the source tables expose a reliable natural key (reference_unique
-- is sparse and non-unique), so this loader uses a DELETE-INSERT-on-window
-- strategy keyed off the parent maintenance.date_operation. Idempotent: a
-- re-run for the same window produces the same final rows.
--
-- Watermark column: parent maintenance.date_operation (lines have no own
-- timestamp). Late-arriving lines beyond the parent's window are picked up
-- by the next incremental cycle if their parent date is touched, otherwise
-- by a periodic backfill.
--
-- Contract:
--   :window_start, :window_end, :etl_run_id
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_maintenance_line (
  maintenance_line_sk   BIGSERIAL PRIMARY KEY,
  maintenance_id        BIGINT NOT NULL,        -- FK -> fact_maintenance.maintenance_id
  tenant_id             INTEGER NOT NULL,
  line_type             VARCHAR NOT NULL,       -- 'offense' | 'sinistre' | 'reparation'
  -- Denormalised parent attributes (so the BI layer can filter by date/vehicle
  -- without re-joining):
  vehicle_id            BIGINT NOT NULL,
  date_operation        TIMESTAMP NOT NULL,
  maintenance_date      DATE NOT NULL,
  -- Common(ish) descriptors
  description           TEXT,                   -- offense.place / sinistre.description_damage / reparation.label
  place                 VARCHAR,                -- offense.place / sinistre.place
  type_code             BIGINT,                 -- offense.offense_type / sinistre.cabinet_expertise / reparation.reparation_type_id
  reference_unique      VARCHAR,
  -- Financials (line-level)
  amount                DOUBLE PRECISION,       -- sinistre.refund / reparation.total_ttc
  amount_secondary      DOUBLE PRECISION,       -- reparation.total_cost (excl. tax) / total_tva
  -- Reparation-only attributes
  odometre              BIGINT,
  quantity              BIGINT,
  num_facture           VARCHAR,
  mark                  VARCHAR,
  -- Lineage
  _etl_run_id           BIGINT,
  _loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_maintenance_line_parent
  ON warehouse.fact_maintenance_line (tenant_id, maintenance_id);
CREATE INDEX IF NOT EXISTS idx_fact_maintenance_line_type_date
  ON warehouse.fact_maintenance_line (line_type, maintenance_date DESC);
CREATE INDEX IF NOT EXISTS idx_fact_maintenance_line_vehicle_month
  ON warehouse.fact_maintenance_line (tenant_id, vehicle_id, maintenance_date);

-- -----------------------------------------------------------------------------
-- Step 1 — purge any prior lines whose parent maintenance row falls in this
-- window, so the re-insert is idempotent. We resolve "in window" via the
-- staging.maintenance row (single source of truth for date_operation).
-- -----------------------------------------------------------------------------
DELETE FROM warehouse.fact_maintenance_line fl
USING staging.maintenance m
WHERE fl.tenant_id      = m.tenant_id
  AND fl.maintenance_id = m.id_maintenance
  AND m.date_operation >= :window_start
  AND m.date_operation <  :window_end;

-- -----------------------------------------------------------------------------
-- Step 2 — re-insert lines for in-window parents.
-- A CTE pre-resolves parent attributes once so each UNION leg stays simple.
-- -----------------------------------------------------------------------------
WITH parents AS (
  SELECT
    m.tenant_id,
    m.id_maintenance      AS maintenance_id,
    m.vehicule_id         AS vehicle_id,
    m.date_operation,
    m.date_operation::DATE AS maintenance_date
  FROM staging.maintenance m
  WHERE m.date_operation >= :window_start
    AND m.date_operation <  :window_end
    AND m.date_operation >= '2019-10-01'::timestamp
    AND EXISTS (
      SELECT 1 FROM warehouse.dim_vehicle dv
      WHERE dv.tenant_id  = m.tenant_id
        AND dv.vehicule_id = m.vehicule_id
    )
)
INSERT INTO warehouse.fact_maintenance_line (
  maintenance_id, tenant_id, line_type,
  vehicle_id, date_operation, maintenance_date,
  description, place, type_code, reference_unique,
  amount, amount_secondary,
  odometre, quantity, num_facture, mark, _etl_run_id
)
-- ---- offense ----
SELECT
  p.maintenance_id, p.tenant_id, 'offense',
  p.vehicle_id, p.date_operation, p.maintenance_date,
  o.place                            AS description,
  o.place,
  o.offense_type                     AS type_code,
  o.reference_unique,
  NULL::DOUBLE PRECISION             AS amount,
  NULL::DOUBLE PRECISION             AS amount_secondary,
  NULL::BIGINT                       AS odometre,
  NULL::BIGINT                       AS quantity,
  NULL::VARCHAR                      AS num_facture,
  NULL::VARCHAR                      AS mark,
  :etl_run_id
FROM staging.offense o
JOIN parents p
  ON p.tenant_id = o.tenant_id
 AND p.maintenance_id = o.id_maintenance

UNION ALL
-- ---- sinistre ----
SELECT
  p.maintenance_id, p.tenant_id, 'sinistre',
  p.vehicle_id, p.date_operation, p.maintenance_date,
  s.description_damage               AS description,
  s.place,
  s.cabinet_expertise                AS type_code,
  s.reference_unique,
  s.refund::DOUBLE PRECISION         AS amount,
  NULL::DOUBLE PRECISION             AS amount_secondary,
  NULL::BIGINT                       AS odometre,
  NULL::BIGINT                       AS quantity,
  NULL::VARCHAR                      AS num_facture,
  NULL::VARCHAR                      AS mark,
  :etl_run_id
FROM staging.sinistre s
JOIN parents p
  ON p.tenant_id = s.tenant_id
 AND p.maintenance_id = s.id_maintenance

UNION ALL
-- ---- reparation ----
SELECT
  p.maintenance_id, p.tenant_id, 'reparation',
  p.vehicle_id, p.date_operation, p.maintenance_date,
  r.label                            AS description,
  NULL::VARCHAR                      AS place,
  r.reparation_type_id               AS type_code,
  r.reference_unique,
  COALESCE(r.total_ttc, r.total_cost::DOUBLE PRECISION)
                                     AS amount,
  r.total_tva                        AS amount_secondary,
  r.odometre,
  r.quantity,
  r.num_facture,
  r.mark,
  :etl_run_id
FROM staging.reparation r
JOIN parents p
  ON p.tenant_id = r.tenant_id
 AND p.maintenance_id = r.id_maintenance;
