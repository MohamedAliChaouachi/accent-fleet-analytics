-- =============================================================================
-- 01_dim_tenant.sql
-- =============================================================================
-- Tenant dimension. Idempotent upsert from distinct tenant_ids in staging.
-- Full-scan is fine — there are 5 tenants.
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.dim_tenant (
  tenant_id     INTEGER PRIMARY KEY,
  tenant_label  TEXT,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

WITH source AS (
  SELECT DISTINCT tenant_id
  FROM staging.path
  WHERE tenant_id IS NOT NULL
  UNION
  SELECT DISTINCT tenant_id FROM staging.device WHERE tenant_id IS NOT NULL
)
INSERT INTO warehouse.dim_tenant (tenant_id, tenant_label)
SELECT tenant_id, 'tenant_' || tenant_id::text
FROM source
ON CONFLICT (tenant_id) DO UPDATE
  SET last_seen_at = NOW();
