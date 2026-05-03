-- =====================================================================
--  01_schema_inspection.sql
--  Inspect tables, columns, primary keys and foreign keys
--  Usage: psql -d accent -f 01_schema_inspection.sql
-- =====================================================================

-- 1. Tables of interest in staging and warehouse
SELECT table_schema, table_name,
       pg_size_pretty(pg_total_relation_size('"'||table_schema||'"."'||table_name||'"')) AS total_size
FROM information_schema.tables
WHERE table_schema IN ('staging', 'warehouse', 'marts')
  AND table_type = 'BASE TABLE'
ORDER BY pg_total_relation_size('"'||table_schema||'"."'||table_name||'"') DESC;

-- 2. Columns with their type and nullability
SELECT
    table_schema,
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default,
    ordinal_position
FROM information_schema.columns
WHERE table_schema IN ('staging', 'warehouse', 'marts')
ORDER BY table_schema, table_name, ordinal_position;

-- 3. Primary keys
SELECT tc.table_schema, tc.table_name, kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema IN ('staging', 'warehouse', 'marts')
ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position;

-- 4. Foreign keys
SELECT
    tc.table_schema   AS src_schema,
    tc.table_name     AS src_table,
    kcu.column_name   AS src_column,
    ccu.table_schema  AS tgt_schema,
    ccu.table_name    AS tgt_table,
    ccu.column_name   AS tgt_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema IN ('staging', 'warehouse', 'marts')
ORDER BY src_schema, src_table;

-- 5. Indexes
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname IN ('staging', 'warehouse', 'marts')
ORDER BY schemaname, tablename, indexname;
