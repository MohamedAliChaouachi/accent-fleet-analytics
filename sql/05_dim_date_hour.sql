-- =============================================================================
-- 05_dim_date_hour.sql
-- =============================================================================
-- Static dimensions: calendar 2019-2027 and 24-row hour band. Run once
-- during bootstrap. Idempotent via ON CONFLICT DO NOTHING.
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
  date_key       INTEGER PRIMARY KEY,              -- YYYYMMDD
  full_date      DATE NOT NULL,
  year           INTEGER NOT NULL,
  quarter        INTEGER NOT NULL,
  month          INTEGER NOT NULL,
  month_name     TEXT NOT NULL,
  day_of_month   INTEGER NOT NULL,
  day_of_week    INTEGER NOT NULL,                 -- 0 = Sunday
  day_name       TEXT NOT NULL,
  is_weekend     BOOLEAN NOT NULL,
  year_month     CHAR(7) NOT NULL,                 -- '2026-04'
  iso_week       INTEGER NOT NULL
);

INSERT INTO warehouse.dim_date
SELECT
  TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
  d::DATE                         AS full_date,
  EXTRACT(YEAR FROM d)::INTEGER   AS year,
  EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
  EXTRACT(MONTH FROM d)::INTEGER  AS month,
  TO_CHAR(d, 'Month')             AS month_name,
  EXTRACT(DAY FROM d)::INTEGER    AS day_of_month,
  EXTRACT(DOW FROM d)::INTEGER    AS day_of_week,
  TO_CHAR(d, 'Day')               AS day_name,
  EXTRACT(DOW FROM d) IN (0, 6)   AS is_weekend,
  TO_CHAR(d, 'YYYY-MM')           AS year_month,
  EXTRACT(WEEK FROM d)::INTEGER   AS iso_week
FROM generate_series('2019-01-01'::date, '2027-12-31'::date, '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_dim_date_full ON warehouse.dim_date (full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_ym   ON warehouse.dim_date (year_month);

-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.dim_hour_band (
  hour_key      INTEGER PRIMARY KEY,                -- 0..23
  period_of_day TEXT NOT NULL,                      -- dawn | morning | afternoon | evening | night
  is_night      BOOLEAN NOT NULL,                   -- 21:00-05:00
  is_rush_hour  BOOLEAN NOT NULL                    -- 07-08 and 17-18
);

INSERT INTO warehouse.dim_hour_band
SELECT
  h,
  CASE
    WHEN h BETWEEN 5  AND 6  THEN 'dawn'
    WHEN h BETWEEN 7  AND 11 THEN 'morning'
    WHEN h BETWEEN 12 AND 16 THEN 'afternoon'
    WHEN h BETWEEN 17 AND 20 THEN 'evening'
    ELSE                           'night'
  END AS period_of_day,
  (h >= 21 OR h <= 5)            AS is_night,
  (h IN (7, 8, 17, 18))          AS is_rush_hour
FROM generate_series(0, 23) AS h
ON CONFLICT (hour_key) DO NOTHING;
