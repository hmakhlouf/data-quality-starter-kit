-- Demo pipeline: raw (all text) -> staging (typed, mapped) -> target (enterprise model).
-- It contains three realistic ETL bugs on purpose, so the checks can catch them:
--   1. the mapping table is missing variant spellings (rows lose their category)
--   2. a re-run appended part of the batch again instead of merging (duplicates)
--   3. records with an unparseable date are silently dropped before the target (lost records)

-- 1. RAW: the export as received, every column is text
CREATE OR REPLACE TABLE raw_program_records AS
SELECT *, 'B2026_11_22' AS batch_id
FROM read_csv('demo/data/source_export.csv', all_varchar = true, header = true);

CREATE OR REPLACE TABLE dim_program AS
SELECT * FROM read_csv('demo/data/programs.csv', header = true);

-- 2. Mapping table (bug 1: only exact upper-case names are mapped)
CREATE OR REPLACE TABLE map_category (source_value VARCHAR, target_value VARCHAR);
INSERT INTO map_category VALUES
  ('YOUTH SERVICES', 'YOUTH'), ('HOUSING SUPPORT', 'HOUSING'), ('JOB TRAINING', 'EMPLOYMENT'),
  ('HEALTH OUTREACH', 'HEALTH'), ('FOOD ASSISTANCE', 'FOOD');

-- 3. STAGING: typed and mapped
CREATE OR REPLACE TABLE stg_program_records AS
SELECT
  r.record_id,
  NULLIF(TRIM(r.program_id), '')                                   AS program_id,
  m.target_value                                                   AS category,
  LOWER(r.status)                                                  AS status,
  TRY_STRPTIME(r.start_date, '%m/%d/%Y')::DATE                     AS start_date,
  TRY_CAST(REPLACE(REPLACE(r.amount, '$', ''), ',', '') AS DECIMAL(12, 2)) AS amount,
  r.batch_id
FROM raw_program_records r
LEFT JOIN map_category m ON m.source_value = UPPER(r.category);

-- 4. TARGET: enterprise model
CREATE OR REPLACE TABLE tgt_program_records AS
SELECT * FROM stg_program_records
WHERE start_date IS NOT NULL;                                      -- bug 3: silent filter

-- bug 2: a re-run appended 15 records a second time
INSERT INTO tgt_program_records
SELECT * FROM stg_program_records WHERE start_date IS NOT NULL ORDER BY record_id LIMIT 15;

-- Volume history from previous runs (used by the ±5% drift rule)
CREATE OR REPLACE TABLE dq_volume_history (table_name VARCHAR, run_ts TIMESTAMP, row_count BIGINT);
INSERT INTO dq_volume_history VALUES
  ('tgt_program_records', TIMESTAMP '2026-11-20 06:00:00', 990),
  ('tgt_program_records', TIMESTAMP '2026-11-21 06:00:00', 960);
