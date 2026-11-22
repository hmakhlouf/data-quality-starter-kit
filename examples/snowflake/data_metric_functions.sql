-- Snowflake native data quality: Data Metric Functions (DMFs).
-- Snowflake runs them on a schedule and stores results in SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS.

ALTER TABLE tgt_program_records SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

ALTER TABLE tgt_program_records ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT      ON (program_id);
ALTER TABLE tgt_program_records ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (record_id);
ALTER TABLE tgt_program_records ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT       ON ();

-- Custom DMF: values outside the allowed status list
CREATE OR REPLACE DATA METRIC FUNCTION dq.invalid_status_count(t TABLE(status VARCHAR))
RETURNS NUMBER AS
'SELECT COUNT(*) FROM t WHERE status IS NULL OR status NOT IN (''open'', ''closed'', ''pending'')';

ALTER TABLE tgt_program_records ADD DATA METRIC FUNCTION dq.invalid_status_count ON (status);

-- Read the results
SELECT measurement_time, metric_name, argument_names, value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_name = 'TGT_PROGRAM_RECORDS'
ORDER BY measurement_time DESC;
