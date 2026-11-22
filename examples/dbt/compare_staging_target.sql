-- analyses/compare_staging_target.sql
-- Column-by-column comparison of staging vs target with dbt audit_helper.
-- Run: dbt compile --select compare_staging_target, then run the compiled SQL.

{{ audit_helper.compare_relations(
    a_relation = ref('stg_program_records'),
    b_relation = ref('tgt_program_records'),
    primary_key = 'record_id'
) }}
