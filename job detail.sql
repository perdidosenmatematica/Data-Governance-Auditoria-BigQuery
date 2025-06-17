SELECT FORMAT_DATE('%Y%m%d',EXTRACT(DATE from creation_time)) as creation_date 
      , J.project_id as project_id_paid
      , CASE WHEN (SELECT value from unnest(labels) where key = "requestor") is not null then CONCAT ((SELECT value from unnest(labels) where key = "requestor"), " by ", user_email) else user_email end as user_email
      , job_type
      , statement_type
      , TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_seconds
      , state
      , total_bytes_processed
      , total_slot_ms
      , destination_table.project_id as destination_project
      , destination_table.dataset_id as destination_dataset
      , destination_table.table_id as destination_table
      , referenced_tables.project_id as referenced_project
      , referenced_tables.dataset_id as referenced_dataset
      , referenced_tables.table_id as referenced_table
      , CASE WHEN starts_with(lower(referenced_tables.table_id), "information_schema") then CONCAT(referenced_tables.project_id, ".", referenced_tables.table_id)
            WHEN statement_type in ("SELECT","DELETE") THEN REPLACE(REGEXP_EXTRACT(LOWER(query), r'from\s*`([^`]+(?:`\.\s*`[^`\s]+)*)`'), r'`', '')
            WHEN statement_type = "UPDATE" THEN REPLACE(REGEXP_EXTRACT(LOWER(query), r'update\s*`?([^`\s]+(?:`\.\s*`[^`\s]+)*)`?'), r'`', '')
            WHEN statement_type = "CREATE_TABLE_AS_SELECT" THEN REPLACE(REGEXP_EXTRACT(LOWER(query), r'table\s*`?([^`\s]+)`?'), r'`', '')
            WHEN statement_type = "INSERT" THEN REPLACE(REGEXP_EXTRACT(LOWER(query), r'into\s*`?([^`\s]+)`?'), r'`', '')
       END as table_id_in_query
      --, query
      , job_stages.status
      , SUM(total_bytes_billed) as total_bytes_billed
      , ROUND(SUM(total_bytes_billed) / POW(10, 12),4) AS total_TB_billed
      , ROUND(SUM(total_bytes_billed) / POW(10, 12) * 6.25,4) AS estimated_cost_usd
      , job_creation_reason.code as job_creation_reason
      , CASE WHEN (SELECT value from unnest(labels) where key = "data_source_id") is not null then "yes" else "no" end as is_scheduled_query
      , (SELECT value from unnest(labels) where key = "requestor") as requestor
      , (SELECT value from unnest(labels) where key = "looker_studio_report_id") as looker_studio_report_id
      , error_result.message AS error_message
FROM `region-eu`.INFORMATION_SCHEMA.JOBS as J, UNNEST(referenced_tables) as referenced_tables, UNNEST(job_stages) as job_stages
GROUP BY ALL