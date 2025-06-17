with table_storage as (
  SELECT table_catalog AS project_id
        , CONCAT (table_catalog, ".", table_schema) as dataset_id
        , CONCAT (table_catalog, ".", table_schema, ".", REGEXP_REPLACE(table_name, r'_\d{1,8}\*?$', '_*')) as table_id
        , REGEXP_REPLACE(table_name, r'_\d{1,8}\*?$', '_*') AS table_name
        , CASE WHEN REGEXP_CONTAINS(table_name, r'^[a-z0-9_]+$') 
            THEN 'Yes'
            ELSE 'No'
          END AS naming_snake_case
        , MIN(EXTRACT(DATE FROM creation_time)) AS creation_date
        , MAX(EXTRACT(DATE FROM storage_last_modified_time)) AS storage_last_modified_date
        , deleted
        , Count(table_name) as total_tables
        , SUM(total_rows) total_rows
        , SUM(total_partitions) total_partitions
        , SUM(ROUND(total_logical_bytes / (1024 * 1024 * 1024), 2)) AS total_logical_gb
        , SUM(ROUND(total_physical_bytes/ (1024 * 1024 * 1024), 2)) AS total_physical_gb
        , SUM(ROUND(active_physical_bytes / POW(1024, 3), 2)) AS active_gb
        , SUM(ROUND(long_term_physical_bytes / POW(1024, 3), 2)) AS long_term_gb
        , SUM(ROUND(time_travel_physical_bytes / POW(1024, 3), 2)) AS time_travel_gb
        , SUM(ROUND(fail_safe_physical_bytes / POW(1024, 3), 2)) AS fail_safe_gb
        -- Storage cost calculated as of June 2025
        , SUM(ROUND((active_physical_bytes / POW(1024, 3)) * 0.02 + (long_term_physical_bytes / POW(1024, 3)) * 0.01 + (fail_safe_physical_bytes / POW(1024, 3)) * 0.02,4)) AS storage_cost_usd,
        -- Estimated cost of consultation (assuming everything is scanned) as of June 2025
        , SUM(ROUND((active_physical_bytes + long_term_physical_bytes) / POW(1024, 4) * 5,4)) AS query_cost_usd,
        -- Estimated cost of partitioned consultation (assuming 10% of the data) as of June 2025
        , SUM(ROUND(SAFE_DIVIDE((active_physical_bytes + long_term_physical_bytes) / POW(1024, 4) * 0.1 * 5,total_partitions),4)) AS cost_per_partition_usd
  FROM 
    `region-eu`.INFORMATION_SCHEMA.TABLE_STORAGE
  GROUP BY ALL
  ),

table_detail as (
  SELECT CONCAT (table_catalog, ".", table_schema, ".", REGEXP_REPLACE(table_name, r'_\d{1,8}\*?$', '_*')) as table_id
      , MAX(CASE WHEN option_name= "description" then option_value else null end) as description
      , MAX(CASE WHEN option_name= "labels" then option_value else null end) as labels
      , MAX(CASE WHEN option_name= "tags" then option_value else null end) as tags
      , MAX(CASE WHEN option_name= "uris" then option_value else null end) as uris --Ubicaciones de los archivos externos.
  FROM `region-eu.INFORMATION_SCHEMA.TABLE_OPTIONS`
  GROUP BY ALL
  ),

table_last_query as (
  SELECT PARSE_DATE('%Y%m%d', creation_date) as last_date_query
        , user_email
        , table_id_in_query
        , is_scheduled_query
        , requestor
  FROM `{your_project_id}.control_gcp.jobs_detail`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY table_id_in_query order by  last_date_query DESC ) = 1
  ),

table_num_colums as (
  SELECT CONCAT (table_catalog, ".", table_schema, ".", REGEXP_REPLACE(table_name, r'_\d{1,8}\*?$', '_*')) as table_id  
        , COUNT(distinct column_name) as num_columns
        , MAX(clustering_ordinal_position) as is_clustered
  FROM 
  `region-eu`.INFORMATION_SCHEMA.COLUMNS
  GROUP BY ALL
  ),

table_complete as (
  SELECT ts.*
    , tc.num_columns
    , tc.is_clustered,
    , td.description
    , td.labels,
    , td.tags,
    , td.uris,
    , tq.last_date_query,
    , CASE WHEN tq.is_scheduled_query = "yes" then "scheduled_query" 
          WHEN tq.requestor = "looker_studio" then "looker_studio" 
          else tq.user_email 
        end as query_made_by,
    , DATE_DIFF(CURRENT_DATE(), last_date_query, DAY) AS days_since_last_query,
    , DATE_DIFF(CURRENT_DATE(), storage_last_modified_date, DAY) AS days_since_last_update
  FROM table_storage as ts
  LEFT JOIN table_detail  as td
    on ts.table_id = td.table_id
  LEFT JOIN table_last_query as tq
    on ts.table_id = tq.table_id_in_query
  LEFT JOIN table_num_colums as tc
    on ts.table_id = tc.table_id
  )

SELECT *,
    CASE 
        WHEN regexp_contains(dataset_id, "analytics_") then "no action"
        WHEN regexp_contains(table_name, "test|prueba") then "delete table test"
        WHEN  (days_since_last_query  > 15 or days_since_last_query is null) and days_since_last_update < 7 then "check updates"
        WHEN  (days_since_last_query  > 30 or days_since_last_query is null) and days_since_last_update >35 then "delete table"
        WHEN naming_snake_case = "no" then "change name"
        WHEN description is null then "add description"
        WHEN total_logical_gb > 10 AND total_partitions = 0 THEN 'partition table'
        WHEN total_logical_gb < 10 AND total_partitions > 0 THEN 'unnecessary partition table'
        WHEN total_logical_gb > 53687091200 AND total_partitions > 0 AND is_clustered = 0 THEN 'cluster table'
        WHEN total_physical_gb > 1000 AND total_rows < 1000 THEN 'optimize storage'
        WHEN num_columns > 100 THEN 'reduce columns'
        WHEN total_logical_gb > 1e12 THEN 'monitor cost'
        ELSE 'no action'
      end AS recommendation
FROM table_complete