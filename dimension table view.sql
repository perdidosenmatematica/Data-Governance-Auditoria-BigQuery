SELECT table_catalog as project_id
      , CONCAT (table_catalog, ".", table_schema) as dataset_id
      , CONCAT (table_catalog, ".", table_schema, ".", REGEXP_REPLACE(table_name, r'_\d{1,8}\*?$', '_*')) as table_id
      , REGEXP_REPLACE(table_name, r'_\d{1,8}\*?$', '_*') AS table_view_name
      , table_type
      , MIN(EXTRACT(DATE FROM creation_time)) as first_creation_date
      , MAX(EXTRACT(DATE FROM creation_time)) as last_creation_date
FROM `region-eu`.INFORMATION_SCHEMA.TABLES
GROUP BY ALL