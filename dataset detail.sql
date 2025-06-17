SELECT catalog_name as project_id
      , CONCAT(catalog_name, ".",schema_name ) as dataset_id
      , schema_name as dataset_name
      , EXTRACT(DATE FROM creation_time) as creation_date
      , EXTRACT(DATE FROM last_modified_time) as last_modified_date
      , location
FROM `region-eu`.INFORMATION_SCHEMA.SCHEMATA