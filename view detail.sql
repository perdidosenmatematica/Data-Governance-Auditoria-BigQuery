With table_base_view as (
    SELECT CONCAT(project_id,".", dataset_id) as dataset_id
            , CONCAT(project_id,".", dataset_id,".", view_id) as view_id
            ,view_id as view_name
            , CASE WHEN REGEXP_CONTAINS(table_name, r'^[a-z0-9_]+$') 
                    THEN 'Yes'
                    ELSE 'No'
                END AS naming_snake_case
            ,CASE WHEN starts_with(origin_table_id, "jobs") or origin_table_id ="region-eu" then CONCAT(project_id,".","INFORMATION_SCHEMA") else origin_table_id end as origin_table
            ,CASE WHEN starts_with(origin_table_id, "jobs") or origin_table_id ="region-eu" then CONCAT(project_id,".","INFORMATION_SCHEMA") else REGEXP_REPLACE(origin_table_id, r'_\d{1,8}\*?$', '_*') end as origin_table_group
    FROM (SELECT table_catalog as project_id
                , table_schema as dataset_id
                , table_name as view_id
                , REPLACE(REGEXP_EXTRACT(LOWER(view_definition), r'from\s*`([^`]+(?:`\.\s*`[^`\s]+)*)`'), r'`', '') AS  origin_table_id
            FROM `region-eu`.INFORMATION_SCHEMA.VIEWS)
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
        , tc.is_clustered
        , td.description
        , td.labels
        , td.tags
        , td.uris
        , tq.last_date_query
        , CASE WHEN tq.is_scheduled_query = "yes" 
                THEN "scheduled_query" 
                ELSE tq.user_email 
            END as query_made_by
        , DATE_DIFF(CURRENT_DATE(), last_date_query, DAY) AS days_since_last_query
    FROM table_base_view as ts
    LEFT JOIN table_detail  as td
        on ts.view_id = td.table_id
    LEFT JOIN table_last_query as tq
        on ts.view_id = tq.table_id_in_query
    LEFT JOIN table_num_colums as tc
        on ts.view_id = tc.table_id
  )

SELECT *,
    CASE 
        WHEN regexp_contains(view_name, "test|prueba") then "delete view test"
        WHEN  days_since_last_query  > 30 then "view unnecessary"
        WHEN correct_naming = "no" then "change name"
        WHEN description is null then "add description"
        WHEN num_columns > 100 THEN 'reduce columns'
        ELSE 'no action'
      end AS recommendation
FROM table_complete