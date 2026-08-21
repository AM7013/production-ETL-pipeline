{{config (materialized='incremental')}}

SELECT 
    "Status",
    COUNT(*) as total_status
FROM {{ref('dbt_model')}}
{{ dbt_utils.group_by(1) }}  