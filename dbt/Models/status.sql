{{config (materialized='incremental')}}

with orders as (
    select * from {{ref('dbt_model')}}
    where "Status" != 'Cancelled'
),

aggerated as (
    SELECT 
        "Status",
        COUNT(*)
    FROM orders
    GROUP BY "Status"
)

select * from aggerated
