{{ config(
    
    materialized='incremental',
    post_hook=[
        "grant select on {{ this }} to postgres",
        "comment on table {{ this }} is 'UPDATED by dbt'"
    ],
    unique_key= '"OrderID"'

    

) }}


select
   {{ dbt_utils.generate_surrogate_key(['"OrderID"', '"CustomerName"']) }},
    "OrderID",
    "CustomerName",
    "Email",
    "ProductName",
    "Quantity",
    "UnitPrice",
    CASE
        WHEN "TotalAmount" ~ '^[0-9]+\.?[0-9]*$' THEN "TotalAmount"::NUMERIC
        ELSE NULL
    END AS "TotalAmount",
    "OrderDate",
    "Region",
    "Status",
    "Discount"
from {{ source('public', 'test') }}


{% if is_incremental() %}
    where "OrderID" > (select max("OrderID") from {{ this }})
{% endif %}
