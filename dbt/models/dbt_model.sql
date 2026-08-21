{{ config(
    
    materialized='incremental',
    post_hook=[
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
from {{ source('public', 'orders') }}



{% if is_incremental() %}
    where "OrderID" > (select max("OrderID") from {{ this }})
{% endif %}
