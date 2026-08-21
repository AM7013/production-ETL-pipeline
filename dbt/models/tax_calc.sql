{{config (materialized='incremental')}}


select
    "OrderID",
    "TotalAmount",
    {{ tax_calc('"UnitPrice"', '"Quantity"', 10) }} as tax_amount
from {{ ref('dbt_model') }}