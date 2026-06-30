{{ config(materialized='table') }}

select
    '"OrderID"',
    {{ percentage_calc(
        numerator='"Discount"',
        denominator='"TotalAmount"',
    ) }} as "Discount_Percentage",

    {{ percentage_calc(
        numerator='"UnitPrice"',
        denominator='"Quantity"',
    ) }} as "Unitprice_Percentage"

from {{ ref('dbt_model') }}