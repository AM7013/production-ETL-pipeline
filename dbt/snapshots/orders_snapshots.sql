{% snapshot orders %}

{{
    config(
        target_schema='snapshots',
        unique_key= '"OrderID"',
        strategy= 'check',
        check_cols=['"Status"']
    )
}}

select
    *,
    CAST("OrderDate" AS DATE) AS OrderDate  
from {{ source('public', 'orders') }}



{% endsnapshot %}
