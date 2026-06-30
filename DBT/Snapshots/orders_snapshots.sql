{% snapshot orders_snapshots %}

{{
    config(
        target_schema='snapshots',
        unique_key='"OrderID"',
        strategy='timestamp',
        updated_at='OrderDate'
    )
}}

select
    *,
    CAST("OrderDate" AS DATE) AS OrderDate  
from {{ source('public', 'test') }}



{% endsnapshot %}
