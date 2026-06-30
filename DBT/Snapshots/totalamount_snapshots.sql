{% snapshot totalamount_snapshots %}

{{
    config(
        target_schema='snapshots',
        unique_key= '"OrderID"',
        strategy='timestamp',
        updated_at = '"OrderDate"',
        post_hook=[
            "grant select on {{ this }} to postgres",
            "comment on table {{ this }} is 'UPDATED by dbt'"
        ],
        
    )
}}

select
    *,
    CAST("OrderDate" AS DATE) AS OrderDate 
from {{ source('public', 'test') }}

{% endsnapshot %}


