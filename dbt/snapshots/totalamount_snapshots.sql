{% snapshot TotalAmount %}

{{
    config(
        target_schema='snapshots',
        unique_key= '"OrderID"',
        strategy= 'check',
        check_cols=['"Status"'],
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


