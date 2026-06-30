{{ config(materialized='incremental') }}

{{ dbt_utils.date_spine(
    datepart='day',
    start_date="cast('2026-01-01' as date)",
    end_date="cast('2030-12-31' as date)"
) }}
