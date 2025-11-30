{{
    config(
        materialized='table'
    )
}}
with source as (

select * from {{ ref('snap_customers_timestamp') }}
)
select * from source where dbt_valid_to is null