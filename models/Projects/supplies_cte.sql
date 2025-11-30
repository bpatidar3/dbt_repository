{{
    config(
        materialized='ephemeral'
    )
}}
with source as (

select * from {{ source('datafeed_shared_schema1', 'STG_SUPPLIERS') }}
)
select * from source