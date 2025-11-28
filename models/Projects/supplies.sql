{{
    config(
        materialized='ephemeral'
    )
}}
with source as (

select * from {{ source('datafeed_shared_schema1', 'STG_SUPPLIERS') }}
), 
renamed as (

select id as supply_id,
sku as product_id,
name as supply_name,
cost as supply_cost,
perishable as is_perishable_supply
from source
)
select * from renamed
