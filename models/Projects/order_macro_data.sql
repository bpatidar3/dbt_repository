{{ config(materialized='table') }}
with  source as (
select * from {{source('datafeed_shared_schema1','STG_PRODUCT_DATA')}}
),

final_data as (select sku as product_id,name as product_name,
     type as product_type,
     description as product_description,
    {{ cents_to_dollars('price') }} as product_price,
    coalesce(type = 'amazon', false) as is_food_item,
    coalesce(type = 'beverage', false) as is_drink_item
 from source

)
select * from final_data

