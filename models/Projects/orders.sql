{{ config(materialized='table') }}

WITH tb1 as(
    select id,
    order_date,
    user_id

    from {{source('datafeed_shared_schema1','STG_ORDER')}})
select * from tb1

