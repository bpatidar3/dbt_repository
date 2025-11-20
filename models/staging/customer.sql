{{ config(materialized='table') }}

WITH tb1  as(
 select
        id ,
        first_name,
        last_name
     from {{source('datafeed_shared_schema','STG_CUSTOMER_DATA')}})
     select * from tb1
