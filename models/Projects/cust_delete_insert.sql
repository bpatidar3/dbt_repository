{{ config(materialized="incremental",
         unique_key='id',
         incremental_strategy="delete_insert",
         
) }}

select * from {{source('datafeed_shared_schema1','CUST_INFO_MERGE')}} 
