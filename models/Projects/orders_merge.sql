{{ config(
materialized="incremental",
incremental_strategy= "merge"
) }}
select * from {{source('datafeed_shared_schema1','STG_ORDER')}} 
