{{ config(
    materialized="incremental",
    incremental_strategy="append",
) }}

select * from {{source('datafeed_shared_schema1','STG_ORDER')}}  where id in (1,2,4)
