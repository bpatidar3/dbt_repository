{{ 
  config(
    tags = ["bksvc", "incremental"],
    materialized = 'incremental'
) 
}}

with cte_lc as (
    {{ bksvc_backfill_date_spine(model_name = 'stg_bksvc_lc_loan', tsf_model_name= 'tsf_bksvc_contract_lighter_capital_ctd', vendor_code = 'LIGT_ATLAS') }}
),
cte_lci80 as (
    {{ bksvc_backfill_date_spine(model_name = 'stg_bksvc_lci80_loan', tsf_model_name= 'tsf_bksvc_contract_lighter_capital_ctd', vendor_code = 'LIGT_I80') }}
)

select * from cte_lc
union all 
select * from cte_lci80