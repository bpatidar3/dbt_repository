{{ 
  config(
    tags = ["bksvc", "incremental"],
    materialized = 'incremental'
) 
}}

with cte_kapitus_loan as (
    {{ bksvc_backfill_date_spine(model_name = 'stg_bksvc_kapitus_loan', tsf_model_name= 'tsf_bksvc_contract_kapitus_ctd', vendor_code = 'KAPITUS') }}
),
cte_kapitus_fb as (
    {{ bksvc_backfill_date_spine(model_name = 'stg_bksvc_kapitus_forbright', tsf_model_name= 'tsf_bksvc_contract_kapitus_ctd', vendor_code = 'KAPFB') }}
)

select * from cte_kapitus_loan
union all 
select * from cte_kapitus_fb