{{ 
  config(
    tags = ["bksvc", "incremental"],
    materialization = 'incremental'
) 
}}

{%- set tsf_source_models = [
    
    'tsf_bksvc_contract_lighter_capital_ctd',
    'tsf_bksvc_contract_kapitus_ctd'
    ] 
-%}
    {%- set list_of_sources = [] -%}
    {%- for tsf in tsf_source_models -%}
        {%- do list_of_sources.append(ref(tsf)) -%}
    {%- endfor -%}

WITH all_union_records AS (
    {%- set schema   = model.schema  -%} 
    
    {%- set bksvc_relations = get_relations_by_pattern('''' ~ schema ~ '''',database = '''' ~ model.database ~ '''', table_pattern = 'tsf_bksvc_contract_%_ctd') %} 
    
    {{ union_relations
        (
            relations = bksvc_relations,
            column_override = {
                '_DBT_SOURCE_RELATION' : 'VARCHAR(100)',
                'AS_OF_DATE' : 'DATE',
                'IMPORT_DATE' : 'DATE',
                'LATEST_FILE_DATE' : 'DATE',
                'CLIENT_CONTRACT_ID' : 'VARCHAR(100)',
                'CONTRACT_ID' : 'VARCHAR(50)',
                'CONTRACT_DATE_ID' : 'VARCHAR(100)',
                'INVESTOR_CODE' : 'VARCHAR(255)',
                'CLIENT_ID' : 'VARCHAR(255)',
                'TYPE_LEASE_LOAN' : 'VARCHAR(5)',
                'CONTRACT_TYPE_CODE' : 'VARCHAR(20)',
                'CONTRACT_TYPE' : 'VARCHAR(100)',
                'ORION_SERVICING_TYPE' : 'VARCHAR(16)',
                'TERM' : 'NUMERIC(38, 0)',
                'REMAINING_TERM' : 'NUMERIC(38, 0)',
                'PAST_DUE_0_AMT' : 'NUMERIC(16, 2)',
                'PAST_DUE_1_10_AMT' : 'NUMERIC(16, 2)',
                'PAST_DUE_11_30_AMT' : 'NUMERIC(16, 2)',
                'PAST_DUE_31_60_AMT' : 'NUMERIC(16, 2)',
                'PAST_DUE_61_90_AMT' : 'NUMERIC(16, 2)',
                'PAST_DUE_91_PLUS_AMT' : 'NUMERIC(16, 2)',
                'IS_CURRENT' : 'NUMERIC(1,0)',
                'IS_PAST_DUE_1_10' : 'NUMERIC(1,0)',
                'IS_PAST_DUE_11_30' : 'NUMERIC(1,0)',
                'IS_PAST_DUE_31_60' : 'NUMERIC(1,0)',
                'IS_PAST_DUE_61_90' : 'NUMERIC(1,0)',
                'IS_PAST_DUE_91_PLUS' : 'NUMERIC(1,0)',
                'VENDOR_CODE' : 'VARCHAR(100)',
                'ORION_BOOK_DATE' : 'DATE',
                'NEXT_DUE_DATE' : 'DATE',
                'RECEIVABLE_BAL' : 'NUMERIC(16, 2)',
                'ORIG_CONTRACT_AMT' : 'NUMERIC(16, 2)',
                'RTR_OUTSTANDING_BAL' : 'NUMERIC(16, 2)',
                
                'PAST_DUE_AMT' : 'NUMERIC(16, 2)',
                'IS_CLOSED' : 'NUMERIC(1, 0)',
                'CUSTOMER_NAME' : 'VARCHAR(300)',
                'BUSINESS_STREET' : 'VARCHAR(100)',
                'BUSINESS_STREET_2' : 'VARCHAR(100)',
                'BUSINESS_CITY' : 'VARCHAR(100)',
                'BUSINESS_ZIP' : 'VARCHAR(100)',
                'NUM_YEARS_IN_BUSINESS' : 'NUMERIC(5, 0)',
                'PAYMENT_FREQUENCY_CODE' : 'VARCHAR(255)',
                '_ETL_LOADED_AT' : 'TIMESTAMP_NTZ'}
        ) }}
)
,cte_bksvc as (
select  distinct
        bksvc._dbt_source_relation,
        bksvc.as_of_date,
        bksvc.import_date,
        bksvc.imported_at,
        bksvc.latest_file_date,
        bksvc.client_contract_id,
        bksvc.contract_id, 
        bksvc.contract_date_id,
        bksvc.investor_code,
        bksvc.client_id,
        raw_client.client_name,
        bksvc.type_lease_loan,
        bksvc.contract_type_code,
        bksvc.contract_type,
        bksvc.orion_servicing_type,
        ------------------------------------------------------
        bksvc.term,
        bksvc.remaining_term,
        bksvc.num_days_delin,
        --backup servicing report fields----------------------
        iff(bksvc.num_days_delin < 1, bksvc.receivable_bal, 0) past_due_0_amt,
        iff(bksvc.num_days_delin between 1 and 10, bksvc.receivable_bal, 0) past_due_1_10_amt,
        iff(bksvc.num_days_delin between 11 and 30, bksvc.receivable_bal, 0) past_due_11_30_amt,
        iff(bksvc.num_days_delin between 31 and 60, bksvc.receivable_bal, 0) past_due_31_60_amt,
        iff(bksvc.num_days_delin between 61 and 90, bksvc.receivable_bal, 0) past_due_61_90_amt,
        iff(bksvc.num_days_delin > 90, bksvc.receivable_bal, 0) past_due_91_plus_amt,

        iff(bksvc.num_days_delin < 1, 1, 0) is_current,
        iff(bksvc.num_days_delin between 1 and 10, 1, 0) is_past_due_1_10,
        iff(bksvc.num_days_delin between 11 and 30, 1, 0) is_past_due_11_30,
        iff(bksvc.num_days_delin between 31 and 60, 1, 0) is_past_due_31_60,
        iff(bksvc.num_days_delin between 61 and 90, 1, 0) is_past_due_61_90,
        iff(bksvc.num_days_delin > 90, 1, 0) is_past_due_91_plus,
        bksvc.vendor_code,
        --dates-----------------
        bksvc.orion_book_date,
        bksvc.next_due_date,
        --amts-------------------
        bksvc.orig_contract_amt,
        bksvc.funded_amt,
        bksvc.receivable_bal,
        bksvc.rtr_outstanding_bal,
        bksvc.past_due_amt,
        --flags------------------
        ifnull(bksvc.is_closed, 0) is_closed,
        --customer info----------
        bksvc.customer_name,
        bksvc.business_street,
        bksvc.business_street_2,
        bksvc.business_city,
        
        --business_county,
        bksvc.num_years_in_business,
        bksvc.payment_frequency_code,
        bksvc._etl_loaded_at

    from all_union_records bksvc
    
    

    left join RAW.BKSVC.DWD_CLIENT raw_client  on lower(bksvc.client_id) = lower(raw_client.client_id)
    {% if is_incremental() %}  
    left join (
                select max(as_of_date) max_as_of_date, vendor_code
                from {{ this }}
                group by vendor_code
                ) as max_client_date
            on bksvc.vendor_code = max_client_date.vendor_code
    where 
        (
            bksvc.as_of_date > max_client_date.max_as_of_date
            or max_client_date.vendor_code is null --new clients
        )
    {% endif %}
)
,cte_range as (
      select 
        bksvc.*,
        _client.client_name sf_client_name,
        max(as_of_date) over(partition by iff(bksvc.client_id = '0014R00003B46oxQAB',client_contract_id,vendor_code), bksvc.client_id) max_as_of_date,
        max(imported_at) over(partition by iff(bksvc.client_id = '0014R00003B46oxQAB',client_contract_id,vendor_code), bksvc.client_id) max_imported_at
    
    from cte_bksvc bksvc
    
        left join RAW.BKSVC.DWD_CLIENT RAW_CLIENT on lower(bksvc.client_id) = lower(RAW_CLIENT.client_id)
    
)
select  distinct
        bksvc._dbt_source_relation,
        dwd_date.date_actual as_of_date,
        bksvc.import_date,
        bksvc.imported_at,
        bksvc.latest_file_date,
        bksvc.client_contract_id,
        bksvc.contract_id, 
        concat(bksvc.contract_id, '-',  replace(dwd_date.date_actual ,'-','')) contract_date_id,
        bksvc.investor_code,
        bksvc.client_id,
        cte_range.sf_client_name client_name,
        bksvc.type_lease_loan,
        bksvc.contract_type_code,
        bksvc.contract_type,
        bksvc.orion_servicing_type,
        ------------------------------------------------------
        bksvc.term,
        bksvc.remaining_term,
        bksvc.num_days_delin,
        --backup servicing report fields----------------------
        iff(bksvc.num_days_delin < 1, bksvc.receivable_bal, 0) past_due_0_amt,
        iff(bksvc.num_days_delin between 1 and 10, bksvc.receivable_bal, 0) past_due_1_10_amt,
        iff(bksvc.num_days_delin between 11 and 30, bksvc.receivable_bal, 0) past_due_11_30_amt,
        iff(bksvc.num_days_delin between 31 and 60, bksvc.receivable_bal, 0) past_due_31_60_amt,
        iff(bksvc.num_days_delin between 61 and 90, bksvc.receivable_bal, 0) past_due_61_90_amt,
        iff(bksvc.num_days_delin > 90, bksvc.receivable_bal, 0) past_due_91_plus_amt,

        iff(bksvc.num_days_delin < 1, 1, 0) is_current,
        iff(bksvc.num_days_delin between 1 and 10, 1, 0) is_past_due_1_10,
        iff(bksvc.num_days_delin between 11 and 30, 1, 0) is_past_due_11_30,
        iff(bksvc.num_days_delin between 31 and 60, 1, 0) is_past_due_31_60,
        iff(bksvc.num_days_delin between 61 and 90, 1, 0) is_past_due_61_90,
        iff(bksvc.num_days_delin > 90, 1, 0) is_past_due_91_plus,
        bksvc.vendor_code,
        --dates-----------------
        bksvc.orion_book_date,
        bksvc.next_due_date,
        
        --amts-------------------
        bksvc.orig_contract_amt,
        
        bksvc.rtr_outstanding_bal,
        bksvc.past_due_amt,
        --flags------------------
        ifnull(bksvc.is_closed, 0) is_closed,
        --customer info----------
        bksvc.customer_name,
        bksvc.business_street,
        bksvc.business_street_2,
        bksvc.business_city,
        bksvc.business_zip,
        --business_county,
        bksvc.num_years_in_business,
        bksvc.payment_frequency_code,
        bksvc._etl_loaded_at

    from all_union_records bksvc

        join cte_range
            on bksvc.client_contract_id = cte_range.client_contract_id
            and cte_range.as_of_date = bksvc.as_of_date
            and cte_range.client_id = bksvc.client_id
            ---------------------------------------------------------
            and cte_range.max_imported_at = cte_range.imported_at
            and cte_range.as_of_date = cte_range.max_as_of_date
            ---------------------------------------------------------
                
        join RAW.BKSVC.DWD_DATE dwd_date
            on dwd_date.date_actual > max_as_of_date   
            and dwd_date.date_actual < current_date()
            and cte_range.vendor_code = bksvc.vendor_code --finova has contracts that move portfolio, causes duplicates (i.e. 40107526_FINOVA)
            
{% if is_incremental() %}
      where date_actual > (select ifnull(max(as_of_date), '2000-01-01') from {{ qualified_tsf_model_name }} where {{ qualified_tsf_model_name }}.vendor_code = bksvc.vendor_code)
  {% endif %}  

union all 
select * from cte_bksvc
