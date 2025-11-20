select
    {{ stage_bksvc_contract_fields(
        contract_no = 'contract_id',
        as_of_date = 'monthended',
        investor_code = 'investor_code',
        contract_type = 'null',
        type_lease_loan = 'Loan',
        _etl_loaded_at = '_etl_loaded_at',
        client_id = 'orion_client_id',
        vendor_code = "'KAPITUS'" ) 
    }}

    {{ empty_date_to_null('originated_on') }} funded_date,
    {{ empty_date_to_null('originated_on') }} origination_date,
    {{ empty_date_to_null('expected_maturity_date') }} maturity_date,
    
    merchant_dba::varchar(300) customer_dba,
    merchant_legal_name::varchar(300) customer_name,
    contract_type::varchar(20) client_contract_type,
    zeroifnull(fico)::numeric fico,
    zeroifnull(yield)::numeric(16,2) borrower_rate,
    state::varchar(5) business_state_code,
    lpad(left(to_varchar(zip),5),5,'0')::varchar(10) business_zip,
    iff(payment_method = 'electronic', 'ach', 'none')::varchar(100) billing_method,
    nullif(sic_code::varchar(20), '') naics_code,
    sic_description::varchar(500) naics_code_desc,    
    zeroifnull(number_of_payments)::numeric term,
    --zeroifnull(replace(original_funding_amount, ',', ''))::numeric(16,2) funded_orig_amt,
    --zeroifnull(original_rtr)::numeric(16,2) rtr_original_bal,
    --zeroifnull(releasedrtr)::numeric(16,2) rtr_released_bal,
    zeroifnull(internal_funding_amount)::numeric(16,2) funded_internal_amt,
    zeroifnull(internal_rtr)::numeric(16,2) rtr_internal_bal,
    --zeroifnull(pool_outstanding_receivable_balance)::numeric(16,2) receivable_pool_bal,
    zeroifnull(internal_funding_amount_remaining)::numeric(16,2) funded_internal_receivable_bal,
    zeroifnull(funded_internal_receivable_bal)::numeric(16,2) receivable_bal,
    zeroifnull(internal_rtr_remaining)::numeric(16,2) rtr_internal_receivable_bal,
    zeroifnull(ANNUALIZED_REVENUE)::numeric(16,2) annual_revenue_amt,
    zeroifnull(past_due_amount)::numeric(16,2) past_due_amt,
    cast(zeroifnull(days_of_no_payment) as numeric) num_days_delin,
    zeroifnull(CALCULATED_FIXED_PAYMENT)::numeric(16,2) payment_amt,
    {{ get_payment_frequency_code_translation('payment_frequency_detailed') }} payment_frequency_code,
    payment_frequency_detailed::varchar(20) payment_frequency,

    account_number::varchar(100) customer_bank_acct_num,
    routing_number::varchar(100) customer_routing_num,
    --dpd_30::numeric times_30_ltd_tally,
   -- dpd_60::numeric times_60_ltd_tally,
    --dpd_90::numeric times_90_ltd_tally,
    {{ empty_date_to_null('_etl_loaded_at') }} _etl_loaded_at

    from {{ source('datafeed_shared_schema', 'KAPITUS_LOANS') }}