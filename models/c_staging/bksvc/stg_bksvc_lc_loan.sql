select
    {{ stage_bksvc_contract_fields(
        contract_no = 'contract_no',
        as_of_date = 'monthended',
        investor_code = 'investor_code',
        contract_type = 'null',
        type_lease_loan = 'Loan',
        _etl_loaded_at = 'etl_loaded_date',
        client_id = 'orion_client_id',
        vendor_code = "'LIGT_ATLAS'") 
    }}
    --investor_code::varchar(50)     vendor_code,
    --dates----------------------
    {{ empty_date_to_null('commencement_date') }}       commencement_date,
    {{ empty_date_to_null('first_payment_due_date') }}  first_payment_due_date,
    {{ empty_date_to_null('last_payment_date') }}       last_payment_date,
    {{ empty_date_to_null('next_due_date') }}           next_due_date,
    {{ empty_date_to_null('non_accrual_date') }}        non_accrual_date,
    {{ empty_date_to_null('charge_off_date') }}         charge_off_date,
    {{ empty_date_to_null('maturity_date') }}           maturity_date,
    {{ empty_date_to_null('closed_date') }}             closed_date,
    --amts-----------------------
    zeroifnull(replace(orig_cost, ',', ''))::numeric(16,2)              orig_contract_amt,
    zeroifnull(replace(current_principal_bal, ',', ''))::numeric(16,2)  principal_bal,
    zeroifnull(replace(current_principal_bal, ',', ''))::numeric(16,2)  receivable_bal,
    zeroifnull(replace(payment_amt, ',', ''))::numeric(16,2)                payment_amt,
    zeroifnull(replace(last_payment_amt, ',', ''))::numeric(16,2)           last_payment_amt,
    zeroifnull(replace(charge_off_principal, ',', ''))::numeric(16,2)       charge_off_amt,
    zeroifnull(replace(total_principal_due_amt, ',', ''))::numeric(16,2)    principal_due_amt,
    zeroifnull(replace(total_interest_due_amt, ',', ''))::numeric(16,2)     interest_due_amt,
    zeroifnull(replace(current_fees_bal, ',', ''))::numeric(16,2)           current_fees_bal,
    -----------------------------
    term::numeric                   term,
    remaining_term::numeric         remaining_term,    
    num_days_delin::numeric         num_days_delin,
    num_payments_complete::numeric  num_payments_made,
    -----------------------------
    ifnull(cast(revenue_based_rate as numeric(10,2)),0) revenue_based_rate,
    ifnull(cast(interest_rate as numeric(10,2)),0)      borrower_rate,
    -----------------------------
    {{ get_payment_frequency_code_translation('payment_type_code') }} payment_frequency_code,
    payment_type_code payment_frequency,
    cast(billing_method_code as varchar(100)) billing_method_code,
    cast(loan_type_code as varchar(100)) client_loan_type_code,
    cast(interest_type as varchar(100)) interest_type,
    cast(asset_code as varchar(100)) client_asset_code,
    naics_code::varchar(20)     naics_code,
    --customer-----------------------------
    customer_no::varchar(100)   customer_id,
    customer_name::varchar(300) customer_name,
    tax_id::varchar(20)         tin,
    {{ concat_street_address('billing_street2', 'billing_street2', 'null', 'business_')}},  
    billing_city::varchar(100)      business_city,
    billing_state::varchar(50)      business_state_name,
    billing_zip::varchar(20)        business_zip,
    primary_phone::varchar(20)      phone,
    email_address::varchar(320)     email_address,
    customer_routing_num::varchar(100)          customer_routing_num,
    customer_bank_acct_num::varchar(100)        customer_bank_acct_num,
    customer_bank_acct_type_code::varchar(10)   customer_bank_acct_type_code,
    _etl_loaded_at
from {{ source('datafeed_shared_schema', 'LC_LOAN') }}