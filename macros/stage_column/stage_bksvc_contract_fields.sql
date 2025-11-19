{% macro stage_bksvc_contract_fields(contract_no,as_of_date,investor_code,contract_type,type_lease_loan,_etl_loaded_at,client_id,vendor_code) -%}
   {{as_of_date}}::date  as_of_date,
   {{_etl_loaded_at}}::date    import_date,
   {{_etl_loaded_at}}::date   imported_at,
   max({{as_of_date}}) over (order by (select 1)) latest_file_date,
   {{contract_no}}::varchar(100) client_contract_id,
   concat(cast({{contract_no}} as varchar(100)),'_',investor_code) contract_id,
   {{ contract_date_id(
        contract_column = 'contract_id',
        schedule_column = null,
        date_column = as_of_date,
        add_days = 0) }} contract_date_id,
    {{ investor_code }}::varchar(10)      investor_code,
    coalesce({{ vendor_code }}, investor_code) vendor_code,
    {{ client_id }}  client_id,
    '{{ type_lease_loan }}' type_lease_loan, 
    case 
        when lower({{ contract_type }}) like 'direct finance%' then 'DFL'
        when lower({{ contract_type }}) like 'operating%' then 'OPER'
        else upper('{{ type_lease_loan }}')
    end::varchar(5)   as contract_type_code,
    case 
        when lower({{ contract_type }}) like 'direct finance%' then {{ contract_type }}
        when lower({{ contract_type }}) like 'operating%' then {{ contract_type }}
        else '{{ type_lease_loan }}'
    end::varchar(50)                 as contract_type,  -- description tied to contract_type_code in leaseworks  
    'Backup Servicing'      orion_servicing_type,
    {{ as_of_date }}::date  orion_book_date,
{% endmacro %} 