{%- macro bksvc_backfill_date_spine(model_name, tsf_model_name, vendor_code='x') -%}
{# --Get min and max date range for each contract; for any missing rows populate with closest earlier date #}
{%- set schema   = model.schema -%}
{%- set schema2   = target.name -%}
{%- set database   = model.database  -%}
{%- set qualified_model_name =  database ~ '.' ~ schema ~ '.' ~ model_name -%}
{%- set qualified_tsf_model_name =  database ~ '.' ~ schema ~ '.' ~ tsf_model_name -%}


with cte_contracts as (
    select 
        *
        , max(imported_at) over (partition by contract_id, as_of_date, investor_code) max_imported_at_by_month
        , max(imported_at) over (partition by contract_id, investor_code) max_imported_at
        , max(as_of_date) over (partition by contract_id, client_id) max_as_of_date
        , min(as_of_date) over (partition by contract_id, client_id) min_as_of_date

    from {{ ref(model_name) }}

)
 -----------------------------------------------------------------------------------------
 --select all dates between contract min and max as_of_date
 --[date_to_join] = the nearest closest earlier date to be used to fill in missing dates 
  --select the remaining fields from information_schema column list
 -----------------------------------------------------------------------------------------

{%- set test = "select column_name column_name from "~ model.database ~ ".information_schema.columns where concat(table_catalog, '.', table_schema, '.', table_name) ilike '" ~ ref(model_name) ~ "' and lower(column_name) not in ('as_of_date', 'contract_date_id', 'contract_id', 'imported_at') order by ordinal_position" %}
--{{test}}

{%- set list_columns = run_query("select column_name column_name from "~ model.database ~ ".information_schema.columns where concat(table_catalog, '.', table_schema, '.', table_name) ilike '" ~ ref(model_name) ~ "' and lower(column_name) not in ('as_of_date', 'contract_date_id', 'contract_id', 'imported_at') order by ordinal_position") %}

select 
    dwd_date.date_actual as_of_date,
    cte_contracts.contract_id contract_id,
    concat(cte_contracts.contract_id, '-',  replace(dwd_date.date_actual ,'-','')) contract_date_id,
    imported_at,
    cte_contracts.as_of_date orig_row_source_date,
    {%- for item in list_columns %}
    cte_contracts.{{ item[0] }} as {{ item[0] }},
    {%- endfor %}
    client.client_name,
    max_imported_at_by_month,
    max_imported_at,
    sysdate() as _row_inserted_at

  from RAW.BKSVC.dwd_date
    join cte_contracts 
        on cte_contracts.max_imported_at_by_month = cte_contracts.imported_at
        and dwd_date.last_day_of_month = cte_contracts.as_of_date
    
    left join RAW.BKSVC.dwd_client client on cte_contracts.client_id = client.client_id
         
  where
    dwd_date.date_Actual between  min_as_of_date and dateadd(day, -1, current_date())
  
  {%- if is_incremental() %}
      and date_actual > (select ifnull(max(as_of_date), '2000-01-01') from {{ qualified_tsf_model_name }} 
     
        {% if vendor_code != 'x' %}
        where vendor_code = '{{ vendor_code }}'
        {% endif %}
        )
  {% endif %}  
{%- endmacro -%} 
