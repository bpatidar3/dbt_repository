{% macro get_payment_frequency_code_translation(payment_frequency_code) -%}
{#- -- map to consistent payment frequency codes that match the SORs -#}
    case 
        --Misc values from backup servicing-------------------------------------
        when ifnull({{ payment_frequency_code }}, '') ilike 'Daily' then 'DA'
        when ifnull({{ payment_frequency_code }}, '') ilike 'Weekly' then 'WE'
        when ifnull({{ payment_frequency_code }}, '') ilike 'Monthly' then 'MO'
        when ifnull({{ payment_frequency_code }}, '') ilike 'Bi-Weekly' then 'BW'
        --LW values------------------------------------------------------------
        when ifnull({{ payment_frequency_code }}, '') = 'A' then 'AN'
        when ifnull({{ payment_frequency_code }}, '') = 'M' then 'MO'
        when ifnull({{ payment_frequency_code }}, '') = 'Q' then 'QU'
        when ifnull({{ payment_frequency_code }}, '') = 'S' then 'SA'        
        else ifnull({{ payment_frequency_code }}, '')
    end
{% endmacro %}