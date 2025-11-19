{% macro concat_street_address(street, street_2, street_3, alias_prefix) -%}
    nullif(ltrim(rtrim({{ street }})) ,'')::varchar(200) as {{ alias_prefix }}street,
    nullif(ltrim(rtrim({{ street_2 }})) ,'')::varchar(200) as {{ alias_prefix }}street_2,
    nullif(ltrim(rtrim({{ street_3 }})) ,'')::varchar(200) as {{ alias_prefix }}street_3,
     case 
        when {{ alias_prefix }}street_2 is null and {{ alias_prefix }}street_3 is null then {{ alias_prefix }}street
        when {{ alias_prefix }}street_2 is not null and {{ alias_prefix }}street_3 is not null then concat({{ alias_prefix }}street, ' ', {{ alias_prefix }}street_2, ' ', {{ alias_prefix }}street_3)
        when {{ alias_prefix }}street_2 is not null then concat({{ alias_prefix }}street, ' ', {{ alias_prefix }}street_2)
        when {{ alias_prefix }}street_3 is not null then concat({{ alias_prefix }}street, ' ', {{ alias_prefix }}street_3)
        else replace(concat({{ alias_prefix }}street, ' ', {{ alias_prefix }}street_2, ' ', {{ alias_prefix }}street_3), '  ','')
    end::varchar(200) {{ alias_prefix }}street_address

{%- endmacro -%} 