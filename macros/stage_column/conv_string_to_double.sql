{% macro conv_string_to_double(column_name) -%}
try_to_double(nullif(replace(replace(replace(trim({{ column_name }}), ',',''),'$',''),' ',''), ''))
{%- endmacro %}