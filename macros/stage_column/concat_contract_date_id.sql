{% macro contract_date_id(contract_column, schedule_column, date_column, add_days) -%}


concat( {{ contract_column }}, '-',

   {%- if  schedule_column|length > 1 -%}
      {%- if 'contract_column' != '0' -%}
         {{ schedule_column }},'-',
      {%- endif -%}
   {%- endif -%}

    replace(DATEADD(D,{{ add_days }},{{ date_column }}) ,'-',''))::varchar(50)


{%- endmacro %}