{% macro default__compare_row_counts(a_relation, b_relation,c_relation) %}

        select
            '{{ a_relation }}' as relation_name,
            count(*) as total_records
        from {{ a_relation }}

        union all

        select
            '{{ b_relation }}' as relation_name,
            count(*) as total_records
        from {{ b_relation }}

        
        union all

        select
            '{{ c_relation }}' as relation_name,


            count(*) as total_records
        from {{ c_relation }}
  
{% endmacro %}

