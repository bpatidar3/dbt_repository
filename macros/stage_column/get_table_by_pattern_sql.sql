{% macro get_tables_by_pattern_sql(schema_pattern, table_pattern, exclude='', database=target.database) %}
    {{ return(adapter.dispatch('get_tables_by_pattern_sql')
        (schema_pattern, table_pattern, exclude, database)) }}
{% endmacro %}

{% macro default__get_tables_by_pattern_sql(schema_pattern, table_pattern, exclude='', database=target.database) %}

        select distinct
            table_schema as {{ adapter.quote('table_schema') }},
            table_name as {{ adapter.quote('table_name') }},
            {{ get_table_types_sql() }}
        from {{ database }}.information_schema.tables
        where table_schema ilike '{{ schema_pattern }}'
        and table_name ilike '{{ table_pattern }}'
        and table_name not ilike '{{ exclude }}'

{% endmacro %}