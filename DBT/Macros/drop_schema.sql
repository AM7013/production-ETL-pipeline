{% macro drop_schema(schema_name) %}
    {% set query %}
        DROP SCHEMA IF EXISTS {{ schema_name }} CASCADE;
    {% endset %}
    {% do run_query(query) %}
    {{ log("Schema " ~ schema_name ~ " dropped", info=True) }}
{% endmacro %}
