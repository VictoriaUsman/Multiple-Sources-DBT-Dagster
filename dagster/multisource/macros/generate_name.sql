{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {# This line forces dbt to use ONLY the custom name (BRONZE) #}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %} 