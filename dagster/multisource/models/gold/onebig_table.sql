{{ config(schema='gold', materialized='table') }}

{% set tables = [
    {'ref': 'silver_startup',    'alias': 's', 'pk': 'CITY'},
    {'ref': 'silver_votertable', 'alias': 'v', 'pk': 'CITY_NAME'},
    {'ref': 'silver_weather',    'alias': 'w', 'pk': 'CITY'}
] %}

SELECT
    {# 1. The main key (No trailing comma here) #}
    {{ tables[0].alias }}.{{ tables[0].pk }} AS CITY
    
    {# 2. Loop through each table and its columns #}
    {%- for table in tables -%}
        {%- set cols = adapter.get_columns_in_relation(ref(table.ref)) -%}
        
        {%- for col in cols -%}
            {#- If the column name is NOT the primary key -#}
            {%- if col.column | upper != table.pk | upper -%}
                {#- We always provide a comma BEFORE the column -#}
                , {{ table.alias }}.{{ col.column }}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
    
    {# 3. Final metadata column (With a leading comma) #}
    , CURRENT_TIMESTAMP() AS last_sync_at

FROM {{ ref(tables[0].ref) }} {{ tables[0].alias }}

{# 4. Standard Join Logic #}
{% for table in tables[1:] %}
LEFT JOIN {{ ref(table.ref) }} {{ table.alias }}
    ON {{ tables[0].alias }}.{{ tables[0].pk }} = {{ table.alias }}.{{ table.pk }}
{% endfor %}