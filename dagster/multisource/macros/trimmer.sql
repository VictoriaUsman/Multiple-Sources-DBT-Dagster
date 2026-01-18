{% macro trim_all_columns(model_name, exclude_list=[]) %}  {# <--- Crucial line #}

    {%- set columns = adapter.get_columns_in_relation(model_name) -%}

    {% for col in columns %}
        {# Filter out excluded columns #}
        {% if col.column.upper() not in exclude_list|map('upper') %}
            
            {% if col.is_string() %}
                TRIM({{ col.quoted }}) AS {{ col.quoted }}
            {% else %}
                {{ col.quoted }}
            {% endif %}
            
            {# Add a comma if it's not the last item in the list #}
            {% if not loop.last %},{% endif %}
            
        {% endif %}
    {% endfor %}

{% endmacro %}