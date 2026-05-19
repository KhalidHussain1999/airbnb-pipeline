{% macro set_query_tag() -%}
    {% set new_query_tag = model.name if model is defined else 'dbt_run' %}
    {% if new_query_tag %}
        {% set original_query_tag = run_query("SELECT CURRENT_QUERY_TAG()").columns[0][0] %}
        {{ log("Setting query_tag to: " ~ new_query_tag) }}
        {% do run_query("ALTER SESSION SET QUERY_TAG = '{}'".format(new_query_tag)) %}
        {{ return(original_query_tag) }}
    {% endif %}
    {{ return('') }}
{% endmacro %}