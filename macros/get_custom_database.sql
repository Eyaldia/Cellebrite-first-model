{% macro get_custom_database(target, node) %}
    {% if target.name == 'dev' %}
        development
    {% elif target.name == 'ci' %}
        staging_test
    {% else %}
        {# Non-dev, non-ci environments use folder-based database logic #}
        {% set file_path = node.original_file_path %}
        {% if file_path | contains('/silver/') %}
            dwh_silver
        {% elif file_path | contains('/gold/') %}
            dwh_gold
        {% else %}
            {{ target.database }}  {# Default fallback to target database #}
        {% endif %}
    {% endif %}
{% endmacro %}