{% macro get_custom_schema(target, node) %}
    {% if target.name == 'dev' %}
        {# Use the profile name to create a developer-specific schema #}
        dbt_{{ target.user | lower }}
    {% else %}
        {# For CI and production, use folder-based schema naming #}
        {% set file_path = node.original_file_path %}
        
        {% if file_path | contains('/silver/') %}
            {% set base_path = file_path.split('/silver/')[1] %}
        {% elif file_path | contains('/gold/') %}
            {% set base_path = file_path.split('/gold/')[1] %}
        {% else %}
            {{ target.schema }}  {# Default fallback to target schema #}
        {% endif %}
        
        {# Replace slashes with underscores to create schema name #}
        {{ base_path.replace('/', '_').replace('\\', '_') }}
    {% endif %}
{% endmacro %}