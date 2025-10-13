{% macro standardize_address(field) %}
    lower({{ field }})
{% endmacro %}