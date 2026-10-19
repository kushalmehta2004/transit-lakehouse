{#- Use the configured schema as-is (gold), not dbt's default "<target>_<custom>" concatenation. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
