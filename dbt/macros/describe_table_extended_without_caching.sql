{#-
    Override of dbt-spark's macro of the same name (fallback used because Iceberg v2 tables
    don't support SHOW TABLE EXTENDED, SPARK-33393).

    dbt-spark joins the DESCRIBE EXTENDED rows into "name: value" lines, but its column parser
    (INFORMATION_COLUMNS_REGEX) expects the schema tree format " |-- col: type (nullable = ...)"
    that SHOW TABLE EXTENDED would have produced. So the catalog came back empty and
    `dbt docs` showed no columns. Here the column rows are folded into one "Schema" row in
    that tree format; the table detail rows (Type, Provider: iceberg, ...) pass through.
-#}
{% macro describe_table_extended_without_caching(table_name) %}
  {% call statement('describe_table_extended_without_caching', fetch_result=True) -%}
    describe extended {{ table_name }}
  {%- endcall %}
  {% set rows = load_result('describe_table_extended_without_caching').table.rows %}
  {% set columns = [] %}
  {% set details = [] %}
  {% set ns = namespace(in_columns=true) %}
  {% for row in rows %}
    {% if row[0] is none or row[0] | trim == '' or row[0].startswith('#') %}
      {% set ns.in_columns = false %}
    {% elif ns.in_columns %}
      {% do columns.append(' |-- ' ~ row[0] ~ ': ' ~ row[1] ~ ' (nullable = true)') %}
    {% else %}
      {% do details.append([row[0], row[1], row[2]]) %}
    {% endif %}
  {% endfor %}
  {% do details.append(['Schema', 'root\n' ~ (columns | join('\n')), '']) %}
  {{ return(details) }}
{% endmacro %}
