{#
  Escape seed literals because dbt-spark session bindings do not escape apostrophes in
  taxi-zone names. Remove this override when dbt-spark handles them.
#}

{% macro spark_escape_literal(value) %}
    {%- if value is none -%}
        null
    {%- elif value is number or value is boolean -%}
        {{ value }}
    {%- else -%}
        {#- Escape backslashes before apostrophes because Spark treats them as escapes. -#}
        '{{ value | string | replace('\\', '\\\\') | replace("'", "''") }}'
    {%- endif -%}
{% endmacro %}


{% macro spark__load_csv_rows(model, agate_table) %}
  {% set batch_size = get_batch_size() %}
  {% set column_override = model['config'].get('column_types', {}) %}
  {% set statements = [] %}

  {% for chunk in agate_table.rows | batch(batch_size) %}
      {% set sql %}
          insert into {{ this.render() }} values
          {% for row in chunk -%}
              ({%- for col_name in agate_table.column_names -%}
                  {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
                  {%- set type = column_override.get(col_name, inferred_type) -%}
                    cast({{ spark_escape_literal(row[loop.index0]) }} as {{ type }})
                  {%- if not loop.last %},{% endif -%}
              {%- endfor -%})
              {%- if not loop.last %},{% endif -%}
          {%- endfor %}
      {% endset %}

      {% do adapter.add_query(sql, abridge_sql_log=True) %}

      {% if loop.index0 == 0 %}
          {% do statements.append(sql) %}
      {% endif %}
  {% endfor %}

  {{ return(statements[0]) }}
{% endmacro %}
