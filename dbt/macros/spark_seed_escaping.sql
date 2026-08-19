{#
  dbt-spark's session method substitutes seed values with
  `dbt/adapters/spark/session.py::_fix_binding`, which wraps a string in single quotes
  and escapes nothing. Four zone names contain an apostrophe -- Governor's Island,
  Prince's Bay -- so the generated INSERT fails to parse and the seed, and everything
  downstream of ref('taxi_zone_lookup'), never builds.

  This inlines escaped literals instead of using binding characters. Delete it when a
  dbt-spark release escapes its own bindings.
#}

{% macro spark_escape_literal(value) %}
    {%- if value is none -%}
        null
    {%- elif value is number or value is boolean -%}
        {{ value }}
    {%- else -%}
        {#- Backslash first: it is itself an escape character in Spark's default mode,
            so doubling the quote before the backslash would re-escape the escape. -#}
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
