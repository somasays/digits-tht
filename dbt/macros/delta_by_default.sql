{#
  Default dbt tables to Delta. The pinned stack rejects dbt-spark's create-or-replace
  statement, while its drop-and-create path works when this macro supplies `using delta`.
  Remove this override when the pinned versions support create-or-replace, then configure
  `+file_format: delta` in dbt_project.yml.
#}

{% macro spark__file_format_clause() %}
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}
  using {{ file_format if file_format is not none else 'delta' }}
{%- endmacro -%}
