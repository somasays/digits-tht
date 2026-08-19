{#
  Build every table as Delta, and build it with the statement Delta can actually run.

  dbt-spark emits no `using` clause when file_format is unset, so its tables came out
  Hive/Parquet while the two PySpark writes were Delta -- a warehouse half in each format.

  Setting `+file_format: delta` is the documented fix and it fails here: it flips dbt onto
  `create or replace table ... using delta as select`, which this Spark rejects with
  "does not support truncate in batch mode". That is not a dbt fault -- the same statement
  fails typed straight into the session, on Delta 3.2.1 and 3.3.2 alike, table present or
  absent. Plain `create table ... using delta as select` works.

  So the format is declared here rather than in config. Leaving `file_format` unset is what
  makes it work: dbt then takes its non-delta branches, which drop the old relation and
  issue a plain `create table` -- the two things this Spark wants -- while this clause
  supplies the format. Setting `+file_format` on a model returns it to dbt's own handling.

  Delete this when `create or replace table ... using delta` works on the pinned Spark and
  Delta, at which point `+file_format: delta` in dbt_project.yml is the whole change.
#}

{% macro spark__file_format_clause() %}
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}
  using {{ file_format if file_format is not none else 'delta' }}
{%- endmacro -%}
