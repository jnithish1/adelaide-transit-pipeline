{#
    Overrides of two dbt-snowflake metadata macros.

    dbt-snowflake 1.12 sends SHOW ... with a LIMIT clause on a new line, and
    current Snowflake accounts reject it ("syntax error ... unexpected 'limit'").
    These versions drop the LIMIT. This project has a handful of schemas and
    objects, far below the 10,000 the LIMIT was guarding against.
    Remove this file once dbt-snowflake ships a fix.
#}

{% macro snowflake__list_schemas(database) -%}
  {% set sql -%}
    show terse schemas in database "{{ database | replace('"', '') }}"
  {%- endset %}
  {{ return(run_query(sql)) }}
{%- endmacro %}


{% macro snowflake__show_objects_sql(schema, max_results_per_iter=10000, watermark=none) -%}
  {%- set _sql -%}
show objects in {{ schema }}
  {%- endset -%}
  {%- do return(_sql) -%}
{%- endmacro %}
