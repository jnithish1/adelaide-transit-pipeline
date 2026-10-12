{#
    Clean up after a CI run: drops every schema created for this pull request
    (CI_PR_<n>, CI_PR_<n>_STAGING, CI_PR_<n>_MARTS, ...).

        dbt run-operation drop_ci_schemas --args "{pr: '42'}"
#}
{% macro drop_ci_schemas(pr) %}
    {% set prefix = 'CI_PR_' ~ pr %}
    {% set schemas = run_query(
        "select schema_name from " ~ target.database ~ ".information_schema.schemata "
        ~ "where schema_name = '" ~ prefix ~ "' or schema_name like '" ~ prefix ~ "\\\\_%'"
    ) %}
    {% for row in schemas %}
        {% do log('dropping ' ~ row[0], info=true) %}
        {% do run_query('drop schema if exists ' ~ target.database ~ '.' ~ row[0] ~ ' cascade') %}
    {% endfor %}
{% endmacro %}
