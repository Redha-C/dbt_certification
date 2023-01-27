{% macro  grant_select(schema=target.schema, env=target.name) %}

    {% set query %}
        grant usage on schema {{ schema }} within the {{ env }} environment;
        grant select on all tables in schema {{ schema }} within the {{ env }} environment;
        grant select on all views in schema {{ schema }} within the {{ env }} environment;
    {%  endset %}

    {{ log('Granting select on all tables and views in schema ' ~ schema ~ ' in ' ~ env ~  ' environment ', info=True) }}

    {% do run_query(query) %}

    {{ log('Privileges granted', info=True) }}
{%  endmacro %}
