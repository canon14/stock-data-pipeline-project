{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}

    {# 1. CI Safeguard: If DBT_ENV is 'CI', force EVERYTHING into the DEV database #}
    {# This overrides the +database: BRONZE_PRODUCTION settings in dbt_project.yml #}
    {%- if env_var('DBT_ENV', 'DEV') == 'CI' -%}

        {{ env_var('SNOWFLAKE_DEV_DATABASE') | trim }}

    {# 2. Production: Use the custom database (Bronze/Silver/Gold) defined in dbt_project.yml #}
    {%- elif custom_database_name is not none -%}

        {{ custom_database_name | trim }}

    {# 3. Local Dev/Fallback: Use the database defined in your profiles.yml/env file #}
    {%- else -%}

        {{ default_database }}

    {%- endif -%}

{%- endmacro %}
