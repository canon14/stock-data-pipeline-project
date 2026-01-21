{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {# 1. CI Environment: Use the branch-based schema name set in GitHub Actions #}
    {%- if env_var('DBT_ENV', 'DEV') == 'CI' -%}

        {{ env_var('SNOWFLAKE_SCHEMA') | trim }}

    {# 2. Production Environment: Use ONLY the custom schema name (e.g., 'landing', 'analytics') #}
    {# This gives you clean names like GOLD_PRODUCTION.ANALYTICS.MY_MODEL #}
    {%- elif target.name == 'prod' and custom_schema_name is not none -%}

        {{ custom_schema_name | trim }}

    {# 3. Local Dev Environment: Concatenate for developer isolation (e.g., 'MAX_LANDING') #}
    {%- elif custom_schema_name is not none -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {# 4. Fallback: If no custom schema is defined, use the profile default #}
    {%- else -%}

        {{ default_schema }}

    {%- endif -%}

{%- endmacro %}
