/* This macro tells dbt: "If I provide a custom schema in my YAML, use it exactly as written." */
{% macro generate_schema_name(custom_schema_name, node) -%}
    
    {%- if custom_schema_name is none -%}
        
        {{ target.schema }}
    
    {%- else -%}
        
        {{ custom_schema_name | trim }}
    
    {%- endif -%}

{%- endmacro %}