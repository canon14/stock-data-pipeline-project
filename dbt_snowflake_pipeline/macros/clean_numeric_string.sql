{% macro clean_numeric_string(column_name) %}
    -- Removes $, commas, and any non-numeric characters except the decimal point
    REGEXP_REPLACE({{ column_name }}, '[^0-9.]', '')::FLOAT
{% endmacro %}
