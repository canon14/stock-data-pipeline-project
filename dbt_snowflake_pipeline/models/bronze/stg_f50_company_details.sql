/*
    Tables -
*/

WITH source AS (

    SELECT *
    FROM {{ ref('fortune_50_companies_jan_2026') }}

),

/*
    Formatted
*/

formatted AS (

    SELECT
        -- PK
        CAST(stock_ticker AS VARCHAR) AS stock_ticker,

        -- Details
        CAST(company_name AS VARCHAR) AS company_name,
        CAST(rank AS INT) AS current_f50_rank,
        CAST(ceo AS VARCHAR) AS company_ceo,
        CAST(headquarters AS VARCHAR) AS company_headquarters,
        CAST(industry AS VARCHAR) AS company_industry,

        -- Measures
        {{ clean_numeric_string('revenue') }} AS total_revenue_in_million,
        {{ clean_numeric_string('employees') }} AS total_employees
    FROM
        source

)

SELECT * FROM formatted
