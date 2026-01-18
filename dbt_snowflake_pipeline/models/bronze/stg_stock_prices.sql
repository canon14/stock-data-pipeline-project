{{
  config(
    materialized = 'table',
    )
}}

/* 
    Tables 
*/

with source as (
    select *
    from {{ source('finance', 'raw_stock_prices') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY SYMBOL, DATE ORDER BY INGESTED_AT DESC) = 1
),

/* 
    Formatted 
*/

formatted as (
    
    select 
        -- PK
        {{ dbt_utils.generate_surrogate_key(['symbol', 'date']) }} AS _surrogate_key,
        
        -- Details
        CAST(symbol AS VARCHAR) AS symbol,
        
        -- Measures
        CAST(open_price AS FLOAT) AS open_price,
        CAST(high_price AS FLOAT) AS high_price,
        CAST(low_price AS FLOAT) AS low_price,
        CAST(close_price AS FLOAT) AS close_price,
        CAST(volume AS INT) AS volume,

        -- Metadata
        CAST(date AS DATE) AS trade_date,
        CAST(ingested_at AS TIMESTAMP) AS ingested_at
    from
        source

)

select * from formatted


