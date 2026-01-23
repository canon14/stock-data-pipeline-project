{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        event_time='trade_date',
        begin='2026-01-01',
        batch_size='month',
        lookback=1
    )
}}

/*
    Tables -
*/

WITH source AS (
    SELECT *
    FROM {{ source('finance', 'raw_stock_prices') }}
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY symbol, date
            ORDER BY ingested_at DESC
        ) = 1
),

/*
    Formatted
*/

formatted AS (

    SELECT
        -- PK

        {{ dbt_utils.generate_surrogate_key(['symbol', 'date']) }}
            AS _surrogate_key,

        -- Details
        CAST(symbol AS VARCHAR) AS stock_ticker,

        -- Measures
        CAST(open_price AS FLOAT) AS open_price,
        CAST(open_price AS FLOAT) * 300 AS open_price_times_300,
        CAST(high_price AS FLOAT) AS high_price,
        CAST(low_price AS FLOAT) AS low_price,
        CAST(close_price AS FLOAT) AS close_price,
        CAST(volume AS INT) AS volume, -- noqa: RF04

        -- Metadata
        CAST(date AS DATE) AS trade_date,
        CAST(ingested_at AS TIMESTAMP) AS ingested_at
    FROM
        source

)

SELECT * FROM formatted
