from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# Import your existing class logic (make sure your script is in a shared folder)
from scripts.ingest_stock_data import StockExtractor


def run_ingestion():
    # We move your "MAIN EXECUTION FLOW" logic here
    # Access your credentials via Airflow Variables or Environment variables
    extractor = StockExtractor(...)
    tickers = ["AAPL", "MSFT", "GOOGL", "TSLA"]
    for ticker in tickers:
        raw_json = extractor.fetch_past_7_days_daily_data(symbol=ticker)
        validated = extractor.validate_and_process_7_days(ticker, raw_json)
        extractor.upload_7_days_to_s3(validated, s3_bucket="your-bucket")


with DAG(
    "stock_market_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    # Task 1: Python Ingestion (API -> S3)
    ingest_task = PythonOperator(
        task_id="ingest_api_to_s3", python_callable=run_ingestion
    )

    # Task 2: Snowflake COPY INTO (S3 -> Snowflake)
    load_snowflake_task = SnowflakeOperator(
        task_id="copy_s3_to_snowflake",
        snowflake_conn_id="snowflake_default",
        sql="""
            COPY INTO RAW.FINANCE.RAW_STOCK_PRICES
            FROM (
              SELECT $1:symbol::varchar, $1:date::date, $1:open_price::float, ...
              FROM @raw.external_stage.s3_external_stage_stock_price
            )
            FILE_FORMAT = (FORMAT_NAME = 'raw.file_format.stock_price_parquet_format')
        """,
    )

    # Task 3: dbt build
    dbt_task = BashOperator(
        task_id="dbt_transform",
        bash_command="cd /usr/local/airflow/dbt_snowflake_pipeline && dbt build --profiles-dir .",
    )

    # Define Dependencies
    ingest_task >> load_snowflake_task >> dbt_task
