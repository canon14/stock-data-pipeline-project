import os
import io
import logging
import time
from dotenv import load_dotenv
import requests
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from pydantic import BaseModel, Field, field_validator, ConfigDict #import pydantic for data validation and cleaning
from typing import List
from datetime import date, datetime

logger = logging.getLogger(__name__)

# Search for .env in the parent directory
load_dotenv("../.env")

# Uses Pydantic to impose data contract and clean column names receive from API
class DailyStockData(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    # Instead of how by default it's named 1.open, the column will be renamed as open_price
    symbol: str
    date: date
    open_price: float = Field(alias="1. open")
    high_price: float = Field(alias="2. high")
    low_price: float = Field(alias="3. low")
    close_price: float = Field(alias="4. close")
    volume: int = Field(alias="5. volume")

    # use field validator to ensure the required columns exist
    @field_validator("open_price", "high_price", "low_price", "close_price")
    @classmethod
    # data test to ensure prices are not less or equal to 0
    def price_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("Stock prices must be greater than zero")
        return v

class StockExtractor:
    def __init__(self, api_key: str, aws_access_key: str, aws_secret_key: str, region: str):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        
        # Initialize the S3 Client using the credentials from the .env
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region
            )
            logger.info("✅ S3 Client initialized successfully.")
            print("✅ S3 Client initialized successfully.") #for development
        except Exception as e:
            logger.error(f"❌ Failed to initialize S3 Client: {e}")
            print(f"❌ Failed to initialize S3 Client: {e}") #for development
            raise

    def fetch_past_7_days_daily_data(self, symbol: str) -> dict:
        """
        Fetches raw daily stock data from the Alpha Vantage API.
        
        Args:
            symbol (str): The stock ticker (e.g., 'AAPL').
            
        Returns:
            dict: The raw JSON response from the API.
            
        Raises:
            HTTPError: If the API returns a non-200 status code.
            RemoteDisconnected: Handled via the custom headers and timeout.
        """
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",
            "apikey": self.api_key
        }
        
        # Industry Standard: Disguise the script as a browser to prevent 
        # the API server from dropping the connection.
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124"
        }
        
        # Timeout added to prevent the script from hanging if the server is slow
        response = requests.get(self.base_url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        return response.json()

    def validate_and_process_7_days(self, symbol: str, raw_data: dict) -> List[DailyStockData]:
        """Parses the last 7 available days from the API response."""
        time_series = raw_data.get("Time Series (Daily)", {})
        
        # 1. Get the sorted dates (newest first)
        all_dates = sorted(time_series.keys(), reverse=True)
        
        # 2. Slice the first 7 dates
        latest_7_dates = all_dates[:7]
        
        validated_records = []
        
        for date_str in latest_7_dates:
            metrics = time_series[date_str]
            # Validate each day using your Pydantic model
            record = DailyStockData(
                symbol=symbol,
                date=date_str,
                **metrics
            )
            validated_records.append(record)
            
        logger.info(f"✅ Processed {len(validated_records)} historical rows for {symbol}")
        print(f"✅ Processed {len(validated_records)} historical rows for {symbol}") #for development
        return validated_records

    def upload_7_days_to_s3(self, records: List[DailyStockData], s3_bucket: str | None = None) -> None:
        """Converts the list of 7 records into a single Parquet file."""

        # Convert list of Pydantic models to a list of dicts for Pandas
        df = pd.DataFrame([r.model_dump() for r in records])
        df['ingested_at'] = datetime.now()
        df['ingested_at'] = df['ingested_at'].astype(str) #convert created_at to varchar and do the timestamp transformation in snowflake

        # df.to_csv('final_data_test.csv')
        
        # Use today's date in the filename for tracking and proper s3 bucket folder structure
        current_date = date.today()
        current_year = current_date.year
        current_month = current_date.month
        
        file_key = f"raw/stocks/{records[0].symbol}/{current_year}/{current_month}/{current_date}_7day_window.parquet"
        
        # creates an in-memory file-like object that handles binary data (bytes)
        parquet_buffer = io.BytesIO()
        # convert dataframe to parquet before load
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        
        self.s3_client.put_object(
            Bucket=s3_bucket,
            Key=file_key,
            Body=parquet_buffer.getvalue()
        ) 

# --- MAIN EXECUTION FLOW ---
if __name__ == "__main__":
    # 1. LOAD ENVIRONMENT VARIABLES
    # We pull these from the .env file. If a variable is missing, os.getenv returns None.
    ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
    S3_BUCKET_DESTINATION = os.getenv("STOCK_DATA_AWS_S3_BUCKET_NAME")
    AWS_ACCESS_KEY_ID = os.getenv("STOCK_DATA_AWS_S3_ACCESS_KEY_ID")  # Fixed: No trailing comma
    AWS_SECRET_ACCESS_KEY = os.getenv("STOCK_DATA_AWS_S3_SECRET_ACCESS_KEY") # Fixed: No trailing comma
    REGION_NAME = os.getenv("AWS_REGION", "us-east-1") # Defaults to us-east-1 if not set

    # 2. SYSTEM HEALTH CHECK (Fail Fast)
    # Check if any critical credentials are missing before starting the expensive API calls.
    required_vars = {
        "API_KEY": ALPHA_VANTAGE_API_KEY,
        "S3_BUCKET": S3_BUCKET_DESTINATION,
        "AWS_KEY": AWS_ACCESS_KEY_ID,
        "AWS_SECRET": AWS_SECRET_ACCESS_KEY
    }
    
    for var_name, value in required_vars.items():
        if not value:
            logger.error(f"❌ CRITICAL ERROR: {var_name} is missing from the environment.")
            exit(1)

    # 3. INITIALIZE EXTRACTOR
    # This sets up our S3 connection and API base URL.
    extractor = StockExtractor(
        api_key=ALPHA_VANTAGE_API_KEY,
        aws_access_key=AWS_ACCESS_KEY_ID,
        aws_secret_key=AWS_SECRET_ACCESS_KEY,
        region=REGION_NAME
    )

    # 4. EXECUTE PIPELINE
    # We wrap the logic in a try-except block to handle errors gracefully.

    #Expand additional tickers below
    tickers = ["AAPL", "MSFT", "GOOGL"]

    # iterate over each ticker in the array
    for ticker in tickers:
        try:
            logger.info(f"🚀 Starting ingestion pipeline for {ticker}...")
            print(f"🚀 Starting ingestion pipeline for {ticker}...") #for development

            # Step A: Fetch Data
            raw_json = extractor.fetch_past_7_days_daily_data(symbol=ticker)
            
            # Step B: Validate & Clean (7-day window)
            # This converts messy API JSON into a list of clean Pydantic objects.
            validated_records = extractor.validate_and_process_7_days(symbol=ticker, raw_data=raw_json)
            
            # Step C: Upload to Bronze Layer (S3)
            # This converts the list to Parquet and ships it to AWS.
            extractor.upload_7_days_to_s3(records=validated_records, s3_bucket=S3_BUCKET_DESTINATION)

            logger.info(f"✅ Pipeline complete. Data for {ticker} is now in S3.")
            print(f"✅ Pipeline complete. Data for {ticker} is now in S3.") #for development

            if ticker != tickers[-1]:
                # pause for 15 sec before processing the next ticker to prevent throttle and API pull failure
                time.sleep(15)
            else:
                print("✅ Finish processing all tickers")

        except Exception as e:
            logger.error(f"💥 Pipeline failed: {str(e)}")
            print(f"💥 Pipeline failed: {str(e)}") #for development
            exit(1)