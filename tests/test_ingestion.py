import pytest
from moto import mock_aws
from scripts.ingest_last7days_stock_data import StockExtractor, DailyStockData


@pytest.fixture
def mock_s3_extractor():
    """Fixture to create a StockExtractor with a mocked S3 environment."""
    with mock_aws():
        extractor = StockExtractor(
            api_key="test_key",
            aws_access_key="testing",
            aws_secret_key="testing",
            region="us-east-1",
        )
        # Create the bucket in the mock environment
        extractor.s3_client.create_bucket(Bucket="test-bucket")
        yield extractor


def test_daily_stock_data_validation():
    """Test Pydantic model validation (Success & Error cases)."""
    # Success case
    data = {
        "1. open": "150.0",
        "2. high": "155.0",
        "3. low": "149.0",
        "4. close": "152.0",
        "5. volume": "1000",
    }
    record = DailyStockData(symbol="AAPL", date="2026-01-01", **data)
    assert record.open_price == 150.0

    # Failure case: Negative price should trigger Pydantic ValueError
    with pytest.raises(ValueError, match="Stock prices must be greater than zero"):
        DailyStockData(symbol="AAPL", date="2026-01-01", **{**data, "1. open": "-10.0"})


def test_fetch_and_upload_flow(mock_s3_extractor, requests_mock):
    """Test the full flow: API fetch -> Validation -> S3 Upload."""
    symbol = "AAPL"
    bucket = "test-bucket"

    # 1. Mock Alpha Vantage Response
    mock_api_data = {
        "Time Series (Daily)": {
            "2026-01-10": {
                "1. open": "100",
                "2. high": "110",
                "3. low": "90",
                "4. close": "105",
                "5. volume": "500",
            }
        }
    }
    requests_mock.get("https://www.alphavantage.co/query", json=mock_api_data)

    # 2. Run extractor methods
    raw_data = mock_s3_extractor.fetch_year_to_date_history(symbol)
    validated = mock_s3_extractor.validate_year_to_date_history(
        symbol=symbol, start_date="2026-01-01", end_date="2026-01-15", raw_data=raw_data
    )
    mock_s3_extractor.upload_year_to_date_history_to_s3(
        records=validated, s3_bucket=bucket
    )

    # 3. Assertions
    assert len(validated) == 1
    # Check if file exists in mocked S3
    objects = mock_s3_extractor.s3_client.list_objects(Bucket=bucket)["Contents"]
    assert any("AAPL/2026_full_historical.parquet" in obj["Key"] for obj in objects)
