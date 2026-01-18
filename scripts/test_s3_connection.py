# test_s3.py
import boto3
import os
from dotenv import load_dotenv

load_dotenv("../.env")

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("STOCK_DATA_AWS_S3_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("STOCK_DATA_AWS_S3_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

try:
    # Try to list the bucket contents (checks ListBucket permission)
    response = s3.list_objects_v2(Bucket=os.getenv("STOCK_DATA_AWS_S3_BUCKET_NAME"))
    print("🚀 Connection Successful! Your script can talk to S3.")
except Exception as e:
    print(f"🛑 Connection Failed: {e}")