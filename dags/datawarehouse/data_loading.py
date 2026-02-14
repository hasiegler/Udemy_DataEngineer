import json
import os
from datetime import date
import logging
import boto3

logger = logging.getLogger(__name__)

def load_data():

    bucket_name = os.environ["S3_BUCKET_NAME"]
    s3_key = f"data/YT_data_{date.today()}.json"
    s3_client = boto3.client("s3")

    try:
        logger.info(f"Processing file: s3://{bucket_name}/{s3_key}")

        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        data = json.loads(response["Body"].read().decode("utf-8"))

        return data
    
    except s3_client.exceptions.NoSuchKey:
        logger.error(f"File not found in S3: s3://{bucket_name}/{s3_key}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in S3 file: s3://{bucket_name}/{s3_key}")
        raise