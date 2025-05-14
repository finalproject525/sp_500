import boto3
from io import StringIO
import os
import pandas as pd
import json
from datetime import datetime
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from io import BytesIO

class S3Uploader:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION')
        )

    def upload_dataframe_as_csv(self, df: pd.DataFrame, s3_key: str):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=True)
        response = self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )
        print(f"✅ Upload successful: s3://{self.bucket_name}/{s3_key}")
        return response


    def upload_json(self, data: list[dict], prefix: str = "kafka"):
        try:
            now = datetime.utcnow()
            timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
            date_path = now.strftime("%Y/%m/%d/%H")

            # Full key with date structure
            key = f"{prefix}/{date_path}/data_{timestamp}.json"
            content = json.dumps(data, indent=2)

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=content
            )
            print(f"✅ JSON upload successful: s3://{self.bucket_name}/{key}")
        except (BotoCoreError, ClientError) as e:
            print(f"❌ JSON upload failed: {e}")
            raise

    def upload_dataframe_as_parquet(self, df: pd.DataFrame, prefix: str = "kafka"):
        try:
            now = datetime.utcnow()
            timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
            date_path = now.strftime("%Y/%m/%d/%H")

            key = f"{prefix}/{date_path}/data_{timestamp}.parquet"

            buffer = BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer.getvalue()
            )

            print(f"✅ Parquet upload successful: s3://{self.bucket_name}/{key}")
        except (BotoCoreError, ClientError) as e:
            print(f"❌ Parquet upload failed: {e}")
            raise