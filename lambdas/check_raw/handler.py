"""Tell Step Functions whether the raw zone already holds a complete ingest."""

import os

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")


def handler(event, context):
    bucket = os.environ["RAW_BUCKET"]
    key = os.environ["RAW_PREFIX"].strip("/") + "/_SUCCESS"
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return {"ingested": True}
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return {"ingested": False}
        raise
