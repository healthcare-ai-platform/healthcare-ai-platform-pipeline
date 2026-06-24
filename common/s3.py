import os

import boto3

S3_BUCKET = os.getenv("S3_BUCKET", "healthai-raw")


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        # Points to local MinIO in dev; remove for real AWS
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
    )


def list_s3_files(prefix: str = "") -> list[str]:
    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def download_file(key: str) -> bytes:
    client = get_s3_client()
    response = client.get_object(Bucket=S3_BUCKET, Key=key)
    return response["Body"].read()


def upload_bytes(key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    client = get_s3_client()
    client.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType=content_type)
