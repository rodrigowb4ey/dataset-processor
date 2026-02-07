import json
import math
from io import BytesIO
from typing import BinaryIO

from minio import Minio

from src.core.config import settings

MIN_PART_SIZE = 5 * 1024 * 1024
MAX_PART_SIZE = 64 * 1024 * 1024
TARGET_PARTS = 10


def build_minio_client() -> Minio:
    endpoint = f"{settings.s3_host}:{settings.s3_port}"
    secure = settings.s3_scheme.lower() == "https"
    return Minio(
        endpoint=endpoint,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        secure=secure,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def upload_object(
    client: Minio,
    bucket: str,
    object_key: str,
    data: BinaryIO,
    length: int,
    content_type: str,
) -> str | None:
    target_size = math.ceil(length / TARGET_PARTS) if length > 0 else MIN_PART_SIZE
    part_size = min(max(MIN_PART_SIZE, target_size), MAX_PART_SIZE)
    result = client.put_object(
        bucket_name=bucket,
        object_name=object_key,
        data=data,
        length=length,
        part_size=part_size,
        content_type=content_type,
    )
    return result.etag


def download_object(client: Minio, bucket: str, object_key: str) -> bytes:
    response = client.get_object(bucket_name=bucket, object_name=object_key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def upload_json_object(
    client: Minio,
    bucket: str,
    object_key: str,
    payload: dict[str, object],
) -> str | None:
    body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
    return upload_object(
        client=client,
        bucket=bucket,
        object_key=object_key,
        data=BytesIO(body),
        length=len(body),
        content_type="application/json",
    )
