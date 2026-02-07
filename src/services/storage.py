"""S3-compatible object storage service helpers."""

import json
import math
from io import BytesIO
from typing import BinaryIO

from minio import Minio

from src.core.config import settings
from src.core.logging import get_logger

MIN_PART_SIZE = 5 * 1024 * 1024
MAX_PART_SIZE = 64 * 1024 * 1024
TARGET_PARTS = 10
logger = get_logger(__name__)


def build_minio_client() -> Minio:
    """Build a MinIO client instance from configured settings."""
    endpoint = f"{settings.s3_host}:{settings.s3_port}"
    secure = settings.s3_scheme.lower() == "https"
    return Minio(
        endpoint=endpoint,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        secure=secure,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    """Create bucket when it does not already exist."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("storage.bucket.created", bucket=bucket)


def upload_object(
    client: Minio,
    bucket: str,
    object_key: str,
    data: BinaryIO,
    length: int,
    content_type: str,
) -> str | None:
    """Upload a stream as an object and return storage ETag."""
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
    logger.info(
        "storage.object.uploaded",
        bucket=bucket,
        object_key=object_key,
        size_bytes=length,
        content_type=content_type,
    )
    return result.etag


def download_object(client: Minio, bucket: str, object_key: str) -> bytes:
    """Download object payload bytes and release the underlying connection."""
    response = client.get_object(bucket_name=bucket, object_name=object_key)
    try:
        payload = response.read()
        logger.info(
            "storage.object.downloaded",
            bucket=bucket,
            object_key=object_key,
            size_bytes=len(payload),
        )
        return payload
    finally:
        response.close()
        response.release_conn()


def upload_json_object(
    client: Minio,
    bucket: str,
    object_key: str,
    payload: dict[str, object],
) -> str | None:
    """Serialize a JSON payload and upload it as an object."""
    body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
    return upload_object(
        client=client,
        bucket=bucket,
        object_key=object_key,
        data=BytesIO(body),
        length=len(body),
        content_type="application/json",
    )
