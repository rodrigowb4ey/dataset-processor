import asyncio
import uuid
from pathlib import Path
from typing import Annotated, cast

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.core.schemas import DatasetSchema, DatasetStatus, DatasetUploadPublic
from src.db.models import Dataset
from src.db.session import get_async_session
from src.services.storage import build_minio_client, ensure_bucket, upload_object
from src.utils.checksum import compute_sha256_and_size

router = APIRouter(prefix="/datasets", tags=["datasets"])

ALLOWED_CONTENT_TYPES = {"text/csv", "application/json"}


@router.post("", response_model=DatasetUploadPublic, status_code=status.HTTP_201_CREATED)
async def upload_dataset(
    session: Annotated[AsyncSession, Depends(get_async_session)],
    name: Annotated[str, Form(...)],
    file: Annotated[UploadFile, File(...)],
) -> Dataset:
    try:
        dataset_input = DatasetSchema(name=name)
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=exc.errors(),
        ) from exc

    content_type = file.content_type or ""
    if content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="Unsupported content type",
        )

    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="filename is required",
        )

    original_filename = Path(file.filename).name
    checksum_sha256, size_bytes = await compute_sha256_and_size(file)

    checksum_query = select(Dataset).where(Dataset.checksum_sha256 == checksum_sha256)
    existing = cast("Dataset | None", await session.scalar(checksum_query))
    if existing:
        return existing

    dataset_id = uuid.uuid4()
    upload_key = f"datasets/{dataset_id}/source/{original_filename}"
    upload_bucket = settings.s3_bucket_uploads

    client = build_minio_client()
    await asyncio.to_thread(ensure_bucket, client, upload_bucket)
    upload_etag = await asyncio.to_thread(
        upload_object,
        client,
        upload_bucket,
        upload_key,
        file.file,
        size_bytes,
        content_type,
    )

    dataset = Dataset(
        id=dataset_id,
        name=dataset_input.name,
        original_filename=original_filename,
        content_type=content_type,
        status=DatasetStatus.uploaded.value,
        checksum_sha256=checksum_sha256,
        size_bytes=size_bytes,
        upload_bucket=upload_bucket,
        upload_key=upload_key,
        upload_etag=upload_etag,
    )
    session.add(dataset)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        existing = cast("Dataset | None", await session.scalar(checksum_query))
        if existing:
            return existing
        raise

    await session.refresh(dataset)
    return dataset
