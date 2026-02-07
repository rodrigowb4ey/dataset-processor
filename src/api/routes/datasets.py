import asyncio
import uuid
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, Response, UploadFile, status
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.core.errors import (
    InvalidRequestError,
    MissingFilenameError,
    StorageError,
    UnsupportedMediaTypeError,
)
from src.core.schemas import (
    DatasetPublic,
    DatasetSchema,
    DatasetStatus,
    DatasetUploadPublic,
    JobEnqueuePublic,
    JobState,
)
from src.db.models import Dataset, Job
from src.db.session import get_async_session
from src.services import datasets as datasets_service
from src.services.storage import build_minio_client, download_object, ensure_bucket, upload_object
from src.utils.checksum import compute_sha256_and_size

router = APIRouter(prefix="/datasets", tags=["datasets"])

ALLOWED_CONTENT_TYPES = {"text/csv", "application/json"}


def _job_response(job: Job) -> JobEnqueuePublic:
    return JobEnqueuePublic(
        job_id=job.id,
        dataset_id=job.dataset_id,
        state=JobState(job.state),
        progress=job.progress,
    )


@router.post("", response_model=DatasetUploadPublic, status_code=status.HTTP_201_CREATED)
async def upload_dataset(
    session: Annotated[AsyncSession, Depends(get_async_session)],
    name: Annotated[str, Form(...)],
    file: Annotated[UploadFile, File(...)],
) -> Dataset:
    try:
        dataset_input = DatasetSchema(name=name)
    except ValidationError as exc:
        raise InvalidRequestError(detail=exc.errors()) from exc

    content_type = file.content_type or ""
    if content_type not in ALLOWED_CONTENT_TYPES:
        raise UnsupportedMediaTypeError()

    if not file.filename:
        raise MissingFilenameError()

    original_filename = Path(file.filename).name
    checksum_sha256, size_bytes = await compute_sha256_and_size(file)

    existing = await datasets_service.get_dataset_by_checksum(session, checksum_sha256)
    if existing:
        return existing

    dataset_id = uuid.uuid4()
    upload_key = f"datasets/{dataset_id}/source/{original_filename}"
    upload_bucket = settings.s3_bucket_uploads

    client = build_minio_client()
    try:
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
    except Exception as exc:
        raise StorageError("Failed to upload dataset to storage.") from exc

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
    return await datasets_service.create_dataset(session, dataset)


@router.get("/{dataset_id}", response_model=DatasetPublic)
async def get_dataset(
    session: Annotated[AsyncSession, Depends(get_async_session)],
    dataset_id: uuid.UUID,
) -> DatasetPublic:
    dataset, latest_job_id, report_available = await datasets_service.get_dataset_summary(
        session, dataset_id
    )

    return DatasetPublic(
        id=dataset.id,
        name=dataset.name,
        status=DatasetStatus(dataset.status),
        row_count=dataset.row_count,
        latest_job_id=latest_job_id,
        report_available=report_available,
        error=dataset.error,
    )


@router.get("/{dataset_id}/report")
async def get_dataset_report(
    session: Annotated[AsyncSession, Depends(get_async_session)],
    dataset_id: uuid.UUID,
) -> Response:
    report = await datasets_service.get_dataset_report(session, dataset_id)

    client = build_minio_client()
    try:
        payload = await asyncio.to_thread(
            download_object,
            client,
            report.report_bucket,
            report.report_key,
        )
    except Exception as exc:
        raise StorageError("Failed to download report from storage.") from exc

    return Response(content=payload, media_type="application/json")


@router.post(
    "/{dataset_id}/process",
    response_model=JobEnqueuePublic,
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_dataset_processing(
    session: Annotated[AsyncSession, Depends(get_async_session)],
    dataset_id: uuid.UUID,
) -> JobEnqueuePublic:
    job = await datasets_service.enqueue_dataset_processing(session, dataset_id)
    return _job_response(job)
