import uuid
from datetime import UTC, datetime
from typing import Any

from celery.exceptions import MaxRetriesExceededError
from minio.error import S3Error
from sqlalchemy import select
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from src.core.config import settings
from src.core.schemas import DatasetStatus, JobState
from src.db.models import Dataset, Job, Report
from src.db.session import SessionLocal
from src.processing import compute_anomalies, compute_stats, parse_rows
from src.processing.parsers import InvalidDatasetFormatError
from src.services.storage import (
    build_minio_client,
    download_object,
    ensure_bucket,
    upload_json_object,
)

from .celery_app import celery_app

RETRYABLE_EXCEPTIONS = (OperationalError, OSError, S3Error)


@celery_app.task
def ping() -> str:
    return "pong"


def _set_job_progress(
    *,
    job_id: uuid.UUID,
    progress: int,
    state: str | None = None,
    error: str | None = None,
    started_at: bool = False,
    finished_at: bool = False,
) -> None:
    with SessionLocal() as session:
        job = session.get(Job, job_id)
        if not job:
            return
        if state is not None:
            job.state = state
        if error is not None:
            job.error = error
        job.progress = progress
        if started_at and job.started_at is None:
            job.started_at = datetime.now(UTC)
        if finished_at:
            job.finished_at = datetime.now(UTC)
        session.commit()


def _mark_dataset_state(
    *,
    dataset_id: uuid.UUID,
    status: str,
    row_count: int | None = None,
    error: str | None = None,
    processed: bool = False,
) -> None:
    with SessionLocal() as session:
        dataset = session.get(Dataset, dataset_id)
        if not dataset:
            return
        dataset.status = status
        dataset.row_count = row_count
        dataset.error = error
        if processed:
            dataset.processed_at = datetime.now(UTC)
        session.commit()


def _upsert_report(dataset_id: uuid.UUID, report_etag: str | None) -> None:
    report_key = f"datasets/{dataset_id}/report/report.json"
    with SessionLocal() as session:
        report = session.scalar(select(Report).where(Report.dataset_id == dataset_id).limit(1))
        if report is None:
            report = Report(
                dataset_id=dataset_id,
                report_bucket=settings.s3_bucket_reports,
                report_key=report_key,
                report_etag=report_etag,
            )
            session.add(report)
        else:
            report.report_bucket = settings.s3_bucket_reports
            report.report_key = report_key
            report.report_etag = report_etag
        session.commit()


def _get_dataset_or_fail(dataset_id: uuid.UUID) -> Dataset:
    with SessionLocal() as session:
        dataset = session.get(Dataset, dataset_id)
        if dataset is None:
            raise InvalidDatasetFormatError("Dataset not found.")
        return dataset


@celery_app.task(name="process_dataset", bind=True, max_retries=3)
def process_dataset(self: Any, dataset_id: str, job_id: str) -> str:
    dataset_uuid = uuid.UUID(dataset_id)
    job_uuid = uuid.UUID(job_id)

    try:
        dataset = _get_dataset_or_fail(dataset_uuid)
        _set_job_progress(
            job_id=job_uuid,
            progress=5,
            state=JobState.started.value,
            started_at=True,
            error=None,
        )
        _mark_dataset_state(
            dataset_id=dataset_uuid, status=DatasetStatus.processing.value, error=None
        )

        minio = build_minio_client()
        payload = download_object(minio, dataset.upload_bucket, dataset.upload_key)
        rows = parse_rows(dataset.content_type, payload)
        _set_job_progress(job_id=job_uuid, progress=25)

        stats = compute_stats(rows)
        _set_job_progress(job_id=job_uuid, progress=60)

        anomalies = compute_anomalies(rows)
        _set_job_progress(job_id=job_uuid, progress=85)

        report_payload: dict[str, Any] = {
            "dataset_id": str(dataset_uuid),
            "generated_at": datetime.now(UTC).isoformat(),
            "row_count": stats["row_count"],
            "null_counts": stats["null_counts"],
            "numeric": stats["numeric"],
            "anomalies": anomalies,
        }
        report_key = f"datasets/{dataset_uuid}/report/report.json"
        ensure_bucket(minio, settings.s3_bucket_reports)
        report_etag = upload_json_object(
            minio,
            settings.s3_bucket_reports,
            report_key,
            report_payload,
        )
        _upsert_report(dataset_uuid, report_etag)

        _mark_dataset_state(
            dataset_id=dataset_uuid,
            status=DatasetStatus.done.value,
            row_count=stats["row_count"],
            processed=True,
            error=None,
        )
        _set_job_progress(
            job_id=job_uuid,
            progress=100,
            state=JobState.success.value,
            finished_at=True,
            error=None,
        )
        return f"success:{dataset_id}:{job_id}"
    except InvalidDatasetFormatError as exc:
        _mark_dataset_state(
            dataset_id=dataset_uuid, status=DatasetStatus.failed.value, error=str(exc)
        )
        _set_job_progress(
            job_id=job_uuid,
            progress=100,
            state=JobState.failure.value,
            finished_at=True,
            error=str(exc),
        )
        return f"failed:{dataset_id}:{job_id}"
    except RETRYABLE_EXCEPTIONS as exc:
        _set_job_progress(
            job_id=job_uuid,
            progress=5,
            state=JobState.retrying.value,
            error=str(exc),
        )
        try:
            raise self.retry(exc=exc, countdown=min(60, 2 ** (self.request.retries + 1)))
        except MaxRetriesExceededError:
            _mark_dataset_state(
                dataset_id=dataset_uuid, status=DatasetStatus.failed.value, error=str(exc)
            )
            _set_job_progress(
                job_id=job_uuid,
                progress=100,
                state=JobState.failure.value,
                finished_at=True,
                error=str(exc),
            )
            return f"failed:{dataset_id}:{job_id}"
    except SQLAlchemyError as exc:
        _set_job_progress(
            job_id=job_uuid,
            progress=100,
            state=JobState.failure.value,
            finished_at=True,
            error=str(exc),
        )
        _mark_dataset_state(
            dataset_id=dataset_uuid, status=DatasetStatus.failed.value, error=str(exc)
        )
        return f"failed:{dataset_id}:{job_id}"
    except Exception as exc:
        _set_job_progress(
            job_id=job_uuid,
            progress=100,
            state=JobState.failure.value,
            finished_at=True,
            error=str(exc),
        )
        _mark_dataset_state(
            dataset_id=dataset_uuid, status=DatasetStatus.failed.value, error=str(exc)
        )
        raise
