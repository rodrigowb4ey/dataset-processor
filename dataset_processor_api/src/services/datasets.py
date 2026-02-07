"""Dataset service layer for database operations and enqueue orchestration."""

import uuid
from datetime import UTC, datetime
from typing import cast

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.errors import DatabaseError, NotFoundError, QueueError
from src.core.logging import get_logger
from src.core.schemas import DatasetStatus, JobState
from src.db.models import Dataset, Job, Report
from src.worker.celery_app import celery_app

ACTIVE_JOB_STATES = (
    JobState.queued.value,
    JobState.started.value,
    JobState.retrying.value,
)
logger = get_logger(__name__)


async def get_dataset_by_checksum(
    session: AsyncSession,
    checksum_sha256: str,
) -> Dataset | None:
    """Return a dataset by checksum, if it exists."""
    try:
        return cast(
            "Dataset | None",
            await session.scalar(select(Dataset).where(Dataset.checksum_sha256 == checksum_sha256)),
        )
    except SQLAlchemyError as exc:
        logger.exception("datasets.get_by_checksum.database_failed", exc_info=exc)
        raise DatabaseError() from exc


async def create_dataset(session: AsyncSession, dataset: Dataset) -> Dataset:
    """Persist a dataset row and keep checksum creation idempotent."""
    try:
        session.add(dataset)
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        existing = await get_dataset_by_checksum(session, dataset.checksum_sha256)
        if existing:
            logger.info(
                "datasets.create.integrity_idempotent_hit",
                dataset_id=str(existing.id),
                checksum_sha256=dataset.checksum_sha256,
            )
            return existing
        logger.exception("datasets.create.integrity_failed", exc_info=exc)
        raise DatabaseError("Dataset already exists or violates constraints.") from exc
    except SQLAlchemyError as exc:
        await session.rollback()
        logger.exception("datasets.create.database_failed", exc_info=exc)
        raise DatabaseError() from exc

    await session.refresh(dataset)
    logger.info("datasets.create.completed", dataset_id=str(dataset.id))
    return dataset


async def get_dataset_summary(
    session: AsyncSession,
    dataset_id: uuid.UUID,
) -> tuple[Dataset, uuid.UUID | None, bool]:
    """Return dataset plus latest job id and report availability."""
    try:
        dataset = cast(
            "Dataset | None",
            await session.scalar(select(Dataset).where(Dataset.id == dataset_id)),
        )
        if not dataset:
            raise NotFoundError("Dataset not found.")

        latest_job_id = await session.scalar(
            select(Job.id)
            .where(Job.dataset_id == dataset_id)
            .order_by(Job.queued_at.desc())
            .limit(1)
        )
        report_id = await session.scalar(
            select(Report.id).where(Report.dataset_id == dataset_id).limit(1)
        )
    except SQLAlchemyError as exc:
        logger.exception("datasets.get_summary.database_failed", exc_info=exc)
        raise DatabaseError() from exc

    return dataset, latest_job_id, bool(report_id)


async def list_dataset_summaries(
    session: AsyncSession,
) -> list[tuple[Dataset, uuid.UUID | None, bool]]:
    """Return all datasets with latest job id and report availability."""
    try:
        datasets = list(
            (await session.scalars(select(Dataset).order_by(Dataset.uploaded_at.desc()))).all()
        )
        if not datasets:
            return []

        dataset_ids = [dataset.id for dataset in datasets]

        jobs = list(
            (
                await session.scalars(
                    select(Job)
                    .where(Job.dataset_id.in_(dataset_ids))
                    .order_by(Job.dataset_id, Job.queued_at.desc())
                )
            ).all()
        )
        latest_job_by_dataset: dict[uuid.UUID, uuid.UUID] = {}
        for job in jobs:
            latest_job_by_dataset.setdefault(job.dataset_id, job.id)

        report_dataset_ids = set(
            (
                await session.scalars(
                    select(Report.dataset_id).where(Report.dataset_id.in_(dataset_ids))
                )
            ).all()
        )
    except SQLAlchemyError as exc:
        logger.exception("datasets.list_summaries.database_failed", exc_info=exc)
        raise DatabaseError() from exc

    return [
        (
            dataset,
            latest_job_by_dataset.get(dataset.id),
            dataset.id in report_dataset_ids,
        )
        for dataset in datasets
    ]


async def get_dataset_report(session: AsyncSession, dataset_id: uuid.UUID) -> Report:
    """Return persisted report metadata for a dataset."""
    try:
        report = cast(
            "Report | None",
            await session.scalar(select(Report).where(Report.dataset_id == dataset_id).limit(1)),
        )
        if report is None:
            raise NotFoundError("Report not found.")
    except SQLAlchemyError as exc:
        logger.exception(
            "datasets.get_report.database_failed", dataset_id=str(dataset_id), exc_info=exc
        )
        raise DatabaseError() from exc

    return report


async def _get_dataset_or_not_found(session: AsyncSession, dataset_id: uuid.UUID) -> Dataset:
    """Return dataset row or raise a not-found domain error."""
    try:
        dataset = cast(
            "Dataset | None", await session.scalar(select(Dataset).where(Dataset.id == dataset_id))
        )
    except SQLAlchemyError as exc:
        logger.exception(
            "datasets.get_dataset_or_not_found.database_failed",
            dataset_id=str(dataset_id),
            exc_info=exc,
        )
        raise DatabaseError() from exc

    if dataset is None:
        raise NotFoundError("Dataset not found.")

    return dataset


async def _get_latest_active_job(session: AsyncSession, dataset_id: uuid.UUID) -> Job | None:
    """Return latest active job for dataset, if one exists."""
    try:
        return cast(
            "Job | None",
            await session.scalar(
                select(Job)
                .where(
                    Job.dataset_id == dataset_id,
                    Job.state.in_(ACTIVE_JOB_STATES),
                )
                .order_by(Job.queued_at.desc())
                .limit(1)
            ),
        )
    except SQLAlchemyError as exc:
        logger.exception(
            "datasets.get_latest_active_job.database_failed",
            dataset_id=str(dataset_id),
            exc_info=exc,
        )
        raise DatabaseError() from exc


async def _get_latest_job(session: AsyncSession, dataset_id: uuid.UUID) -> Job | None:
    """Return latest job for a dataset regardless of state."""
    try:
        return cast(
            "Job | None",
            await session.scalar(
                select(Job)
                .where(Job.dataset_id == dataset_id)
                .order_by(Job.queued_at.desc())
                .limit(1)
            ),
        )
    except SQLAlchemyError as exc:
        logger.exception(
            "datasets.get_latest_job.database_failed",
            dataset_id=str(dataset_id),
            exc_info=exc,
        )
        raise DatabaseError() from exc


async def _dataset_has_report(session: AsyncSession, dataset_id: uuid.UUID) -> bool:
    """Return whether report metadata exists for a dataset."""
    try:
        report_id = await session.scalar(
            select(Report.id).where(Report.dataset_id == dataset_id).limit(1)
        )
    except SQLAlchemyError as exc:
        logger.exception(
            "datasets.dataset_has_report.database_failed",
            dataset_id=str(dataset_id),
            exc_info=exc,
        )
        raise DatabaseError() from exc

    return report_id is not None


async def _commit_with_database_error(session: AsyncSession) -> None:
    """Commit current transaction and normalize database errors."""
    try:
        await session.commit()
    except SQLAlchemyError as exc:
        await session.rollback()
        logger.exception("datasets.commit.database_failed", exc_info=exc)
        raise DatabaseError() from exc


async def _refresh_job_with_database_error(session: AsyncSession, job: Job) -> None:
    """Refresh a job row and normalize database errors."""
    try:
        await session.refresh(job)
    except SQLAlchemyError as exc:
        await session.rollback()
        logger.exception("datasets.refresh_job.database_failed", job_id=str(job.id), exc_info=exc)
        raise DatabaseError() from exc


async def _create_synthetic_success_job(session: AsyncSession, dataset_id: uuid.UUID) -> Job:
    """Create a synthetic success job for already-processed datasets."""
    synthetic_job = Job(
        id=uuid.uuid4(),
        dataset_id=dataset_id,
        state=JobState.success.value,
        progress=100,
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
    )
    session.add(synthetic_job)
    await _commit_with_database_error(session)
    await _refresh_job_with_database_error(session, synthetic_job)
    logger.info(
        "datasets.create_synthetic_success_job.completed",
        dataset_id=str(dataset_id),
        job_id=str(synthetic_job.id),
    )
    return synthetic_job


async def _create_queued_job_or_existing_active(
    session: AsyncSession,
    dataset_id: uuid.UUID,
) -> tuple[Job, bool]:
    """Create a queued job unless a concurrent active job already exists."""
    job = Job(
        id=uuid.uuid4(),
        dataset_id=dataset_id,
        state=JobState.queued.value,
        progress=0,
    )
    session.add(job)

    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        existing_active_job = await _get_latest_active_job(session, dataset_id)
        if existing_active_job is not None:
            logger.info(
                "datasets.create_queued_job.concurrent_active_found",
                dataset_id=str(dataset_id),
                job_id=str(existing_active_job.id),
            )
            return existing_active_job, False
        logger.exception(
            "datasets.create_queued_job.integrity_failed",
            dataset_id=str(dataset_id),
            exc_info=exc,
        )
        raise DatabaseError() from exc
    except SQLAlchemyError as exc:
        await session.rollback()
        logger.exception(
            "datasets.create_queued_job.database_failed",
            dataset_id=str(dataset_id),
            exc_info=exc,
        )
        raise DatabaseError() from exc

    await _refresh_job_with_database_error(session, job)
    logger.info(
        "datasets.create_queued_job.completed",
        dataset_id=str(dataset_id),
        job_id=str(job.id),
    )
    return job, True


async def _enqueue_job_task(session: AsyncSession, dataset_id: uuid.UUID, job: Job) -> Job:
    """Send job task to Celery and persist the Celery task identifier."""
    try:
        async_result = celery_app.send_task(
            "process_dataset",
            [str(dataset_id), str(job.id)],
        )
    except Exception as exc:
        job.state = JobState.failure.value
        job.error = "Failed to enqueue task."
        await _commit_with_database_error(session)
        logger.exception(
            "datasets.enqueue_job_task.queue_failed",
            dataset_id=str(dataset_id),
            job_id=str(job.id),
            exc_info=exc,
        )
        raise QueueError() from exc

    job.celery_task_id = async_result.id
    await _commit_with_database_error(session)
    logger.info(
        "datasets.enqueue_job_task.completed",
        dataset_id=str(dataset_id),
        job_id=str(job.id),
        celery_task_id=async_result.id,
    )
    return job


async def enqueue_dataset_processing(
    session: AsyncSession,
    dataset_id: uuid.UUID,
) -> Job:
    """Resolve idempotent enqueue behavior for dataset processing."""
    dataset = await _get_dataset_or_not_found(session, dataset_id)

    active_job = await _get_latest_active_job(session, dataset_id)
    if active_job is not None:
        logger.info(
            "datasets.enqueue_dataset_processing.active_job_returned",
            dataset_id=str(dataset_id),
            job_id=str(active_job.id),
            job_state=active_job.state,
        )
        return active_job

    latest_job = await _get_latest_job(session, dataset_id)
    report_exists = await _dataset_has_report(session, dataset_id)
    if dataset.status == DatasetStatus.done.value and report_exists:
        if latest_job is not None:
            logger.info(
                "datasets.enqueue_dataset_processing.done_dataset_latest_job_returned",
                dataset_id=str(dataset_id),
                job_id=str(latest_job.id),
            )
            return latest_job
        logger.info(
            "datasets.enqueue_dataset_processing.done_dataset_synthetic_job_created",
            dataset_id=str(dataset_id),
        )
        return await _create_synthetic_success_job(session, dataset_id)

    job, created = await _create_queued_job_or_existing_active(session, dataset_id)
    if not created:
        logger.info(
            "datasets.enqueue_dataset_processing.concurrent_job_returned",
            dataset_id=str(dataset_id),
            job_id=str(job.id),
            job_state=job.state,
        )
        return job

    logger.info(
        "datasets.enqueue_dataset_processing.new_job_created",
        dataset_id=str(dataset_id),
        job_id=str(job.id),
    )
    return await _enqueue_job_task(session, dataset_id, job)
