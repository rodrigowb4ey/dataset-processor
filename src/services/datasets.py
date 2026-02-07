import uuid
from datetime import UTC, datetime
from typing import cast

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.errors import DatabaseError, NotFoundError, QueueError
from src.core.schemas import DatasetStatus, JobState
from src.db.models import Dataset, Job, Report
from src.worker.celery_app import celery_app

ACTIVE_JOB_STATES = (
    JobState.queued.value,
    JobState.started.value,
    JobState.retrying.value,
)


async def get_dataset_by_checksum(
    session: AsyncSession,
    checksum_sha256: str,
) -> Dataset | None:
    try:
        return cast(
            "Dataset | None",
            await session.scalar(select(Dataset).where(Dataset.checksum_sha256 == checksum_sha256)),
        )
    except SQLAlchemyError as exc:
        raise DatabaseError() from exc


async def create_dataset(session: AsyncSession, dataset: Dataset) -> Dataset:
    try:
        session.add(dataset)
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        existing = await get_dataset_by_checksum(session, dataset.checksum_sha256)
        if existing:
            return existing
        raise DatabaseError("Dataset already exists or violates constraints.") from exc
    except SQLAlchemyError as exc:
        await session.rollback()
        raise DatabaseError() from exc

    await session.refresh(dataset)
    return dataset


async def get_dataset_summary(
    session: AsyncSession,
    dataset_id: uuid.UUID,
) -> tuple[Dataset, uuid.UUID | None, bool]:
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
        raise DatabaseError() from exc

    return dataset, latest_job_id, bool(report_id)


async def get_dataset_report(session: AsyncSession, dataset_id: uuid.UUID) -> Report:
    try:
        report = cast(
            "Report | None",
            await session.scalar(select(Report).where(Report.dataset_id == dataset_id).limit(1)),
        )
        if report is None:
            raise NotFoundError("Report not found.")
    except SQLAlchemyError as exc:
        raise DatabaseError() from exc

    return report


async def _get_dataset_or_not_found(session: AsyncSession, dataset_id: uuid.UUID) -> Dataset:
    try:
        dataset = cast(
            "Dataset | None", await session.scalar(select(Dataset).where(Dataset.id == dataset_id))
        )
    except SQLAlchemyError as exc:
        raise DatabaseError() from exc

    if dataset is None:
        raise NotFoundError("Dataset not found.")

    return dataset


async def _get_latest_active_job(session: AsyncSession, dataset_id: uuid.UUID) -> Job | None:
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
        raise DatabaseError() from exc


async def _get_latest_job(session: AsyncSession, dataset_id: uuid.UUID) -> Job | None:
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
        raise DatabaseError() from exc


async def _dataset_has_report(session: AsyncSession, dataset_id: uuid.UUID) -> bool:
    try:
        report_id = await session.scalar(
            select(Report.id).where(Report.dataset_id == dataset_id).limit(1)
        )
    except SQLAlchemyError as exc:
        raise DatabaseError() from exc

    return report_id is not None


async def _commit_with_database_error(session: AsyncSession) -> None:
    try:
        await session.commit()
    except SQLAlchemyError as exc:
        await session.rollback()
        raise DatabaseError() from exc


async def _refresh_job_with_database_error(session: AsyncSession, job: Job) -> None:
    try:
        await session.refresh(job)
    except SQLAlchemyError as exc:
        await session.rollback()
        raise DatabaseError() from exc


async def _create_synthetic_success_job(session: AsyncSession, dataset_id: uuid.UUID) -> Job:
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
    return synthetic_job


async def _create_queued_job_or_existing_active(
    session: AsyncSession,
    dataset_id: uuid.UUID,
) -> tuple[Job, bool]:
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
            return existing_active_job, False
        raise DatabaseError() from exc
    except SQLAlchemyError as exc:
        await session.rollback()
        raise DatabaseError() from exc

    await _refresh_job_with_database_error(session, job)
    return job, True


async def _enqueue_job_task(session: AsyncSession, dataset_id: uuid.UUID, job: Job) -> Job:
    try:
        async_result = celery_app.send_task(
            "process_dataset",
            [str(dataset_id), str(job.id)],
        )
    except Exception as exc:
        job.state = JobState.failure.value
        job.error = "Failed to enqueue task."
        await _commit_with_database_error(session)
        raise QueueError() from exc

    job.celery_task_id = async_result.id
    await _commit_with_database_error(session)
    return job


async def enqueue_dataset_processing(
    session: AsyncSession,
    dataset_id: uuid.UUID,
) -> Job:
    dataset = await _get_dataset_or_not_found(session, dataset_id)

    active_job = await _get_latest_active_job(session, dataset_id)
    if active_job is not None:
        return active_job

    latest_job = await _get_latest_job(session, dataset_id)
    report_exists = await _dataset_has_report(session, dataset_id)
    if dataset.status == DatasetStatus.done.value and report_exists:
        if latest_job is not None:
            return latest_job
        return await _create_synthetic_success_job(session, dataset.id)

    job, created = await _create_queued_job_or_existing_active(session, dataset.id)
    if not created:
        return job

    return await _enqueue_job_task(session, dataset.id, job)
