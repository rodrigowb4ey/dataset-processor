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


async def enqueue_dataset_processing(
    session: AsyncSession,
    dataset_id: uuid.UUID,
) -> Job:
    try:
        dataset = cast(
            "Dataset | None",
            await session.scalar(select(Dataset).where(Dataset.id == dataset_id)),
        )
        if not dataset:
            raise NotFoundError("Dataset not found.")

        active_job = cast(
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
        if active_job:
            return active_job

        latest_job = cast(
            "Job | None",
            await session.scalar(
                select(Job)
                .where(Job.dataset_id == dataset_id)
                .order_by(Job.queued_at.desc())
                .limit(1)
            ),
        )
        report_id = await session.scalar(
            select(Report.id).where(Report.dataset_id == dataset_id).limit(1)
        )
        if dataset.status == DatasetStatus.done.value and report_id:
            if latest_job:
                return latest_job
            synthetic_job = Job(
                id=uuid.uuid4(),
                dataset_id=dataset.id,
                state=JobState.success.value,
                progress=100,
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
            )
            session.add(synthetic_job)
            await session.commit()
            await session.refresh(synthetic_job)
            return synthetic_job

        job = Job(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            state=JobState.queued.value,
            progress=0,
        )
        session.add(job)
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            existing_active_job = cast(
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
            if existing_active_job:
                return existing_active_job
            raise
        await session.refresh(job)

        try:
            async_result = celery_app.send_task(
                "process_dataset",
                [str(dataset.id), str(job.id)],
            )
        except Exception as exc:
            job.state = JobState.failure.value
            job.error = "Failed to enqueue task."
            await session.commit()
            raise QueueError() from exc

        job.celery_task_id = async_result.id
        await session.commit()
    except SQLAlchemyError as exc:
        await session.rollback()
        raise DatabaseError() from exc

    return job
