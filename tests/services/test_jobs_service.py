import re
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from src.core.config import settings
from src.core.errors import DatabaseError, NotFoundError
from src.db.models import Dataset, Job
from src.services import jobs as jobs_service


def build_dataset(*, checksum: str = "checksum") -> Dataset:
    dataset_id = uuid4()
    return Dataset(
        id=dataset_id,
        name="Test dataset",
        original_filename="data.csv",
        content_type="text/csv",
        status="uploaded",
        checksum_sha256=checksum,
        size_bytes=12,
        upload_bucket=settings.s3_bucket_uploads,
        upload_key=f"datasets/{dataset_id}/source/data.csv",
        upload_etag="etag",
    )


def build_job(dataset_id: UUID, *, state: str, progress: int, queued_at: datetime) -> Job:
    return Job(
        dataset_id=dataset_id,
        state=state,
        progress=progress,
        queued_at=queued_at,
    )


async def test_list_jobs_returns_descending_order(async_engine: AsyncEngine) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    dataset = build_dataset(checksum="jobs-order")
    now = datetime.now(UTC)

    async with sessionmaker() as session:
        session.add(dataset)
        await session.commit()

        older_job = build_job(dataset.id, state="success", progress=100, queued_at=now)
        newer_job = build_job(
            dataset.id,
            state="started",
            progress=40,
            queued_at=now + timedelta(seconds=10),
        )
        session.add_all([older_job, newer_job])
        await session.commit()

        result = await jobs_service.list_jobs(session)

    assert [job.id for job in result] == [newer_job.id, older_job.id]


async def test_list_jobs_returns_empty_list(async_engine: AsyncEngine) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)

    async with sessionmaker() as session:
        result = await jobs_service.list_jobs(session)

    assert result == []


async def test_list_jobs_database_error(
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)

    async def failing_scalars(*_args: object, **_kwargs: object) -> None:
        raise SQLAlchemyError("boom")

    monkeypatch.setattr(AsyncSession, "scalars", failing_scalars)

    async with sessionmaker() as session:
        with pytest.raises(DatabaseError):
            await jobs_service.list_jobs(session)


async def test_get_job_returns_job(async_engine: AsyncEngine) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    dataset = build_dataset(checksum="jobs-get")

    async with sessionmaker() as session:
        session.add(dataset)
        await session.commit()

        job = Job(dataset_id=dataset.id, state="started", progress=40)
        session.add(job)
        await session.commit()

        result = await jobs_service.get_job(session, job.id)

    assert result.id == job.id
    assert result.dataset_id == dataset.id
    assert result.state == "started"
    assert result.progress == 40


async def test_get_job_not_found(async_engine: AsyncEngine) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)

    async with sessionmaker() as session:
        with pytest.raises(NotFoundError, match=re.escape("Job not found.")):
            await jobs_service.get_job(session, uuid4())


async def test_get_job_database_error(
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)

    async def failing_scalar(*_args: object, **_kwargs: object) -> None:
        raise SQLAlchemyError("boom")

    monkeypatch.setattr(AsyncSession, "scalar", failing_scalar)

    async with sessionmaker() as session:
        with pytest.raises(DatabaseError):
            await jobs_service.get_job(session, uuid4())
