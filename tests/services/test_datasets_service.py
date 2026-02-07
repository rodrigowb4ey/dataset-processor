from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from src.core.config import settings
from src.core.errors import DatabaseError, NotFoundError, QueueError
from src.db.models import Dataset, Job, Report
from src.services import datasets as datasets_service
from src.worker.celery_app import celery_app


def build_dataset(*, checksum: str = "checksum", status: str = "uploaded") -> Dataset:
    dataset_id = uuid4()
    return Dataset(
        id=dataset_id,
        name="Test dataset",
        original_filename="data.csv",
        content_type="text/csv",
        status=status,
        checksum_sha256=checksum,
        size_bytes=12,
        upload_bucket=settings.s3_bucket_uploads,
        upload_key=f"datasets/{dataset_id}/source/data.csv",
        upload_etag="etag",
    )


def build_report(dataset_id: UUID) -> Report:
    return Report(
        dataset_id=dataset_id,
        report_bucket=settings.s3_bucket_reports,
        report_key=f"datasets/{dataset_id}/report/report.json",
        report_etag="etag",
    )


def build_job(dataset_id: UUID, *, state: str, queued_at: datetime) -> Job:
    return Job(dataset_id=dataset_id, state=state, queued_at=queued_at)


async def test_get_dataset_by_checksum_returns_dataset(async_engine: AsyncEngine) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    dataset = build_dataset(checksum="abc")

    async with sessionmaker() as session:
        session.add(dataset)
        await session.commit()
        result = await datasets_service.get_dataset_by_checksum(session, "abc")

    assert result is not None
    assert result.id == dataset.id


async def test_get_dataset_by_checksum_returns_none(async_engine: AsyncEngine) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)

    async with sessionmaker() as session:
        result = await datasets_service.get_dataset_by_checksum(session, "missing")

    assert result is None


async def test_get_dataset_by_checksum_database_error(
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)

    async def failing_scalar(*_args: object, **_kwargs: object) -> None:
        raise SQLAlchemyError("boom")

    monkeypatch.setattr(AsyncSession, "scalar", failing_scalar)

    async with sessionmaker() as session:
        with pytest.raises(DatabaseError):
            await datasets_service.get_dataset_by_checksum(session, "abc")


async def test_create_dataset_success(async_engine: AsyncEngine) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    dataset = build_dataset(checksum="create")

    async with sessionmaker() as session:
        result = await datasets_service.create_dataset(session, dataset)
        stored = await session.get(Dataset, dataset.id)

    assert result.id == dataset.id
    assert stored is not None


async def test_create_dataset_idempotent_on_integrity_error(
    async_engine: AsyncEngine,
) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)

    async with sessionmaker() as session:
        first = build_dataset(checksum="dup")
        await datasets_service.create_dataset(session, first)

        duplicate = build_dataset(checksum="dup")
        result = await datasets_service.create_dataset(session, duplicate)

        rows = (
            await session.scalars(select(Dataset).where(Dataset.checksum_sha256 == "dup"))
        ).all()

    assert result.id == first.id
    assert len(rows) == 1


async def test_create_dataset_database_error(
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    dataset = build_dataset(checksum="boom")

    async def failing_commit(_self: AsyncSession) -> None:
        raise SQLAlchemyError("boom")

    monkeypatch.setattr(AsyncSession, "commit", failing_commit)

    async with sessionmaker() as session:
        with pytest.raises(DatabaseError):
            await datasets_service.create_dataset(session, dataset)


async def test_get_dataset_summary_returns_latest_job_and_report_flag(
    async_engine: AsyncEngine,
) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    dataset = build_dataset(checksum="summary")
    now = datetime.now(UTC)

    async with sessionmaker() as session:
        session.add(dataset)
        await session.commit()

        job_earlier = build_job(dataset.id, state="success", queued_at=now)
        job_latest = build_job(
            dataset.id,
            state="started",
            queued_at=now + timedelta(seconds=5),
        )
        report = build_report(dataset.id)
        session.add_all([job_earlier, job_latest, report])
        await session.commit()

        (
            summary_dataset,
            latest_job_id,
            report_available,
        ) = await datasets_service.get_dataset_summary(session, dataset.id)

    assert summary_dataset.id == dataset.id
    assert latest_job_id == job_latest.id
    assert report_available is True


async def test_get_dataset_summary_no_jobs_no_report(async_engine: AsyncEngine) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    dataset = build_dataset(checksum="summary-empty")

    async with sessionmaker() as session:
        session.add(dataset)
        await session.commit()

        (
            summary_dataset,
            latest_job_id,
            report_available,
        ) = await datasets_service.get_dataset_summary(session, dataset.id)

    assert summary_dataset.id == dataset.id
    assert latest_job_id is None
    assert report_available is False


async def test_get_dataset_summary_not_found(async_engine: AsyncEngine) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)

    async with sessionmaker() as session:
        with pytest.raises(NotFoundError):
            await datasets_service.get_dataset_summary(session, uuid4())


async def test_get_dataset_summary_database_error(
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)

    async def failing_scalar(*_args: object, **_kwargs: object) -> None:
        raise SQLAlchemyError("boom")

    monkeypatch.setattr(AsyncSession, "scalar", failing_scalar)

    async with sessionmaker() as session:
        with pytest.raises(DatabaseError):
            await datasets_service.get_dataset_summary(session, uuid4())


async def test_enqueue_dataset_processing_creates_job(
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    dataset = build_dataset(checksum="enqueue")
    calls: list[tuple[str, list[str]]] = []

    def fake_send_task(name: str, args: list[str]) -> SimpleNamespace:
        calls.append((name, args))
        return SimpleNamespace(id="task-123")

    monkeypatch.setattr(celery_app, "send_task", fake_send_task)

    async with sessionmaker() as session:
        session.add(dataset)
        await session.commit()

        job = await datasets_service.enqueue_dataset_processing(session, dataset.id)

        jobs = (await session.scalars(select(Job).where(Job.dataset_id == dataset.id))).all()

    assert job.state == "queued"
    assert job.celery_task_id == "task-123"
    assert len(jobs) == 1
    assert calls == [("process_dataset", [str(dataset.id), str(job.id)])]


async def test_enqueue_dataset_processing_returns_active_job(
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    dataset = build_dataset(checksum="active-job")
    now = datetime.now(UTC)

    def fail_send_task(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("send_task should not be called")

    monkeypatch.setattr(celery_app, "send_task", fail_send_task)

    async with sessionmaker() as session:
        session.add(dataset)
        await session.commit()
        active_job = build_job(dataset.id, state="started", queued_at=now)
        session.add(active_job)
        await session.commit()

        result = await datasets_service.enqueue_dataset_processing(session, dataset.id)
        jobs = (await session.scalars(select(Job).where(Job.dataset_id == dataset.id))).all()

    assert result.id == active_job.id
    assert len(jobs) == 1


async def test_enqueue_dataset_processing_done_returns_latest_job(
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    dataset = build_dataset(checksum="done", status="done")
    now = datetime.now(UTC)

    def fail_send_task(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("send_task should not be called")

    monkeypatch.setattr(celery_app, "send_task", fail_send_task)

    async with sessionmaker() as session:
        session.add(dataset)
        await session.commit()
        job_earlier = build_job(dataset.id, state="success", queued_at=now)
        job_latest = build_job(
            dataset.id,
            state="success",
            queued_at=now + timedelta(seconds=5),
        )
        report = build_report(dataset.id)
        session.add_all([job_earlier, job_latest, report])
        await session.commit()

        result = await datasets_service.enqueue_dataset_processing(session, dataset.id)
        jobs = (await session.scalars(select(Job).where(Job.dataset_id == dataset.id))).all()

    assert result.id == job_latest.id
    assert len(jobs) == 2


async def test_enqueue_dataset_processing_done_with_report_but_no_jobs(
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    dataset = build_dataset(checksum="done-no-jobs", status="done")

    def fail_send_task(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("send_task should not be called")

    monkeypatch.setattr(celery_app, "send_task", fail_send_task)

    async with sessionmaker() as session:
        session.add(dataset)
        await session.commit()
        report = build_report(dataset.id)
        session.add(report)
        await session.commit()

        result = await datasets_service.enqueue_dataset_processing(session, dataset.id)

        jobs = (await session.scalars(select(Job).where(Job.dataset_id == dataset.id))).all()

    assert result.state == "success"
    assert result.progress == 100
    assert result.started_at is not None
    assert result.finished_at is not None
    assert len(jobs) == 1


async def test_enqueue_dataset_processing_enqueue_failure_marks_job_failed(
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    dataset = build_dataset(checksum="enqueue-fail")

    def fail_send_task(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(celery_app, "send_task", fail_send_task)

    async with sessionmaker() as session:
        session.add(dataset)
        await session.commit()

        with pytest.raises(QueueError):
            await datasets_service.enqueue_dataset_processing(session, dataset.id)

        jobs = (await session.scalars(select(Job).where(Job.dataset_id == dataset.id))).all()

    assert len(jobs) == 1
    job = jobs[0]
    assert job.state == "failure"
    assert job.error == "Failed to enqueue task."


async def test_enqueue_dataset_processing_not_found(async_engine: AsyncEngine) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)

    async with sessionmaker() as session:
        with pytest.raises(NotFoundError):
            await datasets_service.enqueue_dataset_processing(session, uuid4())


async def test_enqueue_dataset_processing_database_error(
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    dataset = build_dataset(checksum="db-error")

    async with sessionmaker() as session:
        session.add(dataset)
        await session.commit()

    async def failing_commit(_self: AsyncSession) -> None:
        raise SQLAlchemyError("boom")

    monkeypatch.setattr(AsyncSession, "commit", failing_commit)

    async with sessionmaker() as session:
        with pytest.raises(DatabaseError):
            await datasets_service.enqueue_dataset_processing(session, dataset.id)

    async with sessionmaker() as session:
        jobs = (await session.scalars(select(Job).where(Job.dataset_id == dataset.id))).all()

    assert len(jobs) == 0
