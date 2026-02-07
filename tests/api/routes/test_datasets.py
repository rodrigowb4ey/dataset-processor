import hashlib
import json
from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from minio import Minio
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from src.core.config import settings
from src.db.models import Dataset, Job, Report
from src.worker.celery_app import celery_app


def _upload_report_object(
    minio_client: Minio, dataset_id: UUID, payload: dict[str, object]
) -> None:
    if not minio_client.bucket_exists(settings.s3_bucket_reports):
        minio_client.make_bucket(settings.s3_bucket_reports)

    object_key = f"datasets/{dataset_id}/report/report.json"
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    minio_client.put_object(
        bucket_name=settings.s3_bucket_reports,
        object_name=object_key,
        data=BytesIO(body),
        length=len(body),
        content_type="application/json",
    )


async def test_upload_csv_success(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    minio_client: Minio,
) -> None:
    response = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )

    assert response.status_code == 201
    payload = response.json()

    assert payload["name"] == dataset_name
    assert payload["status"] == "uploaded"
    assert payload["checksum_sha256"] == hashlib.sha256(sample_csv_bytes).hexdigest()
    assert payload["size_bytes"] == len(sample_csv_bytes)

    dataset_id = payload["id"]
    object_key = f"datasets/{dataset_id}/source/data.csv"
    result = minio_client.stat_object(settings.s3_bucket_uploads, object_key)
    assert result.size == len(sample_csv_bytes)


async def test_upload_json_success(
    client: AsyncClient,
    dataset_name: str,
    sample_json_bytes: bytes,
) -> None:
    response = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.json", sample_json_bytes, "application/json")},
    )

    assert response.status_code == 201
    payload = response.json()

    assert payload["name"] == dataset_name
    assert payload["status"] == "uploaded"
    assert payload["checksum_sha256"] == hashlib.sha256(sample_json_bytes).hexdigest()
    assert payload["size_bytes"] == len(sample_json_bytes)


async def test_upload_idempotent_same_checksum(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
) -> None:
    first = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    second = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )

    assert first.status_code == 201
    assert second.status_code == 201

    first_payload = first.json()
    second_payload = second.json()

    assert first_payload["id"] == second_payload["id"]
    assert first_payload["checksum_sha256"] == second_payload["checksum_sha256"]
    assert first_payload["size_bytes"] == second_payload["size_bytes"]


async def test_upload_missing_name_returns_422(
    client: AsyncClient, sample_csv_bytes: bytes
) -> None:
    response = await client.post(
        "/datasets",
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )

    assert response.status_code == 422


async def test_upload_blank_name_returns_422(client: AsyncClient, sample_csv_bytes: bytes) -> None:
    response = await client.post(
        "/datasets",
        data={"name": "   "},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )

    assert response.status_code == 422
    assert isinstance(response.json().get("detail"), list)


async def test_upload_missing_file_returns_422(client: AsyncClient, dataset_name: str) -> None:
    response = await client.post(
        "/datasets",
        data={"name": dataset_name},
    )

    assert response.status_code == 422


async def test_upload_missing_filename_returns_422(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
) -> None:
    response = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("", sample_csv_bytes, "text/csv")},
    )

    assert response.status_code == 422
    assert isinstance(response.json().get("detail"), list)


async def test_upload_unsupported_media_type_returns_415(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
) -> None:
    response = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.txt", sample_csv_bytes, "text/plain")},
    )

    assert response.status_code == 415
    assert response.json()["detail"] == "Unsupported content type."


async def test_upload_storage_error_returns_503(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.api.routes import datasets as datasets_module

    def raise_error(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(datasets_module, "upload_object", raise_error)

    response = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )

    assert response.status_code == 503
    assert response.json()["detail"] == "Failed to upload dataset to storage."


async def test_upload_database_error_returns_503(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failing_commit(_self: AsyncSession) -> None:
        raise SQLAlchemyError("boom")

    monkeypatch.setattr(AsyncSession, "commit", failing_commit)

    response = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )

    assert response.status_code == 503
    assert response.json()["detail"] == "Database error."


async def test_get_dataset_success_defaults(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
) -> None:
    upload = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )

    dataset_id = upload.json()["id"]
    response = await client.get(f"/datasets/{dataset_id}")

    assert response.status_code == 200
    payload = response.json()

    assert payload["id"] == dataset_id
    assert payload["name"] == dataset_name
    assert payload["status"] == "uploaded"
    assert payload["row_count"] is None
    assert payload["latest_job_id"] is None
    assert payload["report_available"] is False
    assert payload["error"] is None


async def test_get_dataset_not_found_returns_404(client: AsyncClient) -> None:
    response = await client.get(f"/datasets/{uuid4()}")

    assert response.status_code == 404
    assert response.json()["detail"] == "Dataset not found."


async def test_get_dataset_invalid_uuid_returns_422(client: AsyncClient) -> None:
    response = await client.get("/datasets/not-a-uuid")

    assert response.status_code == 422


async def test_get_dataset_with_jobs_and_report(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    async_engine: AsyncEngine,
) -> None:
    upload = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    dataset_id = UUID(upload.json()["id"])

    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    now = datetime.now(UTC)
    async with sessionmaker() as session:
        job_earlier = Job(dataset_id=dataset_id, state="success", queued_at=now)
        job_latest = Job(
            dataset_id=dataset_id,
            state="started",
            queued_at=now + timedelta(seconds=5),
        )
        report = Report(
            dataset_id=dataset_id,
            report_bucket=settings.s3_bucket_reports,
            report_key=f"datasets/{dataset_id}/report/report.json",
            report_etag="etag",
        )
        session.add_all([job_earlier, job_latest, report])
        await session.commit()

    response = await client.get(f"/datasets/{dataset_id}")

    assert response.status_code == 200
    payload = response.json()

    assert payload["latest_job_id"] == str(job_latest.id)
    assert payload["report_available"] is True


async def test_get_dataset_failed_includes_error(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    async_engine: AsyncEngine,
) -> None:
    upload = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    dataset_id = UUID(upload.json()["id"])

    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    async with sessionmaker() as session:
        dataset = await session.get(Dataset, dataset_id)
        assert dataset is not None
        dataset.status = "failed"
        dataset.error = "Parse failed"
        dataset.row_count = 0
        await session.commit()

    response = await client.get(f"/datasets/{dataset_id}")

    assert response.status_code == 200
    payload = response.json()

    assert payload["status"] == "failed"
    assert payload["row_count"] == 0
    assert payload["error"] == "Parse failed"


async def test_process_dataset_enqueues_job(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upload = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    dataset_id = UUID(upload.json()["id"])

    monkeypatch.setattr(
        celery_app,
        "send_task",
        lambda *_args, **_kwargs: SimpleNamespace(id="task-123"),
    )

    response = await client.post(f"/datasets/{dataset_id}/process")

    assert response.status_code == 202
    payload = response.json()
    assert payload["dataset_id"] == str(dataset_id)
    assert payload["state"] == "queued"
    assert payload["progress"] == 0

    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    async with sessionmaker() as session:
        jobs = (await session.scalars(select(Job).where(Job.dataset_id == dataset_id))).all()

    assert len(jobs) == 1
    job = jobs[0]
    assert payload["job_id"] == str(job.id)
    assert job.state == "queued"
    assert job.progress == 0
    assert job.celery_task_id == "task-123"


async def test_process_dataset_returns_active_job(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upload = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    dataset_id = UUID(upload.json()["id"])

    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    now = datetime.now(UTC)
    async with sessionmaker() as session:
        active_job = Job(dataset_id=dataset_id, state="started", queued_at=now)
        session.add(active_job)
        await session.commit()

    def fail_send_task(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("send_task should not be called")

    monkeypatch.setattr(celery_app, "send_task", fail_send_task)

    response = await client.post(f"/datasets/{dataset_id}/process")

    assert response.status_code == 202
    payload = response.json()
    assert payload["job_id"] == str(active_job.id)

    async with sessionmaker() as session:
        jobs = (await session.scalars(select(Job).where(Job.dataset_id == dataset_id))).all()
        assert len(jobs) == 1


async def test_process_dataset_done_returns_latest_job(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upload = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    dataset_id = UUID(upload.json()["id"])

    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    now = datetime.now(UTC)
    async with sessionmaker() as session:
        dataset = await session.get(Dataset, dataset_id)
        assert dataset is not None
        dataset.status = "done"
        job_earlier = Job(dataset_id=dataset_id, state="success", queued_at=now)
        job_latest = Job(
            dataset_id=dataset_id,
            state="success",
            queued_at=now + timedelta(seconds=5),
        )
        report = Report(
            dataset_id=dataset_id,
            report_bucket=settings.s3_bucket_reports,
            report_key=f"datasets/{dataset_id}/report/report.json",
            report_etag="etag",
        )
        session.add_all([job_earlier, job_latest, report])
        await session.commit()

    def fail_send_task(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("send_task should not be called")

    monkeypatch.setattr(celery_app, "send_task", fail_send_task)

    response = await client.post(f"/datasets/{dataset_id}/process")

    assert response.status_code == 202
    payload = response.json()
    assert payload["job_id"] == str(job_latest.id)

    async with sessionmaker() as session:
        jobs = (await session.scalars(select(Job).where(Job.dataset_id == dataset_id))).all()
        assert len(jobs) == 2


async def test_process_dataset_missing_returns_404(client: AsyncClient) -> None:
    response = await client.post(f"/datasets/{uuid4()}/process")

    assert response.status_code == 404
    assert response.json()["detail"] == "Dataset not found."


async def test_process_dataset_done_with_report_but_no_jobs(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upload = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    dataset_id = UUID(upload.json()["id"])

    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    async with sessionmaker() as session:
        dataset = await session.get(Dataset, dataset_id)
        assert dataset is not None
        dataset.status = "done"
        report = Report(
            dataset_id=dataset_id,
            report_bucket=settings.s3_bucket_reports,
            report_key=f"datasets/{dataset_id}/report/report.json",
            report_etag="etag",
        )
        session.add(report)
        await session.commit()

    def fail_send_task(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("send_task should not be called")

    monkeypatch.setattr(celery_app, "send_task", fail_send_task)

    response = await client.post(f"/datasets/{dataset_id}/process")

    assert response.status_code == 202
    payload = response.json()
    assert payload["dataset_id"] == str(dataset_id)
    assert payload["state"] == "success"
    assert payload["progress"] == 100

    async with sessionmaker() as session:
        jobs = (await session.scalars(select(Job).where(Job.dataset_id == dataset_id))).all()
        assert len(jobs) == 1


async def test_process_dataset_enqueue_failure_marks_job_failed(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    async_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upload = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    dataset_id = UUID(upload.json()["id"])

    def fail_send_task(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(celery_app, "send_task", fail_send_task)

    response = await client.post(f"/datasets/{dataset_id}/process")

    assert response.status_code == 503
    assert response.json()["detail"] == "Failed to enqueue task."

    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    async with sessionmaker() as session:
        jobs = (await session.scalars(select(Job).where(Job.dataset_id == dataset_id))).all()

    assert len(jobs) == 1
    job = jobs[0]
    assert job.state == "failure"
    assert job.error == "Failed to enqueue task."


async def test_process_dataset_database_error_returns_503(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upload = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    dataset_id = upload.json()["id"]

    async def failing_commit(_self: AsyncSession) -> None:
        raise SQLAlchemyError("boom")

    monkeypatch.setattr(AsyncSession, "commit", failing_commit)

    response = await client.post(f"/datasets/{dataset_id}/process")

    assert response.status_code == 503
    assert response.json()["detail"] == "Database error."


async def test_get_report_success(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    async_engine: AsyncEngine,
    minio_client: Minio,
) -> None:
    upload = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    dataset_id = UUID(upload.json()["id"])

    report_payload = {"row_count": 2, "null_counts": {"value": 0}}
    _upload_report_object(minio_client, dataset_id, report_payload)

    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    async with sessionmaker() as session:
        report = Report(
            dataset_id=dataset_id,
            report_bucket=settings.s3_bucket_reports,
            report_key=f"datasets/{dataset_id}/report/report.json",
            report_etag="etag",
        )
        session.add(report)
        await session.commit()

    response = await client.get(f"/datasets/{dataset_id}/report")

    assert response.status_code == 200
    assert response.json() == report_payload


async def test_get_report_object_missing_returns_503(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    async_engine: AsyncEngine,
) -> None:
    upload = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    dataset_id = UUID(upload.json()["id"])

    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    async with sessionmaker() as session:
        report = Report(
            dataset_id=dataset_id,
            report_bucket=settings.s3_bucket_reports,
            report_key=f"datasets/{dataset_id}/report/report.json",
            report_etag="etag",
        )
        session.add(report)
        await session.commit()

    response = await client.get(f"/datasets/{dataset_id}/report")

    assert response.status_code == 503
    assert response.json()["detail"] == "Failed to download report from storage."


async def test_get_report_not_ready_returns_404(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
) -> None:
    upload = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    dataset_id = upload.json()["id"]

    response = await client.get(f"/datasets/{dataset_id}/report")

    assert response.status_code == 404
    assert response.json()["detail"] == "Report not found."
