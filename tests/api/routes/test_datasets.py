import hashlib
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from minio import Minio
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from src.core.config import settings
from src.db.models import Dataset, Job, Report


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
    now = datetime.now(timezone.utc)
    async with sessionmaker() as session:
        job_earlier = Job(dataset_id=dataset_id, state="queued", queued_at=now)
        job_latest = Job(
            dataset_id=dataset_id,
            state="started",
            queued_at=now + timedelta(seconds=5),
        )
        report = Report(
            dataset_id=dataset_id,
            report_json={"rows": 2},
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
