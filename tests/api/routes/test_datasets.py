import hashlib

import pytest
from httpx import AsyncClient
from minio import Minio
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings


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
