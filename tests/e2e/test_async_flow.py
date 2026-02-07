import asyncio
import time
from typing import cast

import pytest
from httpx import AsyncClient


async def _poll_job_until_terminal(
    client: AsyncClient,
    job_id: str,
    timeout_seconds: float = 25.0,
    interval_seconds: float = 0.2,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    last_payload: dict[str, object] | None = None

    while time.monotonic() < deadline:
        response = await client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        payload = cast("dict[str, object]", response.json())
        last_payload = payload
        if payload["state"] in {"success", "failure"}:
            return payload
        await asyncio.sleep(interval_seconds)

    raise AssertionError(f"Timed out waiting for job terminal state. last_payload={last_payload}")


@pytest.mark.e2e
async def test_async_e2e_upload_process_poll_report_success(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    e2e_celery_worker: None,
) -> None:
    del e2e_celery_worker

    upload_response = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    assert upload_response.status_code == 201
    dataset_id = upload_response.json()["id"]

    process_response = await client.post(f"/datasets/{dataset_id}/process")
    assert process_response.status_code == 202
    process_payload = process_response.json()

    terminal_job = await _poll_job_until_terminal(client, process_payload["job_id"])
    assert terminal_job["state"] == "success"
    assert terminal_job["progress"] == 100
    assert terminal_job["finished_at"] is not None

    dataset_response = await client.get(f"/datasets/{dataset_id}")
    assert dataset_response.status_code == 200
    dataset_payload = dataset_response.json()
    assert dataset_payload["status"] == "done"
    assert dataset_payload["row_count"] == 2
    assert dataset_payload["report_available"] is True
    assert dataset_payload["error"] is None

    report_response = await client.get(f"/datasets/{dataset_id}/report")
    assert report_response.status_code == 200
    report_payload = report_response.json()
    assert report_payload["dataset_id"] == dataset_id
    assert report_payload["row_count"] == 2
    assert "null_counts" in report_payload
    assert "numeric" in report_payload
    assert "anomalies" in report_payload


@pytest.mark.e2e
async def test_async_e2e_invalid_dataset_fails_and_exposes_error(
    client: AsyncClient,
    dataset_name: str,
    e2e_celery_worker: None,
) -> None:
    del e2e_celery_worker

    invalid_json = b'{"id": 1, "value": 10}'
    upload_response = await client.post(
        "/datasets",
        data={"name": f"{dataset_name} invalid"},
        files={"file": ("invalid.json", invalid_json, "application/json")},
    )
    assert upload_response.status_code == 201
    dataset_id = upload_response.json()["id"]

    process_response = await client.post(f"/datasets/{dataset_id}/process")
    assert process_response.status_code == 202
    process_payload = process_response.json()

    terminal_job = await _poll_job_until_terminal(client, process_payload["job_id"])
    assert terminal_job["state"] == "failure"
    assert terminal_job["progress"] == 100
    assert terminal_job["error"] is not None

    dataset_response = await client.get(f"/datasets/{dataset_id}")
    assert dataset_response.status_code == 200
    dataset_payload = dataset_response.json()
    assert dataset_payload["status"] == "failed"
    assert dataset_payload["error"] is not None
    assert "list of objects" in dataset_payload["error"]

    report_response = await client.get(f"/datasets/{dataset_id}/report")
    assert report_response.status_code == 404
