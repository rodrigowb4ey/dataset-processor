import logging

import pytest
from httpx import AsyncClient


async def test_request_id_header_is_included(client: AsyncClient) -> None:
    response = await client.get("/")

    assert response.status_code == 200
    assert "X-Request-ID" in response.headers


async def test_request_lifecycle_logs_include_request_id(
    client: AsyncClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)

    response = await client.get("/")

    assert response.status_code == 200
    response_request_id = response.headers["X-Request-ID"]

    started_logs = [
        record.msg
        for record in caplog.records
        if record.name == "src.api.main"
        and isinstance(record.msg, dict)
        and record.msg.get("event") == "http.request.started"
    ]
    completed_logs = [
        record.msg
        for record in caplog.records
        if record.name == "src.api.main"
        and isinstance(record.msg, dict)
        and record.msg.get("event") == "http.request.completed"
    ]

    assert started_logs
    assert completed_logs
    assert all(log_entry.get("request_id") == response_request_id for log_entry in started_logs)
    assert all(log_entry.get("request_id") == response_request_id for log_entry in completed_logs)
