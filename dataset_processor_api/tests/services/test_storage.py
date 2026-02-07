from typing import BinaryIO, cast

import pytest
from minio import Minio

from src.services import storage


class _FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.closed = False
        self.released = False

    def read(self) -> bytes:
        return self._payload

    def close(self) -> None:
        self.closed = True

    def release_conn(self) -> None:
        self.released = True


class _FakeClient:
    def __init__(self, response: _FakeResponse) -> None:
        self.response = response

    def get_object(self, *, bucket_name: str, object_name: str) -> _FakeResponse:
        assert bucket_name == "uploads"
        assert object_name == "datasets/id/source/data.csv"
        return self.response


def test_download_object_reads_and_releases_connection() -> None:
    response = _FakeResponse(b"payload")
    client = _FakeClient(response)

    content = storage.download_object(
        cast("Minio", client),
        "uploads",
        "datasets/id/source/data.csv",
    )

    assert content == b"payload"
    assert response.closed is True
    assert response.released is True


def test_upload_json_object_uses_json_content_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_upload_object(
        client: Minio,
        bucket: str,
        object_key: str,
        data: BinaryIO,
        length: int,
        content_type: str,
    ) -> str:
        del client
        captured["bucket"] = bucket
        captured["object_key"] = object_key
        captured["content_type"] = content_type
        captured["length"] = length
        captured["body"] = data.read()
        return "etag-123"

    monkeypatch.setattr(storage, "upload_object", fake_upload_object)

    etag = storage.upload_json_object(
        cast("Minio", object()),
        "reports",
        "datasets/id/report/report.json",
        {"rows": 2, "null_counts": {"value": 0}},
    )

    body = cast("bytes", captured["body"])
    assert etag == "etag-123"
    assert captured["bucket"] == "reports"
    assert captured["object_key"] == "datasets/id/report/report.json"
    assert captured["content_type"] == "application/json"
    assert captured["length"] == len(body)
    assert body == b'{"rows":2,"null_counts":{"value":0}}'
