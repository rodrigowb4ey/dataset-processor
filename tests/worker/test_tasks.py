from types import SimpleNamespace
from uuid import uuid4

import pytest
from celery.exceptions import MaxRetriesExceededError

from src.core.config import settings
from src.core.schemas import DatasetStatus, JobState
from src.processing.parsers import InvalidDatasetFormatError
from src.worker import tasks


def test_process_dataset_success_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_id = uuid4()
    job_id = uuid4()

    progress_updates: list[dict[str, object]] = []
    dataset_updates: list[dict[str, object]] = []
    report_updates: list[dict[str, object]] = []
    ensured_buckets: list[str] = []

    monkeypatch.setattr(
        tasks,
        "_get_dataset_or_fail",
        lambda _dataset_id: SimpleNamespace(
            upload_bucket="uploads",
            upload_key="datasets/id/source/data.csv",
            content_type="text/csv",
        ),
    )
    monkeypatch.setattr(
        tasks,
        "_set_job_progress",
        lambda **kwargs: progress_updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(
        tasks,
        "_mark_dataset_state",
        lambda **kwargs: dataset_updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(tasks, "build_minio_client", lambda: object())
    monkeypatch.setattr(tasks, "download_object", lambda *_args: b"id,value\n1,10\n2,20\n")
    monkeypatch.setattr(
        tasks,
        "parse_rows",
        lambda *_args: [{"id": "1", "value": "10"}, {"id": "2", "value": "20"}],
    )
    monkeypatch.setattr(
        tasks,
        "compute_stats",
        lambda _rows: {
            "row_count": 2,
            "null_counts": {"id": 0, "value": 0},
            "numeric": {"value": {"min": 10.0, "max": 20.0, "mean": 15.0}},
        },
    )
    monkeypatch.setattr(
        tasks,
        "compute_anomalies",
        lambda _rows: {"duplicates_count": 0, "outliers": {}},
    )
    monkeypatch.setattr(
        tasks, "ensure_bucket", lambda _client, bucket: ensured_buckets.append(bucket)
    )
    monkeypatch.setattr(tasks, "upload_json_object", lambda *_args: "etag-1")
    monkeypatch.setattr(
        tasks,
        "_upsert_report",
        lambda dataset_uuid, report_payload, report_etag: report_updates.append(
            {
                "dataset_id": dataset_uuid,
                "report_payload": report_payload,
                "report_etag": report_etag,
            }
        ),
    )

    result = tasks.process_dataset.run(str(dataset_id), str(job_id))

    assert result == f"success:{dataset_id}:{job_id}"
    assert [update["progress"] for update in progress_updates] == [5, 25, 60, 85, 100]
    assert progress_updates[0]["state"] == JobState.started.value
    assert progress_updates[-1]["state"] == JobState.success.value
    assert [update["status"] for update in dataset_updates] == [
        DatasetStatus.processing.value,
        DatasetStatus.done.value,
    ]
    assert ensured_buckets == [settings.s3_bucket_reports]
    assert len(report_updates) == 1
    assert report_updates[0]["dataset_id"] == dataset_id
    assert report_updates[0]["report_etag"] == "etag-1"


def test_process_dataset_invalid_format_fails_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = uuid4()
    job_id = uuid4()

    progress_updates: list[dict[str, object]] = []
    dataset_updates: list[dict[str, object]] = []

    monkeypatch.setattr(
        tasks,
        "_get_dataset_or_fail",
        lambda _dataset_id: SimpleNamespace(
            upload_bucket="uploads",
            upload_key="datasets/id/source/data.csv",
            content_type="text/csv",
        ),
    )
    monkeypatch.setattr(
        tasks,
        "_set_job_progress",
        lambda **kwargs: progress_updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(
        tasks,
        "_mark_dataset_state",
        lambda **kwargs: dataset_updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(tasks, "build_minio_client", lambda: object())
    monkeypatch.setattr(tasks, "download_object", lambda *_args: b"id,value\n1,10\n")
    monkeypatch.setattr(
        tasks,
        "parse_rows",
        lambda *_args: (_ for _ in ()).throw(InvalidDatasetFormatError("invalid format")),
    )
    monkeypatch.setattr(
        tasks.process_dataset,
        "retry",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("retry not expected")),
    )

    result = tasks.process_dataset.run(str(dataset_id), str(job_id))

    assert result == f"failed:{dataset_id}:{job_id}"
    assert [update["state"] for update in progress_updates] == [
        JobState.started.value,
        JobState.failure.value,
    ]
    assert [update["status"] for update in dataset_updates] == [
        DatasetStatus.processing.value,
        DatasetStatus.failed.value,
    ]
    assert progress_updates[-1]["error"] == "invalid format"


def test_process_dataset_retry_exhaustion_marks_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = uuid4()
    job_id = uuid4()

    progress_updates: list[dict[str, object]] = []
    dataset_updates: list[dict[str, object]] = []

    monkeypatch.setattr(
        tasks,
        "_get_dataset_or_fail",
        lambda _dataset_id: SimpleNamespace(
            upload_bucket="uploads",
            upload_key="datasets/id/source/data.csv",
            content_type="text/csv",
        ),
    )
    monkeypatch.setattr(
        tasks,
        "_set_job_progress",
        lambda **kwargs: progress_updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(
        tasks,
        "_mark_dataset_state",
        lambda **kwargs: dataset_updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(tasks, "build_minio_client", lambda: object())
    monkeypatch.setattr(
        tasks,
        "download_object",
        lambda *_args: (_ for _ in ()).throw(OSError("temporary network issue")),
    )
    monkeypatch.setattr(
        tasks.process_dataset,
        "retry",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(MaxRetriesExceededError()),
    )

    result = tasks.process_dataset.run(str(dataset_id), str(job_id))

    assert result == f"failed:{dataset_id}:{job_id}"
    assert [update["state"] for update in progress_updates] == [
        JobState.started.value,
        JobState.retrying.value,
        JobState.failure.value,
    ]
    assert [update["status"] for update in dataset_updates] == [
        DatasetStatus.processing.value,
        DatasetStatus.failed.value,
    ]
    assert progress_updates[-1]["error"] == "temporary network issue"


def test_process_dataset_unexpected_error_is_reraised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = uuid4()
    job_id = uuid4()

    progress_updates: list[dict[str, object]] = []
    dataset_updates: list[dict[str, object]] = []

    monkeypatch.setattr(
        tasks,
        "_get_dataset_or_fail",
        lambda _dataset_id: SimpleNamespace(
            upload_bucket="uploads",
            upload_key="datasets/id/source/data.csv",
            content_type="text/csv",
        ),
    )
    monkeypatch.setattr(
        tasks,
        "_set_job_progress",
        lambda **kwargs: progress_updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(
        tasks,
        "_mark_dataset_state",
        lambda **kwargs: dataset_updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(tasks, "build_minio_client", lambda: object())
    monkeypatch.setattr(tasks, "download_object", lambda *_args: b"id,value\n1,10\n")
    monkeypatch.setattr(tasks, "parse_rows", lambda *_args: [{"id": "1", "value": "10"}])
    monkeypatch.setattr(
        tasks,
        "compute_stats",
        lambda _rows: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(RuntimeError, match="boom"):
        tasks.process_dataset.run(str(dataset_id), str(job_id))

    assert progress_updates[-1]["state"] == JobState.failure.value
    assert dataset_updates[-1]["status"] == DatasetStatus.failed.value
