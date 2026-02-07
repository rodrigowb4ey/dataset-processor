from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from src.db.models import Job


async def _upload_dataset(client: AsyncClient, dataset_name: str, sample_csv_bytes: bytes) -> UUID:
    response = await client.post(
        "/datasets",
        data={"name": dataset_name},
        files={"file": ("data.csv", sample_csv_bytes, "text/csv")},
    )
    return UUID(response.json()["id"])


async def test_get_jobs_empty_returns_empty_list(client: AsyncClient) -> None:
    response = await client.get("/jobs")

    assert response.status_code == 200
    assert response.json() == {"jobs": []}


async def test_get_jobs_returns_descending_order(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    async_engine: AsyncEngine,
) -> None:
    dataset_id = await _upload_dataset(client, dataset_name, sample_csv_bytes)

    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    now = datetime.now(UTC)
    async with sessionmaker() as session:
        older_job = Job(dataset_id=dataset_id, state="success", progress=100, queued_at=now)
        newer_job = Job(
            dataset_id=dataset_id,
            state="started",
            progress=40,
            queued_at=now + timedelta(seconds=10),
        )
        session.add_all([older_job, newer_job])
        await session.commit()

    response = await client.get("/jobs")

    assert response.status_code == 200
    payload = response.json()
    jobs = payload["jobs"]
    assert len(jobs) == 2
    assert jobs[0]["id"] == str(newer_job.id)
    assert jobs[1]["id"] == str(older_job.id)


async def test_get_job_success(
    client: AsyncClient,
    dataset_name: str,
    sample_csv_bytes: bytes,
    async_engine: AsyncEngine,
) -> None:
    dataset_id = await _upload_dataset(client, dataset_name, sample_csv_bytes)

    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    async with sessionmaker() as session:
        job = Job(dataset_id=dataset_id, state="started", progress=40)
        session.add(job)
        await session.commit()

    response = await client.get(f"/jobs/{job.id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == str(job.id)
    assert payload["dataset_id"] == str(dataset_id)
    assert payload["state"] == "started"
    assert payload["progress"] == 40


async def test_get_job_not_found_returns_404(client: AsyncClient) -> None:
    response = await client.get(f"/jobs/{uuid4()}")

    assert response.status_code == 404
    assert response.json()["detail"] == "Job not found."


async def test_get_job_invalid_uuid_returns_422(client: AsyncClient) -> None:
    response = await client.get("/jobs/not-a-uuid")

    assert response.status_code == 422
