import uuid
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.schemas import JobList, JobPublic, JobState
from src.db.models import Job
from src.db.session import get_async_session
from src.services import jobs as jobs_service

router = APIRouter(prefix="/jobs", tags=["jobs"])


def _to_job_public(job: Job) -> JobPublic:
    return JobPublic(
        id=job.id,
        dataset_id=job.dataset_id,
        state=JobState(job.state),
        progress=job.progress,
        error=job.error,
        queued_at=job.queued_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
    )


@router.get("", response_model=JobList)
async def list_jobs(
    session: Annotated[AsyncSession, Depends(get_async_session)],
) -> JobList:
    jobs = await jobs_service.list_jobs(session)
    return JobList(jobs=[_to_job_public(job) for job in jobs])


@router.get("/{job_id}", response_model=JobPublic)
async def get_job(
    session: Annotated[AsyncSession, Depends(get_async_session)],
    job_id: uuid.UUID,
) -> JobPublic:
    job = await jobs_service.get_job(session, job_id)
    return _to_job_public(job)
