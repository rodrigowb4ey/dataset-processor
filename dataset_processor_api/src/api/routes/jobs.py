"""Job API routes for listing and retrieving processing jobs."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logging import get_logger
from src.core.schemas import JobList, JobPublic, JobState
from src.db.models import Job
from src.db.session import get_async_session
from src.services import jobs as jobs_service

router = APIRouter(prefix="/jobs", tags=["jobs"])
logger = get_logger(__name__)


def _to_job_public(job: Job) -> JobPublic:
    """Convert a Job ORM entity to the public API schema."""
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
    """Return all jobs ordered by most recently queued first."""
    jobs = await jobs_service.list_jobs(session)
    logger.info("jobs.list.completed", total_jobs=len(jobs))
    return JobList(jobs=[_to_job_public(job) for job in jobs])


@router.get("/{job_id}", response_model=JobPublic)
async def get_job(
    session: Annotated[AsyncSession, Depends(get_async_session)],
    job_id: uuid.UUID,
) -> JobPublic:
    """Return a single job by identifier."""
    job = await jobs_service.get_job(session, job_id)
    logger.info("jobs.get.completed", job_id=str(job.id), job_state=job.state)
    return _to_job_public(job)
