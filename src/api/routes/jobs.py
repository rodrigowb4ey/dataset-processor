import uuid
from typing import Annotated, cast

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.errors import DatabaseError, NotFoundError
from src.core.schemas import JobPublic, JobState
from src.db.models import Job
from src.db.session import get_async_session

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("/{job_id}", response_model=JobPublic)
async def get_job(
    session: Annotated[AsyncSession, Depends(get_async_session)],
    job_id: uuid.UUID,
) -> JobPublic:
    try:
        job = cast("Job | None", await session.scalar(select(Job).where(Job.id == job_id)))
        if job is None:
            raise NotFoundError("Job not found.")
    except SQLAlchemyError as exc:
        raise DatabaseError() from exc

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
