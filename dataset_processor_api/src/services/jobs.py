"""Job service layer for job retrieval operations."""

import uuid
from typing import cast

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.errors import DatabaseError, NotFoundError
from src.core.logging import get_logger
from src.db.models import Job

logger = get_logger(__name__)


async def list_jobs(session: AsyncSession) -> list[Job]:
    """Return all jobs ordered by queue time descending."""
    try:
        jobs = (
            await session.scalars(select(Job).order_by(Job.queued_at.desc(), Job.id.desc()))
        ).all()
    except SQLAlchemyError as exc:
        logger.exception("jobs.list.database_failed", exc_info=exc)
        raise DatabaseError() from exc

    return list(jobs)


async def get_job(session: AsyncSession, job_id: uuid.UUID) -> Job:
    """Return a single job by identifier."""
    try:
        job = cast("Job | None", await session.scalar(select(Job).where(Job.id == job_id)))
        if job is None:
            raise NotFoundError("Job not found.")
    except SQLAlchemyError as exc:
        logger.exception("jobs.get.database_failed", job_id=str(job_id), exc_info=exc)
        raise DatabaseError() from exc

    return job
