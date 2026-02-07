import uuid
from typing import cast

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.errors import DatabaseError, NotFoundError
from src.db.models import Job


async def list_jobs(session: AsyncSession) -> list[Job]:
    try:
        jobs = (
            await session.scalars(select(Job).order_by(Job.queued_at.desc(), Job.id.desc()))
        ).all()
    except SQLAlchemyError as exc:
        raise DatabaseError() from exc

    return list(jobs)


async def get_job(session: AsyncSession, job_id: uuid.UUID) -> Job:
    try:
        job = cast("Job | None", await session.scalar(select(Job).where(Job.id == job_id)))
        if job is None:
            raise NotFoundError("Job not found.")
    except SQLAlchemyError as exc:
        raise DatabaseError() from exc

    return job
