import uuid
from typing import Annotated, cast

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.errors import DatabaseError, NotFoundError
from src.db.models import Report
from src.db.session import get_async_session

router = APIRouter(prefix="/datasets", tags=["reports"])


@router.get("/{dataset_id}/report")
async def get_dataset_report(
    session: Annotated[AsyncSession, Depends(get_async_session)],
    dataset_id: uuid.UUID,
) -> dict[str, object]:
    try:
        report = cast(
            "Report | None",
            await session.scalar(select(Report).where(Report.dataset_id == dataset_id).limit(1)),
        )
        if report is None:
            raise NotFoundError("Report not found.")
    except SQLAlchemyError as exc:
        raise DatabaseError() from exc
    return cast("dict[str, object]", report.report_json)
