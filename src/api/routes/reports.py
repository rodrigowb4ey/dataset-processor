import asyncio
import uuid
from typing import Annotated, cast

from fastapi import APIRouter, Depends, Response
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.errors import DatabaseError, NotFoundError, StorageError
from src.db.models import Report
from src.db.session import get_async_session
from src.services.storage import build_minio_client, download_object

router = APIRouter(prefix="/datasets", tags=["reports"])


@router.get("/{dataset_id}/report")
async def get_dataset_report(
    session: Annotated[AsyncSession, Depends(get_async_session)],
    dataset_id: uuid.UUID,
) -> Response:
    try:
        report = cast(
            "Report | None",
            await session.scalar(select(Report).where(Report.dataset_id == dataset_id).limit(1)),
        )
        if report is None:
            raise NotFoundError("Report not found.")
    except SQLAlchemyError as exc:
        raise DatabaseError() from exc

    client = build_minio_client()
    try:
        payload = await asyncio.to_thread(
            download_object,
            client,
            report.report_bucket,
            report.report_key,
        )
    except Exception as exc:
        raise StorageError("Failed to download report from storage.") from exc

    return Response(content=payload, media_type="application/json")
