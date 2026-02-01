from collections.abc import AsyncGenerator, Generator
from typing import Annotated

from fastapi import Depends
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from src.core.config import settings

async_engine = create_async_engine(settings.database_url_async, pool_pre_ping=True)
sync_engine = create_engine(settings.database_url_sync, pool_pre_ping=True)

AsyncSessionLocal = async_sessionmaker(async_engine, expire_on_commit=False)
SessionLocal = sessionmaker(bind=sync_engine, expire_on_commit=False)


async def get_async_session() -> AsyncGenerator[AsyncSession]:
    async with AsyncSessionLocal() as session:
        yield session


def get_sync_session() -> Generator[Session]:
    with SessionLocal() as session:
        yield session


T_AsyncSession = Annotated[AsyncSession, Depends(get_async_session)]
T_SyncSession = Annotated[Session, Depends(get_sync_session)]
