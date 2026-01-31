from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from src.core.config import settings

async_engine = create_async_engine(settings.database_url_async, pool_pre_ping=True)
sync_engine = create_engine(settings.database_url_sync, pool_pre_ping=True)

AsyncSessionLocal = async_sessionmaker(async_engine, expire_on_commit=False)
SessionLocal = sessionmaker(bind=sync_engine, expire_on_commit=False)


def get_async_session() -> AsyncSession:
    return AsyncSessionLocal()


def get_sync_session() -> Session:
    return SessionLocal()
