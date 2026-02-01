import asyncio
import os
import secrets
import time
from collections.abc import AsyncGenerator, Generator
from urllib.parse import urlparse

import psycopg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from minio import Minio
from psycopg import sql
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

from src.api.main import app
from src.db.session import get_async_session

POSTGRES_IMAGE = os.getenv("TEST_POSTGRES_IMAGE", "postgres:16-alpine")
MINIO_IMAGE = os.getenv("TEST_MINIO_IMAGE", "minio/minio:latest")

POSTGRES_USER = "dataset"
POSTGRES_PASSWORD = "dataset"
POSTGRES_DB = "dataset"

S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"


def _randstr(length: int = 16) -> str:
    return secrets.token_hex(length // 2)


def _replace_scheme(url: str, scheme: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(scheme=scheme).geturl()


def _wait_for_minio(client: Minio, timeout: float = 10.0) -> None:
    started = time.monotonic()
    while True:
        try:
            client.list_buckets()
            return
        except Exception:
            if time.monotonic() - started >= timeout:
                raise
            time.sleep(0.2)


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    container = PostgresContainer(
        POSTGRES_IMAGE,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def db_urls(postgres_container: PostgresContainer) -> Generator[dict[str, str], None, None]:
    database_url = urlparse(postgres_container.get_connection_url())
    base_url = database_url._replace(scheme="postgresql")
    database_name = base_url.path.removeprefix("/") or "postgres"
    test_database = f"{database_name}_test_{_randstr()}"
    main_url = base_url.geturl()
    test_url = base_url._replace(path=f"/{test_database}").geturl()

    main_conn = psycopg.connect(main_url, autocommit=True)
    main_conn.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(test_database)))

    try:
        yield {"test_url": test_url, "test_database": test_database}
    finally:
        main_conn.execute(
            sql.SQL("DROP DATABASE {} WITH (FORCE);").format(sql.Identifier(test_database))
        )
        main_conn.close()


@pytest.fixture(scope="session")
def dbengine(db_urls: dict[str, str]) -> Generator[Engine, None, None]:
    sync_url = _replace_scheme(db_urls["test_url"], "postgresql+psycopg")
    engine = create_engine(sync_url)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def async_engine(db_urls: dict[str, str]) -> Generator[AsyncEngine, None, None]:
    async_url = _replace_scheme(db_urls["test_url"], "postgresql+asyncpg")
    engine = create_async_engine(async_url)
    try:
        yield engine
    finally:
        try:
            asyncio.run(engine.dispose())
        except RuntimeError:
            engine.sync_engine.dispose()


@pytest_asyncio.fixture()
async def dbsession(
    dbengine: Engine, async_engine: AsyncEngine
) -> AsyncGenerator[AsyncSession, None]:
    from src.db.base import Base
    import src.db.models  # noqa: F401

    Base.metadata.create_all(dbengine)
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)
    async with sessionmaker() as session:
        yield session
    Base.metadata.drop_all(dbengine)


@pytest.fixture(scope="session")
def minio_container() -> Generator[MinioContainer, None, None]:
    container = MinioContainer(MINIO_IMAGE)
    container.with_env("MINIO_ROOT_USER", S3_ACCESS_KEY)
    container.with_env("MINIO_ROOT_PASSWORD", S3_SECRET_KEY)
    container.with_env("MINIO_ACCESS_KEY", S3_ACCESS_KEY)
    container.with_env("MINIO_SECRET_KEY", S3_SECRET_KEY)
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def minio_client(minio_container: MinioContainer) -> Minio:
    host = minio_container.get_container_host_ip()
    port = minio_container.get_exposed_port(9000)
    client = Minio(
        f"{host}:{port}",
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        secure=False,
    )
    _wait_for_minio(client)
    return client


@pytest_asyncio.fixture()
async def client(
    dbsession: AsyncSession,
    minio_client: Minio,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[AsyncClient, None]:
    async def get_session_override() -> AsyncGenerator[AsyncSession, None]:
        yield dbsession

    from src.api.routes import datasets as datasets_module

    app.dependency_overrides[get_async_session] = get_session_override
    monkeypatch.setattr(datasets_module, "build_minio_client", lambda: minio_client)

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client

    app.dependency_overrides.clear()


@pytest.fixture()
def dataset_name() -> str:
    return "Test dataset"


@pytest.fixture()
def sample_csv_bytes() -> bytes:
    return b"id,value\n1,10\n2,20\n"


@pytest.fixture()
def sample_json_bytes() -> bytes:
    return b'[{"id": 1, "value": 10}, {"id": 2, "value": 20}]'
