import os
import secrets
import time
from collections.abc import AsyncGenerator, Generator
from urllib.parse import urlparse

import psycopg
import pytest
import pytest_asyncio
from celery.contrib.testing.worker import start_worker
from httpx import ASGITransport, AsyncClient
from kombu import Connection
from minio import Minio
from psycopg import sql
from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from testcontainers.core.container import DockerContainer  # type: ignore[import-untyped]
from testcontainers.minio import MinioContainer  # type: ignore[import-untyped]
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

from src.api.main import app
from src.db.session import get_async_session

POSTGRES_IMAGE = os.getenv("TEST_POSTGRES_IMAGE", "postgres:16-alpine")
MINIO_IMAGE = os.getenv("TEST_MINIO_IMAGE", "minio/minio:latest")
RABBITMQ_IMAGE = os.getenv("TEST_RABBITMQ_IMAGE", "rabbitmq:3.13-management")

POSTGRES_USER = "dataset"
POSTGRES_PASSWORD = "dataset"
POSTGRES_DB = "dataset"

S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"

RABBITMQ_USER = "dataset"
RABBITMQ_PASSWORD = "dataset"


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


def _wait_for_rabbitmq(url: str, timeout: float = 20.0) -> None:
    started = time.monotonic()
    while True:
        try:
            with Connection(url, connect_timeout=2) as connection:
                connection.connect()
            return
        except Exception:
            if time.monotonic() - started >= timeout:
                raise
            time.sleep(0.2)


def _quote_ident(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer]:
    container = PostgresContainer(
        POSTGRES_IMAGE,
        username=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def db_urls(postgres_container: PostgresContainer) -> Generator[dict[str, str]]:
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
def db_metadata() -> MetaData:
    import src.db.models  # noqa: F401
    from src.db.base import Base

    return Base.metadata


@pytest_asyncio.fixture(scope="session")
async def async_engine(db_urls: dict[str, str]) -> AsyncGenerator[AsyncEngine]:
    async_url = _replace_scheme(db_urls["test_url"], "postgresql+asyncpg")
    engine = create_async_engine(async_url, poolclass=NullPool)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def db_schema(async_engine: AsyncEngine, db_metadata: MetaData) -> AsyncGenerator[None]:
    async with async_engine.begin() as connection:
        await connection.run_sync(db_metadata.create_all)
    yield
    async with async_engine.begin() as connection:
        await connection.run_sync(db_metadata.drop_all)


@pytest_asyncio.fixture(autouse=True)
async def db_cleanup(
    async_engine: AsyncEngine,
    db_metadata: MetaData,
    db_schema: None,
) -> AsyncGenerator[None]:
    del db_schema

    table_names = [_quote_ident(table.name) for table in reversed(db_metadata.sorted_tables)]
    if table_names:
        truncate_sql = f"TRUNCATE TABLE {', '.join(table_names)} RESTART IDENTITY CASCADE"
        async with async_engine.begin() as connection:
            await connection.exec_driver_sql(truncate_sql)

    yield


@pytest.fixture(scope="session")
def minio_container() -> Generator[MinioContainer]:
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


@pytest.fixture(scope="session")
def rabbitmq_container() -> Generator[DockerContainer]:
    container = DockerContainer(RABBITMQ_IMAGE)
    container.with_env("RABBITMQ_DEFAULT_USER", RABBITMQ_USER)
    container.with_env("RABBITMQ_DEFAULT_PASS", RABBITMQ_PASSWORD)
    container.with_exposed_ports(5672)
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def rabbitmq_url(rabbitmq_container: DockerContainer) -> str:
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(5672)
    url = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{host}:{port}//"
    _wait_for_rabbitmq(url)
    return url


@pytest.fixture(scope="module")
def e2e_celery_worker(
    db_urls: dict[str, str],
    minio_client: Minio,
    rabbitmq_url: str,
) -> Generator[None]:
    from src.services import datasets as datasets_service
    from src.worker import tasks as worker_tasks
    from src.worker.celery_app import celery_app

    sync_url = _replace_scheme(db_urls["test_url"], "postgresql+psycopg")
    sync_engine = create_engine(sync_url, pool_pre_ping=True)
    session_local = sessionmaker(bind=sync_engine, expire_on_commit=False)

    patcher = pytest.MonkeyPatch()
    patcher.setattr(worker_tasks, "SessionLocal", session_local)
    patcher.setattr(worker_tasks, "build_minio_client", lambda: minio_client)
    patcher.setattr(datasets_service, "celery_app", celery_app)

    celery_app.conf.update(
        broker_url=rabbitmq_url,
        broker_connection_retry_on_startup=True,
        task_always_eager=False,
    )

    try:
        with start_worker(celery_app, perform_ping_check=False, concurrency=1, pool="solo"):
            yield
    finally:
        patcher.undo()
        sync_engine.dispose()


@pytest_asyncio.fixture()
async def client(
    minio_client: Minio,
    monkeypatch: pytest.MonkeyPatch,
    async_engine: AsyncEngine,
) -> AsyncGenerator[AsyncClient]:
    sessionmaker = async_sessionmaker(async_engine, expire_on_commit=False)

    async def get_session_override() -> AsyncGenerator[AsyncSession]:
        async with sessionmaker() as session:
            yield session

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
