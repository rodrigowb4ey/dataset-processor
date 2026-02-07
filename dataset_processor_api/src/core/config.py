"""Application configuration loaded from environment variables."""

from pydantic import Field, computed_field
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed settings object shared by API and worker processes."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    postgres_user: str = Field(default="dataset", alias="POSTGRES_USER")
    postgres_password: str = Field(default="dataset", alias="POSTGRES_PASSWORD")
    postgres_host: str = Field(default="postgres", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="dataset", alias="POSTGRES_DB")

    rabbitmq_user: str = Field(default="dataset", alias="RABBITMQ_USER")
    rabbitmq_password: str = Field(default="dataset", alias="RABBITMQ_PASSWORD")
    rabbitmq_host: str = Field(default="rabbitmq", alias="RABBITMQ_HOST")
    rabbitmq_port: int = Field(default=5673, alias="RABBITMQ_PORT")
    rabbitmq_vhost: str = Field(default="/", alias="RABBITMQ_VHOST")

    s3_scheme: str = Field(default="http", alias="S3_SCHEME")
    s3_host: str = Field(default="minio", alias="S3_HOST")
    s3_port: int = Field(default=9000, alias="S3_PORT")
    s3_access_key: str = Field(default="minio", alias="S3_ACCESS_KEY")
    s3_secret_key: str = Field(default="minio123", alias="S3_SECRET_KEY")
    s3_bucket_uploads: str = Field(default="uploads", alias="S3_BUCKET_UPLOADS")
    s3_bucket_reports: str = Field(default="reports", alias="S3_BUCKET_REPORTS")

    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: str = Field(default="console", alias="LOG_FORMAT")
    service_name: str = Field(default="dataset-processor", alias="SERVICE_NAME")
    environment: str = Field(default="local", alias="ENVIRONMENT")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url_async(self) -> str:
        """Build the async SQLAlchemy database URL."""
        return MultiHostUrl.build(
            scheme="postgresql+asyncpg",
            username=self.postgres_user,
            password=self.postgres_password,
            host=self.postgres_host,
            port=self.postgres_port,
            path=self.postgres_db,
        ).unicode_string()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url_sync(self) -> str:
        """Build the sync SQLAlchemy database URL."""
        return MultiHostUrl.build(
            scheme="postgresql+psycopg",
            username=self.postgres_user,
            password=self.postgres_password,
            host=self.postgres_host,
            port=self.postgres_port,
            path=self.postgres_db,
        ).unicode_string()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def celery_broker_url(self) -> str:
        """Build Celery AMQP broker URL from RabbitMQ settings."""
        vhost = self.rabbitmq_vhost.lstrip("/")
        if vhost:
            path = vhost
            suffix = f"/{path}"
        else:
            suffix = "//"
        return (
            f"amqp://{self.rabbitmq_user}:{self.rabbitmq_password}"
            f"@{self.rabbitmq_host}:{self.rabbitmq_port}{suffix}"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def s3_endpoint_url(self) -> str:
        """Build S3-compatible endpoint URL."""
        return f"{self.s3_scheme}://{self.s3_host}:{self.s3_port}"


settings = Settings()
