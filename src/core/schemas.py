"""Pydantic schemas and enums used by API and worker layers."""

from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Message(BaseModel):
    """Generic success message response."""

    message: str


class ErrorResponse(BaseModel):
    """Generic error response model."""

    detail: str


class DatasetStatus(str, Enum):
    """Allowed dataset processing states."""

    uploaded = "uploaded"
    processing = "processing"
    done = "done"
    failed = "failed"


class JobState(str, Enum):
    """Allowed asynchronous job states."""

    queued = "queued"
    started = "started"
    retrying = "retrying"
    success = "success"
    failure = "failure"


class DatasetSchema(BaseModel):
    """Input schema for dataset creation."""

    name: str = Field(min_length=1)

    @field_validator("name")
    @classmethod
    def name_validate(cls, value: str) -> str:
        """Normalize and validate dataset name values."""
        value = value.strip()
        if not value:
            raise ValueError("name must not be blank")
        return value


class DatasetUploadPublic(BaseModel):
    """Public dataset response returned after upload."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    status: DatasetStatus
    checksum_sha256: str
    size_bytes: int


class DatasetPublic(BaseModel):
    """Public dataset summary response."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    status: DatasetStatus
    row_count: int | None = None
    latest_job_id: UUID | None = None
    report_available: bool
    error: str | None = None


class DatasetList(BaseModel):
    """List wrapper for dataset responses."""

    datasets: list[DatasetPublic]


class JobEnqueuePublic(BaseModel):
    """Response model for enqueue processing endpoint."""

    job_id: UUID
    dataset_id: UUID
    state: JobState
    progress: int = Field(ge=0, le=100)


class JobPublic(BaseModel):
    """Public representation of a processing job."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    dataset_id: UUID
    state: JobState
    progress: int = Field(ge=0, le=100)
    error: str | None = None
    queued_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


class JobList(BaseModel):
    """List wrapper for job responses."""

    jobs: list[JobPublic]


class ReportPublic(BaseModel):
    """Public report metadata response model."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    dataset_id: UUID
    report_bucket: str
    report_key: str
    report_etag: str | None = None
    created_at: datetime


class ReportList(BaseModel):
    """List wrapper for report responses."""

    reports: list[ReportPublic]
