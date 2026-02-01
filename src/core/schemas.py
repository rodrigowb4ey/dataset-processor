from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator


class Message(BaseModel):
    message: str


class ErrorResponse(BaseModel):
    detail: str


class DatasetStatus(str, Enum):
    uploaded = "uploaded"
    processing = "processing"
    done = "done"
    failed = "failed"


class JobState(str, Enum):
    queued = "queued"
    started = "started"
    retrying = "retrying"
    success = "success"
    failure = "failure"


class DatasetSchema(BaseModel):
    name: str = Field(min_length=1)

    @field_validator("name")
    @classmethod
    def name_validate(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("name must not be blank")
        return value


class DatasetUploadPublic(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    status: DatasetStatus
    checksum_sha256: str
    size_bytes: int


class DatasetPublic(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    status: DatasetStatus
    row_count: int | None = None
    latest_job_id: UUID | None = None
    report_available: bool
    error: str | None = None


class DatasetList(BaseModel):
    datasets: list[DatasetPublic]


class JobEnqueuePublic(BaseModel):
    job_id: UUID
    dataset_id: UUID
    state: JobState
    progress: int = Field(ge=0, le=100)


class JobPublic(BaseModel):
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
    jobs: list[JobPublic]


class ReportPublic(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    report_json: dict[str, JsonValue]


class ReportList(BaseModel):
    reports: list[ReportPublic]
