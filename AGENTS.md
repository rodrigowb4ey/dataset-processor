# AGENTS

## Overview
This project is a full-stack async dataset processor + report builder.

Users can:
- upload CSV/JSON datasets
- trigger background processing
- poll job progress
- fetch generated reports

Current status: the core functional flow is implemented and tested, including true async end-to-end coverage with a real broker and worker in tests, plus a React web client for interacting with API endpoints.

## Stack
- FastAPI (async API)
- SQLAlchemy 2.0
- API uses async engine (`asyncpg`)
- Worker uses sync engine (`psycopg`)
- React + TypeScript + Mantine UI (web)
- Vite + Vitest (web tooling/tests)
- Postgres (source of truth for datasets/jobs/reports)
- RabbitMQ (Celery broker)
- MinIO (object storage)
- Alembic (migrations)
- Pytest + Testcontainers (testing)

## Runtime (Docker Compose)

### Services
- `api` (FastAPI + uvicorn)
- `worker` (Celery worker)
- `web` (React + Vite dev server)
- `postgres`
- `rabbitmq` (management UI enabled)
- `minio`
- `minio-mc` (init container that creates buckets)

### Volumes
- `postgres_data`
- `minio_data`
- `web_node_modules`

### Networking / Ports
Internal service hosts used by app/worker:
- Postgres: `postgres:5432`
- RabbitMQ: `rabbitmq:5673`
- MinIO: `minio:9000`

Host ports exposed by compose:
- Web: `5173`
- API: `8000`
- Postgres: `5432`
- RabbitMQ AMQP: `5673`
- RabbitMQ UI: `15672`
- MinIO API: `9000`
- MinIO Console: `9001`

## Environment Variables
Canonical env values used by this project:

```env
POSTGRES_USER=dataset
POSTGRES_PASSWORD=dataset
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=dataset
RABBITMQ_USER=dataset
RABBITMQ_PASSWORD=dataset
RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5673
RABBITMQ_VHOST=/
S3_SCHEME=http
S3_HOST=minio
S3_PORT=9000
S3_ACCESS_KEY=minio
S3_SECRET_KEY=minio123
S3_BUCKET_UPLOADS=uploads
S3_BUCKET_REPORTS=reports
```

## Storage Contract (MinIO)

### Buckets
- `uploads` (raw dataset objects)
- `reports` (generated report objects)

### Object Key Conventions
Upload object key:

```text
datasets/{dataset_id}/source/{original_filename}
```

Report object key:

```text
datasets/{dataset_id}/report/report.json
```

## Database Schema (Postgres)

### `datasets`
- `id` UUID PK
- `name` text not null
- `original_filename` text not null
- `content_type` text not null
- `status` in (`uploaded`, `processing`, `done`, `failed`)
- `checksum_sha256` text unique not null
- `size_bytes` bigint not null
- `uploaded_at` timestamptz default now()
- `processed_at` timestamptz nullable
- `row_count` int nullable
- `error` text nullable
- `upload_bucket` text not null
- `upload_key` text not null
- `upload_etag` text nullable

Indexes:
- unique `checksum_sha256`
- index `status`
- index `uploaded_at`

### `jobs`
- `id` UUID PK
- `dataset_id` UUID FK -> `datasets.id` (cascade delete)
- `celery_task_id` text nullable
- `state` in (`queued`, `started`, `retrying`, `success`, `failure`)
- `progress` int 0..100 default 0
- `queued_at` timestamptz default now()
- `started_at` timestamptz nullable
- `finished_at` timestamptz nullable
- `error` text nullable

Indexes:
- index `dataset_id`
- index `state`
- index `queued_at`
- partial unique index `uq_jobs_active_dataset` on `dataset_id` where state in (`queued`, `started`, `retrying`)

### `reports`
- `id` UUID PK
- `dataset_id` UUID unique FK -> `datasets.id`
- `created_at` timestamptz default now()
- `report_bucket` text not null
- `report_key` text not null
- `report_etag` text nullable

Indexes:
- index `created_at`

## API Contract (Current)

### Health
- `GET /`
  - returns `{"Hello":"World"}`

### Cross-cutting behavior
- API responses include `X-Request-ID` for request correlation.
- Expected error mapping:
  - `422`: invalid request payload/form values (validation details can be a list)
  - `415`: unsupported upload content type
  - `404`: missing dataset/job/report
  - `503`: storage/database/queue failures
  - `500`: safe fallback for unhandled exceptions

### Datasets
1. `POST /datasets` (multipart form-data: `name`, `file`)
   - allowed content types: `text/csv`, `application/json`
   - computes SHA256 + size
   - normalizes filename to basename before building storage key
   - uploads to MinIO uploads bucket
   - writes dataset row with `status=uploaded`
   - idempotent by checksum (returns existing dataset; does not mutate existing metadata)
   - response model: `DatasetUploadPublic`
   - status: `201`

2. `GET /datasets`
   - returns all dataset summaries in descending `uploaded_at` order
   - each summary includes `latest_job_id` and `report_available`
   - response model: `DatasetList`

3. `GET /datasets/{dataset_id}`
   - returns dataset summary with `latest_job_id` and `report_available`
   - response model: `DatasetPublic`

4. `POST /datasets/{dataset_id}/process`
   - idempotent enqueue behavior:
      - if active job exists (`queued|started|retrying`), returns it
      - if dataset is `done` and report exists, returns latest job
      - if dataset is `done` and report exists but no prior job, creates synthetic success job
      - else creates new queued job and enqueues Celery task
   - enqueue race safety: concurrent requests can hit active-job unique index and return the existing active job
   - enqueue failure behavior: if queue send fails after job creation, job is marked `failure` with `error="Failed to enqueue task."`
   - saves `celery_task_id`
   - response model: `JobEnqueuePublic`
   - status: `202`

5. `GET /datasets/{dataset_id}/report`
   - loads JSON report object from MinIO (`report_bucket` + `report_key` in DB)
   - returns `404` if report is not ready
   - returns `503` if metadata exists but report object download fails

### Jobs
6. `GET /jobs`
   - returns all jobs in descending `queued_at` order
   - response model: `JobList`

7. `GET /jobs/{job_id}`
   - returns job details
   - response model: `JobPublic`
   - returns `404` when missing

## Background Processing (Celery)

Task name: `process_dataset(dataset_id, job_id)`

Additional task: `ping() -> "pong"` (used by worker/test health checks)

Behavior:
- fetch dataset
- set job/dataset processing state
- download dataset object from MinIO
- parse:
  - payload decoding accepts UTF-8 and UTF-8 BOM
  - CSV via `csv.DictReader`
  - JSON must be a list of objects
- compute:
  - row count
  - null counts per field (`None` and blank/whitespace strings are null)
  - numeric stats (`min`, `max`, `mean`) for numeric-only fields
    - bool values are treated as non-numeric
  - anomalies:
    - duplicate row count
    - IQR outliers + examples (only when field has >=4 numeric values and positive IQR)
- upload report JSON to MinIO reports bucket
- upsert report row in DB
- finalize dataset and job

Progress milestones:
- 5: started / dataset processing
- 25: parsed
- 60: stats computed
- 85: anomalies computed
- 100: report persisted + done

Retry rules:
- retryable: `OperationalError`, `OSError`, `S3Error`
- non-retryable: invalid dataset format/schema/content
- max retries: 3 (exponential backoff with cap)

## Testing (Current)

Implemented test coverage:
- API route tests
  - `tests/api/routes/test_datasets.py`
  - `tests/api/routes/test_jobs.py`
  - `tests/api/routes/test_logging.py`
- Service tests
  - `tests/services/test_datasets_service.py`
  - `tests/services/test_jobs_service.py`
  - `tests/services/test_storage.py`
- Core logging tests
  - `tests/core/test_logging_config.py`
- Processing unit tests
  - `tests/processing/test_parsers.py`
  - `tests/processing/test_stats.py`
  - `tests/processing/test_anomalies.py`
- Worker task tests
  - `tests/worker/test_tasks.py`
- True async e2e tests
  - `tests/e2e/test_async_flow.py`
  - runs with real RabbitMQ + Celery test worker + Postgres + MinIO

## Current Project Structure

```text
.
├── AGENTS.md
├── README.md
├── docker-compose.yml
├── dataset_processor_api/
│   ├── pyproject.toml
│   ├── uv.lock
│   ├── docker/
│   │   ├── api.Dockerfile
│   │   └── worker.Dockerfile
│   ├── migrations/
│   ├── src/
│   ├── tests/
│   ├── docs/
│   └── postman/
└── dataset_processor_web/
    ├── package.json
    ├── vite.config.ts
    └── src/
```

## Stretch Roadmap (Future)

- Presigned uploads
  - add endpoint(s) that issue short-lived MinIO signed URLs
  - allow clients to upload directly to object storage
  - add completion/verification step before dataset is considered `uploaded`

- Celery beat cleanup
  - add `beat` service and periodic cleanup tasks
  - detect and mark stale jobs as failed
  - optional retention cleanup for old jobs/reports/orphaned objects

- WebSocket progress
  - add WS endpoint (for example `/ws/jobs/{job_id}`)
  - push job state/progress updates in real time
  - keep polling endpoints as fallback

## Notes
- `POST /datasets/{dataset_id}/process?force=true` is not implemented.
- Report retrieval reads from MinIO using metadata stored in Postgres.
