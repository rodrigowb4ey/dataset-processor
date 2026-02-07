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

## Focused Backend Roadmap

This roadmap is intentionally limited to the five backend priorities below.

### 1) Pagination and Filtering for `GET /datasets` and `GET /jobs`

Goals:
- prevent unbounded list responses
- keep ordering stable and deterministic
- support practical filters for real operations

Plan:
- add bounded pagination (recommended: cursor-based)
  - request: `limit`, `cursor`
  - response: `next_cursor`, `has_more`
  - enforce max page size in API validation
- add dataset filters
  - `status`, `uploaded_before`, `uploaded_after`, `name_contains`
- add job filters
  - `state`, `dataset_id`, `queued_before`, `queued_after`
- preserve stable sort order with tiebreakers
  - datasets: `uploaded_at DESC, id DESC`
  - jobs: `queued_at DESC, id DESC`
- add supporting indexes for filter + sort combinations
- expand API and service tests for pagination correctness and filter combinations

### 2) Queue Reliability Hardening with Transactional Outbox

Goals:
- remove DB-write vs broker-publish mismatch windows
- make enqueue behavior recoverable and idempotent

Plan:
- add an outbox table for publish intents (for example `job_outbox`)
  - `id`, `job_id`, `payload`, `status`, `attempt_count`, `next_attempt_at`, `last_error`, timestamps
- in one DB transaction
  - create queued job
  - create outbox event
  - do not publish directly to RabbitMQ inside the request transaction
- add an outbox dispatcher worker/loop
  - publish pending events
  - persist publish result (`celery_task_id`, delivered status)
  - retry with backoff on transient errors
- enforce idempotency
  - safe duplicate publish handling
  - safe repeated dispatcher retries
- add tests for failure windows
  - crash after commit before publish
  - publish succeeds but ack/update fails

### 3) Production Observability (Metrics, Tracing, Alerts)

Goals:
- make failures and bottlenecks visible
- shorten detection and recovery time

Plan:
- add metrics exposure (Prometheus-compatible endpoint)
- instrument API and worker with core metrics
  - enqueue attempts/success/failure
  - job transitions by state
  - retry scheduled/exhausted counts
  - processing duration histogram
  - queue lag and stale-job gauges
- add distributed tracing (OpenTelemetry)
  - API request span
  - enqueue/outbox publish span
  - worker task span
  - DB and storage child spans
- define alert rules and runbook
  - stuck `queued|started|retrying` jobs
  - elevated failure rate
  - retry exhaustion spikes
  - worker heartbeat loss

### 4) Large-File Scalability (Streaming and Memory-Safe Compute)

Goals:
- keep memory bounded for large datasets
- maintain current report semantics where feasible

Plan:
- replace full-buffer download/parse with streaming reads from storage
- implement incremental parsers
  - CSV row streaming
  - JSON array streaming parser (object-by-object)
- refactor stats/anomaly computation to incremental aggregators
  - online row count and null counts
  - numeric min/max/mean without storing full columns
- handle outlier detection at scale
  - two-pass or sketch-based quantile strategy
  - bounded outlier example collection
- add safety limits and configs
  - max file size, max rows, max fields, parser guards
- extend tests with larger synthetic payloads and memory-focused assertions

### 5) Celery Beat Cleanup and Retention Automation

Goals:
- automatically recover stuck lifecycle states
- keep DB and object storage tidy over time

Plan:
- add a `beat` service to docker-compose for scheduled maintenance tasks
- add stale-job sweeper task
  - detect jobs stuck in `queued|started|retrying` beyond threshold
  - mark job `failure` and dataset `failed` with explicit stale reason
- add retention cleanup task
  - remove or archive old terminal jobs/reports metadata by policy
- add orphan reconciliation task
  - detect DB report metadata without object and object without metadata
  - apply safe cleanup policy with audit logging
- add dry-run mode and safety windows for destructive cleanup paths
- expose cleanup metrics (candidates scanned, cleaned, skipped, failed)

## Recommended Execution Order

1. Observability baseline (item 3)
2. Pagination/filtering (item 1)
3. Transactional outbox (item 2)
4. Celery beat cleanup and retention (item 5)
5. Large-file streaming scalability refactor (item 4)

## Definition of Done for This Roadmap

- list endpoints are bounded by default and filterable
- enqueue path is resilient to partial-failure windows
- metrics/traces/alerts make stuck or failing pipelines visible
- worker processing remains memory-safe for large datasets
- periodic cleanup keeps jobs/reports/storage consistent

## Notes
- `POST /datasets/{dataset_id}/process?force=true` is not implemented.
- Report retrieval reads from MinIO using metadata stored in Postgres.
