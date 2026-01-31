# AGENTS

## Overview
Build an async dataset processor + report builder where users can upload datasets (CSV/JSON), trigger background processing, poll job progress, and fetch a generated report.

## Stack
- FastAPI (async endpoints)
- SQLAlchemy 2.0
- API uses async engine (asyncpg)
- Worker uses sync engine (psycopg) to keep Celery tasks straightforward
- Postgres (source of truth: datasets, jobs, reports)
- RabbitMQ (Celery broker)
- MinIO (object storage for datasets + reports)

## Docker Compose Requirements
Your `docker-compose.yml` must start everything.

### Services
- `api` (FastAPI + uvicorn)
- `worker` (Celery worker)
- `postgres`
- `rabbitmq` (management UI enabled)
- `minio` (optionally add `mc` init container to create buckets)

### Volumes
- Postgres data volume
- MinIO data volume

### Networking
All containers on the same compose network; `api` and `worker` reach:
- Postgres at `postgres:5432`
- RabbitMQ at `rabbitmq:5673`
- MinIO at `minio:9000`

## Environment Variables
Standardize these values:

```
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
- `uploads` — raw dataset objects
- `reports` — generated report objects

### Object Key Conventions
Upload object key:

```
datasets/{dataset_id}/source/{original_filename}
```

Report object key:

```
datasets/{dataset_id}/report/report.json
```

Optionally: `datasets/{dataset_id}/report/report.html`.

Why conventions matter: easier debugging and reproducible reports.

## Database Schema (Postgres)
Use UUID PKs and TIMESTAMPTZ. Store report payload in JSONB.

### datasets
Represents an uploaded dataset and its lifecycle.

Columns:
- `id` UUID PK
- `name` text not null
- `original_filename` text not null
- `content_type` text not null
- `status` text check in (`uploaded`, `processing`, `done`, `failed`)
- `checksum_sha256` text not null
- `size_bytes` bigint not null
- `uploaded_at` timestamptz default now()
- `processed_at` timestamptz nullable
- `row_count` int nullable
- `error` text nullable

Object storage fields:
- `upload_bucket` text not null (e.g. `uploads`)
- `upload_key` text not null
- `upload_etag` text nullable (store if you want; useful for debugging)

Indexes:
- UNIQUE(`checksum_sha256`) (optional but recommended for dedup)
- INDEX(`status`)
- INDEX(`uploaded_at`)

### jobs
Tracks background processing attempts and progress.

Columns:
- `id` UUID PK
- `dataset_id` UUID FK -> `datasets.id` (cascade delete)
- `celery_task_id` text nullable
- `state` text check in (`queued`, `started`, `retrying`, `success`, `failure`)
- `progress` int 0..100 default 0
- `queued_at` timestamptz default now()
- `started_at` timestamptz nullable
- `finished_at` timestamptz nullable
- `error` text nullable

Indexes:
- INDEX(`dataset_id`)
- INDEX(`state`)
- INDEX(`queued_at`)

### reports
Stores final report metadata and JSON output.

Columns:
- `id` UUID PK
- `dataset_id` UUID UNIQUE FK -> `datasets.id`
- `report_json` jsonb not null
- `created_at` timestamptz default now()

Object storage fields:
- `report_bucket` text not null (e.g. `reports`)
- `report_key` text not null
- `report_etag` text nullable

Indexes:
- INDEX(`created_at`)

## API Contract

### 1) Upload dataset
`POST /datasets` (multipart/form-data)

Fields:
- `name`: string
- `file`: UploadFile (CSV/JSON)

Behavior:
- Validate content type (`text/csv`, `application/json`).
- Stream upload into MinIO `uploads` bucket.
- Compute sha256 while streaming (or before upload if you prefer).
- Insert into `datasets` with:
  - `status=uploaded`
  - `upload_bucket`, `upload_key`, `checksum_sha256`, `size_bytes`

Response (201):

```
{
  "id": "uuid",
  "name": "January sales",
  "status": "uploaded",
  "checksum_sha256": "...",
  "size_bytes": 12345
}
```

Idempotency option (recommended):
- If `checksum_sha256` already exists, return the existing dataset (200 or 201; pick one and document it).

### 2) Get dataset
`GET /datasets/{dataset_id}`

Response (200):

```
{
  "id": "uuid",
  "name": "...",
  "status": "processing",
  "row_count": 1000,
  "latest_job_id": "uuid-or-null",
  "report_available": false,
  "error": null
}
```

### 3) Enqueue processing (idempotent)
`POST /datasets/{dataset_id}/process`

Rules:
- If dataset is `done` and report exists, return existing latest job (or create a synthetic success job).
- If there is an active job (`queued|started|retrying`), return it (no duplicates).
- Else:
  - create a new job (`queued`, `progress=0`)
  - enqueue Celery task `process_dataset(dataset_id, job_id)`
  - save `celery_task_id`

Response (202):

```
{ "job_id": "uuid", "dataset_id": "uuid", "state": "queued", "progress": 0 }
```

### 4) Poll job
`GET /jobs/{job_id}`

Response (200):

```
{
  "id": "uuid",
  "dataset_id": "uuid",
  "state": "started",
  "progress": 60,
  "error": null,
  "queued_at": "...",
  "started_at": "...",
  "finished_at": null
}
```

### 5) Get report (JSON)
`GET /datasets/{dataset_id}/report`

Returns the JSON report (either read `reports.report_json` from DB or stream from MinIO).

Response:
- `200` JSON body (report)
- `404` if not ready

## Background Job Behavior (Celery)
Task: `process_dataset(dataset_id, job_id)`

Worker must:
- Fetch dataset row from Postgres.
- Download the dataset object from MinIO (uploads bucket/key).
- Parse and validate:
  - CSV -> `DictReader`
  - JSON -> list of objects
- Compute:
  - `row_count`
  - null counts per field
  - numeric min/max/mean (for numeric-ish fields)
  - anomalies:
    - duplicates count
    - outliers using IQR (store count plus some examples)
- Write report:
  - upload `report.json` to MinIO reports bucket using the report key convention
  - insert/update `reports` row with `report_json`, `report_bucket`, `report_key`, `etag`
- Update statuses:
  - dataset -> `done`, set `processed_at`
  - job -> `success`, `progress=100`

### Progress Milestones
Write progress to DB at these points:
- 5%: started (dataset -> `processing`)
- 25%: downloaded + parsed
- 60%: stats computed
- 85%: anomalies computed
- 100%: report uploaded + DB updated

### Retry Rules
- Retry on transient errors (MinIO connection hiccups, Postgres connection, network).
- Do not retry on invalid format or schema (fail fast).

## Definition of Done (Acceptance Criteria)

### Functional
- docker compose up starts `api` + `worker` + `postgres` + `rabbitmq` + `minio`
- Upload endpoint:
  - streams file into MinIO
  - stores dataset row (including bucket/key/checksum)
- Process endpoint:
  - creates job row
  - enqueues Celery task
- Worker:
  - downloads object from MinIO
  - processes it
  - uploads report to MinIO
  - writes report metadata + JSON to Postgres
  - updates job progress throughout
- Report endpoint returns report JSON once ready
- Failure path: invalid dataset results in job failure + dataset failed + error stored
- Idempotency: no duplicate concurrent jobs; "already done" does not reprocess unless you later add `?force=true`

### Quality
- End-to-end tests: upload -> process -> poll until success -> fetch report
- End-to-end tests: invalid upload -> process -> failure -> error visible
- README:
  - how to run the stack
  - how to curl upload/process/poll/report
  - where to see RabbitMQ and MinIO UIs

## Recommended Milestone Order
1. Compose stack boots all services.
2. Alembic migrations + models.
3. Upload -> MinIO + dataset row.
4. Process endpoint creates job + enqueues.
5. Worker downloads from MinIO, writes dummy report to MinIO + DB.
6. Add real parsing + stats + anomalies + progress updates.
7. Tighten idempotency + retries.
8. Tests + README.

## Target Final Structure
```
.
├── AGENTS.md
├── README.md
├── pyproject.toml
├── uv.lock
├── docker-compose.yml
├── docker/
│   ├── api.Dockerfile
│   ├── worker.Dockerfile
│   └── minio.Dockerfile
├── alembic.ini
├── migrations/
│   ├── env.py
│   └── versions/
├── src/
│   ├── __init__.py
│   ├── api/
│   │   ├── main.py
│   │   ├── deps.py
│   │   └── routes/
│   │       ├── datasets.py
│   │       ├── jobs.py
│   │       └── reports.py
│   ├── core/
│   │   ├── config.py
│   │   └── logging.py
│   ├── db/
│   │   ├── base.py
│   │   ├── session.py
│   │   └── models.py
│   ├── schemas/
│   │   ├── dataset.py
│   │   ├── job.py
│   │   └── report.py
│   ├── services/
│   │   ├── storage.py
│   │   ├── datasets.py
│   │   └── reports.py
│   ├── processing/
│   │   ├── parsers.py
│   │   ├── stats.py
│   │   └── anomalies.py
│   ├── worker/
│   │   ├── celery_app.py
│   │   └── tasks.py
│   └── utils/
│           ├── checksum.py
│           └── streaming.py
└── tests/
    ├── e2e/
    └── unit/
```

## Stretch
- Presigned uploads
- Celery beat cleanup
- WebSocket progress
