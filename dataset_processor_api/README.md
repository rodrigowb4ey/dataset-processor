# Dataset Processor

[![CI](https://github.com/rodrigowb4ey/dataset-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/rodrigowb4ey/dataset-processor/actions/workflows/ci.yml)

Async dataset processing service where users upload CSV/JSON files, enqueue background processing, poll job progress, and fetch generated reports.

This folder contains the backend package inside the monorepo. The frontend lives in `../dataset_processor_web`.

## Stack

- FastAPI (async API)
- Celery worker
- Postgres (datasets, jobs, report metadata)
- RabbitMQ (broker)
- MinIO (dataset + report JSON object storage)
- Docker Compose for full local runtime

## Prerequisites

- Docker + Docker Compose
- `uv` (used to run task commands)
- `curl`
- `jq` (optional, used in the examples)

## Developer Commands (Taskipy)

List all available tasks:

```bash
uv run task --list
```

Most common workflows:

```bash
# quality gates
uv run task qa
uv run task check

# tests
uv run task test
uv run task test-parallel
uv run task test-all
uv run task test-all-parallel
uv run task test-e2e
uv run task test-fast  # alias for test

# combined checks
uv run task verify
uv run task verify-all

# docs
uv run task docs-serve
uv run task docs-build

# local stack
uv run task stack-up
uv run task stack-rebuild
uv run task stack-ps
uv run task stack-logs
uv run task stack-down

# migrations (compose-first)
uv run task migrate
uv run task db-current
uv run task db-history
uv run task db-downgrade
uv run task db-revision -m "describe your change"
```

## CI (GitHub Actions)

The CI workflow lives at `../.github/workflows/ci.yml` and runs on every pull request and on pushes to `main`.

Jobs are intentionally mapped to Taskipy commands to keep local and CI behavior aligned:

- `quality`: `uv run task check`
- `tests`: `uv run task test`
- `e2e`: `uv run task test-e2e`

To run the same checks locally before opening a PR:

```bash
uv run task check
uv run task test
uv run task test-e2e
```

## Logging

The API and worker use structured logging with request/task context propagation.

Notable behavior:

- API responses include `X-Request-ID`, and the same correlation id is emitted as `request_id` in logs.
- Sensitive values are redacted when keys contain fragments such as `password`, `secret`, `token`, `authorization`, `access_key`, or `secret_key`.

Supported environment variables:

- `LOG_LEVEL` (default: `INFO`)
- `LOG_FORMAT` (`console` or `json`, default: `console`)
- `SERVICE_NAME` (default: `dataset-processor`)
- `ENVIRONMENT` (default: `local`)

Set `LOG_FORMAT=json` in non-local environments for machine-readable logs.

## Documentation

This project uses MkDocs Material + mkdocstrings for documentation.

Serve docs locally:

```bash
uv run task docs-serve
```

Build docs in strict mode:

```bash
uv run task docs-build
```

## Run the Stack

1. Start everything:

```bash
uv run task stack-rebuild
```

2. Run migrations:

```bash
uv run task migrate
```

3. Quick health check:

```bash
curl -sS http://localhost:8000/
```

Expected response:

```json
{"Hello":"World"}
```

## API Workflow (curl)

Set a base URL:

```bash
BASE_URL=http://localhost:8000
```

Create a sample dataset file:

```bash
cat > /tmp/sales.csv <<'EOF'
id,region,total
1,north,10
2,south,20
3,south,30
EOF
```

### 1) Upload dataset

```bash
DATASET_ID=$(curl -sS -X POST "$BASE_URL/datasets" \
  -F "name=January sales" \
  -F "file=@/tmp/sales.csv;type=text/csv" | jq -r '.id')

echo "$DATASET_ID"
```

### 2) Get dataset

```bash
curl -sS "$BASE_URL/datasets/$DATASET_ID" | jq
```

### 2.1) List datasets

```bash
curl -sS "$BASE_URL/datasets" | jq
```

### 3) Enqueue processing

```bash
JOB_ID=$(curl -sS -X POST "$BASE_URL/datasets/$DATASET_ID/process" | jq -r '.job_id')

echo "$JOB_ID"
```

### 4) Poll job

```bash
while true; do
  JOB=$(curl -sS "$BASE_URL/jobs/$JOB_ID")
  STATE=$(echo "$JOB" | jq -r '.state')
  echo "$JOB" | jq '{id, state, progress, error}'

  if [ "$STATE" = "success" ] || [ "$STATE" = "failure" ]; then
    break
  fi

  sleep 1
done
```

### 5) Fetch report

```bash
curl -sS "$BASE_URL/datasets/$DATASET_ID/report" | jq
```

### Extra: list jobs

```bash
curl -sS "$BASE_URL/jobs" | jq
```

## API Behavior Notes

- Upload idempotency is checksum-based: uploading the same bytes returns the original dataset row and does not overwrite previously stored metadata.
- Upload filenames are normalized to basename before storage key generation (path components are stripped).
- Processing enqueue is idempotent: existing active jobs are reused, and completed datasets with reports reuse their latest job (or create a synthetic success job when no prior job exists).
- `GET /datasets` returns dataset summaries ordered by most recent upload first, including `latest_job_id` and `report_available`.
- Concurrent enqueue races are handled by a partial unique index on active jobs; conflicting requests return the same active job.
- If queue publish fails after creating a queued job, that job is marked `failure` with `error="Failed to enqueue task."`, and the endpoint returns `503`.
- `GET /datasets/{dataset_id}/report` returns `404` when report metadata is missing, and `503` when metadata exists but the report object cannot be downloaded.
- Expected error mapping:
  - `422`: invalid request payload/form values
  - `415`: unsupported upload content type
  - `404`: missing dataset/job/report
  - `503`: storage/database/queue failures
  - `500`: safe fallback for unhandled exceptions

## Report Semantics

- Text payload decoding accepts UTF-8 and UTF-8 BOM.
- Null counting treats `None` and blank/whitespace strings as null.
- Numeric stats include only numeric-only fields; booleans are treated as non-numeric.
- Outlier detection uses IQR and runs only when a field has at least 4 numeric values with positive IQR.

## Failure-path Example

Upload an invalid JSON payload (object instead of list):

```bash
cat > /tmp/invalid.json <<'EOF'
{"id": 1, "total": 100}
EOF

BAD_DATASET_ID=$(curl -sS -X POST "$BASE_URL/datasets" \
  -F "name=Invalid sample" \
  -F "file=@/tmp/invalid.json;type=application/json" | jq -r '.id')

BAD_JOB_ID=$(curl -sS -X POST "$BASE_URL/datasets/$BAD_DATASET_ID/process" | jq -r '.job_id')

curl -sS "$BASE_URL/jobs/$BAD_JOB_ID" | jq
curl -sS "$BASE_URL/datasets/$BAD_DATASET_ID" | jq
```

The job should end in `failure`, dataset status should become `failed`, and `error` should be populated.

## Service UIs

- Web: `http://localhost:5173`
- API: `http://localhost:8000`
- RabbitMQ management UI: `http://localhost:15672`
  - user: `dataset`
  - password: `dataset`
- MinIO Console: `http://localhost:9001`
  - user: `minio`
  - password: `minio123`

## Postman

Import collection:

- `postman/dataset-processor.postman_collection.json`

Collection variables:

- `base_url` (default: `http://localhost:8000`)
- `dataset_id`
- `job_id`

`Upload dataset` and `Enqueue processing` requests automatically store `dataset_id` and `job_id` into collection variables.

## Running Tests

Run fast tests (default, excludes e2e):

```bash
uv run task test
```

Run full suite (including e2e):

```bash
uv run task test-all
```

Run full suite in parallel (optional):

```bash
uv run task test-all-parallel
```

Run fast tests in parallel (optional):

```bash
uv run task test-parallel
```

Run only e2e tests:

```bash
uv run task test-e2e
```
