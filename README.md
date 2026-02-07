# Dataset Processor

[![CI](https://github.com/rodrigowb4ey/dataset-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/rodrigowb4ey/dataset-processor/actions/workflows/ci.yml)

Async dataset processing service where users upload CSV/JSON files, enqueue background processing, poll job progress, and fetch generated reports.

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

The CI workflow lives at `.github/workflows/ci.yml` and runs on every pull request and on pushes to `main`.

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
