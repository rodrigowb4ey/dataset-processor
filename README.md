# Dataset Processor

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
- `curl`
- `jq` (optional, used in the examples)

## Run the Stack

1. Start everything:

```bash
docker compose up -d --build
```

2. Run migrations:

```bash
docker compose exec api alembic upgrade head
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

```bash
uv run pytest
```

Run only e2e tests:

```bash
uv run pytest -m e2e
```
