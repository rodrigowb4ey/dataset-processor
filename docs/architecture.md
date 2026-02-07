# Architecture

## High-level flow

1. Client uploads a dataset (`POST /datasets`).
2. API stores file in MinIO and metadata in Postgres.
3. Client requests processing (`POST /datasets/{id}/process`).
4. API enqueues `process_dataset` task in RabbitMQ.
5. Celery worker processes file, computes report, and writes report metadata.
6. Client polls job state (`GET /jobs/{id}`) and fetches report (`GET /datasets/{id}/report`).

## Service boundaries

- **API process**
  - Handles HTTP validation and orchestration.
  - Uses async DB engine (`asyncpg`).
  - Does not perform heavy report computations.
- **Worker process**
  - Performs parsing/statistics/anomaly computations.
  - Uses sync DB engine (`psycopg`).
  - Owns progress updates and final state transitions.

## Storage contracts

- Upload objects:

```text
datasets/{dataset_id}/source/{original_filename}
```

- Report objects:

```text
datasets/{dataset_id}/report/report.json
```

## Data model summary

- `datasets`: upload metadata + final processing outcome.
- `jobs`: execution state/progress timeline.
- `reports`: report object location and metadata.

## Reliability model

- Idempotent dataset uploads by checksum.
- Idempotent process enqueue behavior.
- Retryable worker errors for transient infra failures (DB/network/S3).
- Non-retryable worker errors for invalid dataset content/format.
