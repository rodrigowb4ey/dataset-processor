# Dataset Processor

Dataset Processor is an async backend that ingests CSV/JSON files, processes them in the background,
and serves generated report payloads.

## What it does

- Upload datasets to object storage.
- Persist dataset/job/report metadata in Postgres.
- Queue asynchronous processing with Celery over RabbitMQ.
- Compute statistics and anomaly summaries.
- Expose report retrieval endpoints.

## Core runtime components

- **API**: FastAPI app with async SQLAlchemy sessions.
- **Worker**: Celery worker with sync SQLAlchemy sessions.
- **Storage**: MinIO buckets for uploads and generated reports.
- **Database**: Postgres as source of truth.
- **Broker**: RabbitMQ for task delivery.

## Documentation map

- Architecture overview: `architecture.md`
- API request lifecycle: `api-workflow.md`
- Processing internals: `processing-pipeline.md`
- Logging strategy and event schema: `logging.md`
- Operations and troubleshooting: `operations.md`
- Auto-generated API reference: `reference/index.md`
