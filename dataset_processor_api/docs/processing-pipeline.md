# Processing Pipeline

## Task entrypoint

- Celery task name: `process_dataset`.
- Inputs: `dataset_id`, `job_id`.
- Result string format: `success:{dataset_id}:{job_id}` or `failed:{dataset_id}:{job_id}`.

## Pipeline stages

1. Fetch dataset metadata.
2. Mark dataset as `processing` and job as `started`.
3. Download upload object from storage.
4. Parse rows:
   - CSV via `csv.DictReader`
   - JSON must be a list of objects
5. Compute stats:
   - row count
   - null counts by field
   - numeric min/max/mean for numeric-only fields
6. Compute anomalies:
   - duplicate row count
   - IQR outliers with examples
7. Persist report JSON to storage.
8. Upsert report metadata row.
9. Mark dataset and job as completed.

## Progress milestones

- `5`: started
- `25`: parsed
- `60`: stats computed
- `85`: anomalies computed
- `100`: report persisted and done

## Retry behavior

- Retryable exceptions:
  - `OperationalError`
  - `OSError`
  - `S3Error`
- Max retries: `3` with exponential backoff capped at 60 seconds.
- Non-retryable dataset format/content errors fail immediately.
