# API Workflow

## Health

- `GET /` returns a lightweight health payload.

## Dataset upload

- `POST /datasets` accepts multipart form-data:
  - `name`: dataset display name
  - `file`: CSV or JSON file
- API computes checksum and size before storage upload.
- If checksum already exists, existing dataset is returned (idempotent behavior).

## Dataset retrieval

- `GET /datasets` returns all dataset summaries ordered by most recent upload.
- `GET /datasets/{dataset_id}` returns:
  - dataset status
  - latest job id
  - report availability flag

## Processing enqueue

- `POST /datasets/{dataset_id}/process` returns `202` with job details.
- Endpoint is idempotent:
  - returns active job when one exists
  - returns existing success job for completed datasets with report
  - creates synthetic success job when needed
  - creates and enqueues new queued job otherwise

## Jobs

- `GET /jobs` returns all jobs (descending queue time).
- `GET /jobs/{job_id}` returns one job.

## Report retrieval

- `GET /datasets/{dataset_id}/report` streams the report JSON object from storage.
- Returns `404` when report metadata is not ready.
