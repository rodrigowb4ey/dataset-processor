# Logging

## Goals

- Produce structured, searchable logs across API and worker processes.
- Carry correlation metadata end-to-end for incident triage.
- Avoid leaking secrets or sensitive values.

## Stack

- `structlog` for structured event logging.
- Stdlib logging bridge for third-party loggers.
- `contextvars` to propagate request/task context.
- `asgi-correlation-id` middleware for HTTP request IDs.

## Environment variables

- `LOG_LEVEL` (default: `INFO`)
- `LOG_FORMAT` (`console` or `json`, default: `console`)
- `SERVICE_NAME` (default: `dataset-processor`)
- `ENVIRONMENT` (default: `local`)

## Context fields

Common fields emitted by processors:

- `service`
- `environment`
- `request_id` (for HTTP requests when available)
- additional per-event fields such as `dataset_id`, `job_id`, and `status_code`

## Event naming convention

Use dotted event names scoped by domain:

- `http.request.started`
- `dataset.upload.completed`
- `datasets.enqueue_job_task.completed`
- `worker.task.retry_scheduled`

This keeps queries predictable in centralized log tooling.

## Redaction

The logging pipeline redacts values for sensitive key fragments, including:

- `password`
- `secret`
- `token`
- `authorization`
- `access_key`
- `secret_key`

## Local vs non-local output

- Local development: human-readable console renderer.
- Non-local environments: set `LOG_FORMAT=json` for machine-parsed logs.
