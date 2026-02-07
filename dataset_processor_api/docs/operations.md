# Operations

## Local stack

Start stack:

```bash
uv run task stack-rebuild
```

Run migrations:

```bash
uv run task migrate
```

View service logs:

```bash
uv run task stack-logs
```

Open web UI:

- `http://localhost:5173`

## Quality checks

Run full local quality checks:

```bash
uv run task check
```

Run tests only:

```bash
uv run task test
```

Build documentation:

```bash
uv run task docs-build
```

## Troubleshooting

### Jobs stuck in queued

- Verify RabbitMQ is healthy and reachable.
- Verify worker container is running.
- Check worker logs for broker connection errors.

### Report not found

- Confirm job reached `success`.
- Confirm report row exists in `reports` table.
- Confirm object exists in MinIO reports bucket.

### Repeated retries

- Look for `worker.task.retry_scheduled` and `worker.task.retry_exhausted` events.
- Confirm Postgres and MinIO network health.
- Check transient infrastructure errors before rerunning.
