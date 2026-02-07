# Dataset Processor Monorepo

This repository contains a full-stack study project with an async Python API and a React web client.

## Layout

- `dataset_processor_api`: FastAPI + SQLAlchemy + Alembic + Celery worker
- `dataset_processor_web`: React + TypeScript + Mantine UI + Vite + Vitest
- `docker-compose.yml`: local orchestration (Postgres, RabbitMQ, MinIO, API, worker, web)

## Quick start

1. Start the stack:

```bash
docker compose up -d --build
```

2. Run DB migrations:

```bash
docker compose run --rm api alembic upgrade head
```

3. Open services:

- Web app: `http://localhost:5173`
- API: `http://localhost:8000`
- RabbitMQ UI: `http://localhost:15672`
- MinIO Console: `http://localhost:9001`

## Working on each project

Backend:

```bash
cd dataset_processor_api
uv sync --locked --all-groups
uv run task test
```

Frontend:

```bash
cd dataset_processor_web
npm ci
npm run dev
```
