FROM python:3.13-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    UV_LINK_MODE=copy \
    UV_NO_DEV=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

COPY --from=ghcr.io/astral-sh/uv:0.9.28 /uv /uvx /bin/

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
