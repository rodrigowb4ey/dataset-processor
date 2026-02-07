"""FastAPI application entrypoint, middleware, and error handlers."""

import time
from collections.abc import Awaitable, Callable

from asgi_correlation_id import CorrelationIdMiddleware
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response

from src.api.routes.datasets import router as datasets_router
from src.api.routes.jobs import router as jobs_router
from src.core.config import settings
from src.core.errors import AppError, UnexpectedError
from src.core.logging import bind_context, clear_context, configure_logging, get_logger

configure_logging(
    log_level=settings.log_level,
    log_format=settings.log_format,
    service_name=settings.service_name,
    environment=settings.environment,
)

app = FastAPI()
logger = get_logger(__name__)

app.include_router(datasets_router)
app.include_router(jobs_router)


@app.middleware("http")
async def request_logging_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Log incoming requests and responses with timing metadata."""
    started_at = time.perf_counter()
    clear_context()
    bind_context(http_method=request.method, http_path=request.url.path)

    logger.info("http.request.started")

    try:
        response = await call_next(request)
    except Exception:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        logger.exception("http.request.failed", duration_ms=duration_ms)
        clear_context()
        raise

    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    logger.info(
        "http.request.completed",
        status_code=response.status_code,
        duration_ms=duration_ms,
    )
    clear_context()
    return response


app.add_middleware(
    CorrelationIdMiddleware,
    header_name="X-Request-ID",
    update_request_header=True,
)


@app.exception_handler(AppError)
async def app_error_handler(_request: Request, exc: AppError) -> JSONResponse:
    """Translate domain-level exceptions into API responses."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": jsonable_encoder(exc.detail)},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    """Return a safe error response for unhandled exceptions."""
    logger.exception("app.unhandled_exception", exc_info=exc)
    fallback = UnexpectedError()
    return JSONResponse(status_code=fallback.status_code, content={"detail": fallback.detail})


@app.get("/")
def read_root() -> dict[str, str]:
    """Return a lightweight healthcheck response."""
    return {"Hello": "World"}
