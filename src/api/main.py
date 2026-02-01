import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.api.routes.datasets import router as datasets_router
from src.core.errors import AppError, UnexpectedError

app = FastAPI()
logger = logging.getLogger(__name__)

app.include_router(datasets_router)


@app.exception_handler(AppError)
async def app_error_handler(_request: Request, exc: AppError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(Exception)
async def unhandled_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled exception", exc_info=exc)
    fallback = UnexpectedError()
    return JSONResponse(status_code=fallback.status_code, content={"detail": fallback.detail})


@app.get("/")
def read_root() -> dict[str, str]:
    return {"Hello": "World"}
