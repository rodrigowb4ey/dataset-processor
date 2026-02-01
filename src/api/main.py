from fastapi import FastAPI

from src.api.routes.datasets import router as datasets_router

app = FastAPI()

app.include_router(datasets_router)


@app.get("/")
def read_root() -> dict[str, str]:
    return {"Hello": "World"}
