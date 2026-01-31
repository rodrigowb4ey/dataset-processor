from celery import Celery

from src.core.config import settings

celery_app: Celery = Celery(
    "dataset_processor",
    broker=settings.celery_broker_url,
    include=["src.worker.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
)
