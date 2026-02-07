"""Celery application configuration for background processing tasks."""

from celery import Celery

from src.core.config import settings
from src.core.logging import configure_logging

configure_logging(
    log_level=settings.log_level,
    log_format=settings.log_format,
    service_name=settings.service_name,
    environment=settings.environment,
)

celery_app: Celery = Celery(
    "dataset_processor",
    broker=settings.celery_broker_url,
    include=["src.worker.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    worker_hijack_root_logger=False,
    worker_redirect_stdouts=False,
)
