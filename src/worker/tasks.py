from .celery_app import celery_app


@celery_app.task
def ping() -> str:
    return "pong"
