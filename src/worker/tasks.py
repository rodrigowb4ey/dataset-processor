from .celery_app import celery_app


@celery_app.task
def ping() -> str:
    return "pong"


@celery_app.task(name="process_dataset")
def process_dataset(dataset_id: str, job_id: str) -> str:
    return f"queued:{dataset_id}:{job_id}"
