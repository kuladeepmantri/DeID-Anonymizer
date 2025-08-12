from __future__ import annotations
from celery import Celery
import os

broker = os.getenv("CELERY_BROKER_URL")
backend = os.getenv("CELERY_RESULT_BACKEND")

celery_app = Celery("deid_worker", broker=broker, backend=backend)
celery_app.autodiscover_tasks(["app.worker"])

@celery_app.task(name="deid.echo")
def echo(value: str) -> str:
    return value