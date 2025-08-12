from __future__ import annotations
from app.worker.celery_app import celery_app
from app.services.deid import DeidService

@celery_app.task(name="deid.process_text")
def process_text(text: str) -> str:
    import asyncio
    return asyncio.run(DeidService().deidentify_text(text))
