import os
import sys
from celery import Celery

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

celery_app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
)

celery_app.autodiscover_tasks(["app.api.annotate", "app.api.annotation"])

celery_app.conf.update(
    timezone="UTC",
    enable_utc=True,
    result_expires=3600,
    task_acks_late=True,
    worker_concurrency=1,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
)
