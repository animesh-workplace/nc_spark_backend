import os
import sys
from celery import Celery

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

AGING_INTERVAL_SECONDS = 1800

BEAT_DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "database",
    "celerybeat-schedule",
)

celery_app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
)


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
    imports=[
        "app.api.aging",
        "app.api.annotate",
        "app.api.annotation",
    ],
    beat_schedule_filename=BEAT_DB_PATH,
    beat_schedule={
        "boost-stale-tasks": {
            "schedule": AGING_INTERVAL_SECONDS,
            "task": "app.api.aging.boost_stale_tasks",
        },
    },
    broker_transport_options={
        "sep": ":",
        "priority_steps": list(range(10)),
        "queue_order_strategy": "priority",
    },
)
