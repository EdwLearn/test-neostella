import os
from celery import Celery

broker = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@rabbitmq:5672//")
backend = os.getenv("CELERY_RESULT_BACKEND", "rpc://")

celery = Celery(
    "neostella_task", 
    broker=broker, 
    backend=backend,
    include = ["app.tasks"]
    )
celery.conf.update(task_track_started=True) 
celery.autodiscover_tasks(["app"])