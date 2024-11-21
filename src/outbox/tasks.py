import structlog
from celery import shared_task

from outbox.services import OutboxProcessor

logger = structlog.get_logger(__name__)

@shared_task(bind=True)
def process_outbox_events(self) -> None:  # noqa: ANN001
    processor = OutboxProcessor(batch_size=100)
    try:
        processor.process_events()
    except Exception as e:
        self.retry(exc=e, countdown=10)
