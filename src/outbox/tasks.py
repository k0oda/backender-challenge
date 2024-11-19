import structlog
from celery import shared_task
from django.utils import timezone

from core.event_log_client import EventLogClient
from outbox.models import EventOutbox

logger = structlog.get_logger(__name__)

@shared_task(bind=True)
def process_outbox_events(self) -> None:  # noqa: ANN001
    try:
        events = EventOutbox.objects.filter(processed=False)[:100]
        if not events:
            return

        batch_data = [
            (
                event.event_type,
                event.event_date_time,
                event.environment,
                event.event_context,
            )
            for event in events
        ]

        with EventLogClient.init() as client:
            client.insert(
                data=batch_data,
            )

        EventOutbox.objects.filter(id__in=[e.id for e in events]).update(
            processed=True,
            processed_at=timezone.now(),
        )
    except Exception as e:
        logger.error('failure to process events', error=str(e))
        self.retry(exc=e, countdown=10)
