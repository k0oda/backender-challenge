import re
from typing import Any

import structlog
from django.conf import settings
from django.utils import timezone

from core.base_model import Model
from core.event_log_client import EventLogClient
from outbox.models import EventOutbox

logger = structlog.get_logger(__name__)

class OutboxProcessor:
    def __init__(self, batch_size: int = 100) -> None:
        self.batch_size = batch_size

    def process_events(self) -> None:
        try:
            events = EventOutbox.objects.filter(processed=False)[:self.batch_size]
            if not events:
                logger.info('no events in outbox')
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

    def insert_to_outbox(
            self,
            data: list[Model],
    ) -> None:
        try:
            converted_data = self._convert_data(data)
            e = EventOutbox.objects.bulk_create([
                EventOutbox(
                    event_type=event[0],
                    event_date_time=event[1],
                    environment=event[2],
                    event_context=event[3],
                ) for event in converted_data
            ])
        except Exception as e:
            logger.error('unable to insert data to outbox', error=str(e))

    def _convert_data(self, data: list[Model]) -> list[tuple[Any]]:
        return [
            (
                self._to_snake_case(event.__class__.__name__),
                timezone.now(),
                settings.ENVIRONMENT,
                event.model_dump_json(),
            )
            for event in data
        ]

    def _to_snake_case(self, event_name: str) -> str:
        result = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', event_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', result).lower()
