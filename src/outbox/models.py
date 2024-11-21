from django.db import models
from django.utils import timezone


class EventOutbox(models.Model):
    event_type = models.CharField(max_length=255)
    event_date_time = models.DateTimeField(default=timezone.now)
    environment = models.CharField(max_length=255)
    event_context = models.JSONField()
    processed = models.BooleanField(default=False)
    processed_at = models.DateTimeField(null=True, blank=True)
