from django.db import models


class EventLogOutbox(models.Model):
    EVENT_TYPE_MAX_LENGTH = 50
    ENVIRONMENT_MAX_LENGTH = 20

    event_type = models.CharField(max_length=EVENT_TYPE_MAX_LENGTH, null=False, db_index=True)
    event_date_time = models.DateTimeField(db_index=True)
    environment = models.CharField(max_length=ENVIRONMENT_MAX_LENGTH, null=False, db_index=True)
    event_context = models.TextField()
    is_sent = models.BooleanField(default=False, db_index=True)

    class Meta:
        db_table = "event_log_outbox"
