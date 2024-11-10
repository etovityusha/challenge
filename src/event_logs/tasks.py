from datetime import timedelta

from django.utils import timezone

from core.celery import app
from event_logs.client import ClickhouseEventLogClient
from event_logs.models import EventLogOutbox


@app.task
def send_unsent_logs_to_clickhouse() -> str:
    unsent_logs = EventLogOutbox.objects.select_for_update().filter(is_sent=False)

    if not unsent_logs.exists():
        return "No unsent logs to process."

    with ClickhouseEventLogClient.init() as clickhouse_client:
        logs_to_send = [
            (getattr(log, col_name) for col_name in clickhouse_client.EVENT_LOG_COLUMNS) for log in unsent_logs
        ]
        clickhouse_client.insert_raw(logs_to_send)

    unsent_logs.update(is_sent=True)

    return f"Successfully sent {len(logs_to_send)} logs to Clickhouse."


@app.task
def delete_sent_logs() -> str:
    retention_period = timezone.now() - timedelta(days=30)
    sent_logs = EventLogOutbox.objects.filter(is_sent=True, event_date_time__lt=retention_period)
    deleted_count, _ = sent_logs.delete()
    return f"Deleted {deleted_count} sent logs."
