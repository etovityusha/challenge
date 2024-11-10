from django.contrib import admin

from event_logs.models import EventLogOutbox


@admin.register(EventLogOutbox)
class EventLogOutboxAdmin(admin.ModelAdmin):
    pass
