import datetime
from unittest.mock import ANY

import pytest
from clickhouse_connect.driver import Client

from core import settings
from core.base_model import Model
from event_logs.client import ClickhouseEventLogClient, OutboxEventLogClient
from event_logs.models import EventLogOutbox


@pytest.fixture(scope="function", autouse=True)
def f_clean_up_event_log(f_ch_client: Client) -> None:
    f_ch_client.query(f"TRUNCATE TABLE {settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME}")


class TestModel(Model):
    field1: int
    field2: str


@pytest.fixture
def test_result() -> list[TestModel]:
    return [TestModel(field1=1, field2="2")]


def test_clickhouse_client(
    f_ch_client: Client,
    test_result: list[TestModel],
) -> None:
    with ClickhouseEventLogClient(client=f_ch_client) as client:
        client.insert(data=test_result)

        log = client.query("SELECT * FROM default.event_log")
        assert log == [
            (
                "test_model",
                ANY,
                "Local",
                '{"field1":1,"field2":"2"}',
                1,
            ),
        ]


@pytest.mark.django_db
def test_outbox_client(
    test_result: list[TestModel],
) -> None:
    with OutboxEventLogClient.init() as client:
        client.insert(data=test_result)

    qs = EventLogOutbox.objects.all()
    assert len(qs) == 1

    obj = qs[0]
    assert obj.environment == "Local"
    assert obj.event_type == "test_model"
    assert obj.event_context == '{"field1":1,"field2":"2"}'
    assert isinstance(obj.event_date_time, datetime.datetime)
