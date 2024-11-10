import abc
import datetime
import re
from collections.abc import Callable, Generator
from contextlib import contextmanager
from functools import wraps
from typing import Any, Protocol, TypeVar

import clickhouse_connect
import structlog
from clickhouse_connect.driver.exceptions import DatabaseError
from django.conf import settings
from django.utils import timezone

from core.base_model import Model
from event_logs.models import EventLogOutbox

logger = structlog.get_logger(__name__)


ConvertedData = list[tuple[str, datetime.datetime, str, str]]


class EventLogClientProtocol(Protocol):
    @abc.abstractmethod
    def insert(self, data: list[Model]) -> None:
        pass

    def _convert_data(self, data: list[Model]) -> ConvertedData:
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
        result = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", event_name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", result).lower()

    @classmethod
    @contextmanager
    def init(cls) -> Generator:
        yield cls()


class ClickhouseEventLogClient(EventLogClientProtocol):
    EVENT_LOG_COLUMNS = [
        "event_type",
        "event_date_time",
        "environment",
        "event_context",
    ]

    def __init__(self, client: clickhouse_connect.driver.Client) -> None:
        self._client = client

    @classmethod
    @contextmanager
    def init(cls) -> Generator["ClickhouseEventLogClient"]:
        client = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            user=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            query_retries=2,
            connect_timeout=30,
            send_receive_timeout=10,
        )
        try:
            yield cls(client)
        except Exception as e:
            logger.error("error while executing clickhouse query", error=str(e))
        finally:
            client.close()

    def insert(
        self,
        data: list[Model],
    ) -> None:
        try:
            self._client.insert(
                data=self._convert_data(data),
                column_names=self.__class__.EVENT_LOG_COLUMNS,
                database=settings.CLICKHOUSE_SCHEMA,
                table=settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME,
            )
        except DatabaseError as e:
            logger.error("unable to insert data to clickhouse", error=str(e))

    def insert_raw(self, data: list[tuple]) -> None:
        try:
            self._client.insert(
                data=data,
                column_names=self.__class__.EVENT_LOG_COLUMNS,
                database=settings.CLICKHOUSE_SCHEMA,
                table=settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME,
            )
        except DatabaseError as e:
            logger.error("unable to insert data to clickhouse", error=str(e))

    def query(self, query: str) -> Any:  # noqa: ANN401
        logger.debug("executing clickhouse query", query=query)

        try:
            return self._client.query(query).result_rows
        except DatabaseError as e:
            logger.error("failed to execute clickhouse query", error=str(e))
            return


class StubEventLogClient(EventLogClientProtocol):
    def insert(self, data: list[Model]) -> None:
        pass

    @classmethod
    @contextmanager
    def init(cls) -> Generator["StubEventLogClient"]:
        yield cls()


class OutboxEventLogClient(EventLogClientProtocol):
    def insert(
        self,
        data: list[Model],
    ) -> None:
        converted: ConvertedData = self._convert_data(data)
        self._save_to_outbox(converted)

    def _save_to_outbox(self, converted_data: ConvertedData) -> None:
        outbox_records = [
            EventLogOutbox(
                event_type=event_type,
                event_date_time=event_date_time,
                environment=environment,
                event_context=event_context,
                is_sent=False,
            )
            for event_type, event_date_time, environment, event_context in converted_data
        ]
        EventLogOutbox.objects.bulk_create(outbox_records)


T = TypeVar("T", bound=ClickhouseEventLogClient)


def insert_to_event_log(
    saver_factory: Callable[[], T],
) -> Callable[[Callable[..., list[Model]]], Callable[..., list[Model]]]:
    """
    A decorator for automatically inserting the result of a function into the event log.

    Arguments:
        saver_factory (Callable[[], T]): A factory function that returns an instance implementing
        the `insert` method for saving the list of models in the event log.

    Returns:
        Callable: A wrapped function that stores the result in the event log.
    """

    def decorator(func: Callable[..., list[Model]]) -> Callable[..., list[Model]]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> list[Model]:
            result: list[Model] = func(*args, **kwargs)
            with saver_factory() as client:
                client.insert(result)
                logger.info("Event logs inserted by client", client_name=client.__class__.__name__)
            return result

        return wrapper

    return decorator


def get_event_saver(event_saver_type: str):
    """
    Returns a factory function for creating event log clients based on the specified type.

    Arguments:
        event_saver_type (str): The type of event saver client to create.
        Must be one of "STUB", "CLICKHOUSE", or "OUTBOX".
    """
    match event_saver_type:
        case "STUB":
            cls = StubEventLogClient
        case "CLICKHOUSE":
            cls = ClickhouseEventLogClient
        case "OUTBOX":
            cls = OutboxEventLogClient
        case _:
            raise ValueError(f"Unknown event saver type: {event_saver_type}")

    return cls.init
