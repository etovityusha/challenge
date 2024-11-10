import clickhouse_connect
import pytest
from clickhouse_connect.driver import Client


@pytest.fixture(scope="module")
def f_ch_client() -> Client:
    client = clickhouse_connect.get_client(host="clickhouse")
    yield client
    client.close()


@pytest.fixture(autouse=True)
def use_default_settings(settings):
    settings.EVENT_SAVER_TYPE = "STUB"
