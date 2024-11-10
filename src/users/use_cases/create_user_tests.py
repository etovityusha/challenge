from unittest.mock import MagicMock, patch

import pytest

from users.use_cases import CreateUser, CreateUserRequest, UserCreated

pytestmark = [pytest.mark.django_db]


@pytest.fixture()
def f_use_case() -> CreateUser:
    return CreateUser()


def test_user_created(f_use_case: CreateUser) -> None:
    request = CreateUserRequest(
        email="test@email.com",
        first_name="Test",
        last_name="Testovich",
    )

    response = f_use_case.execute(request)

    assert response.result.email == "test@email.com"
    assert response.error == ""


def test_emails_are_unique(f_use_case: CreateUser) -> None:
    request = CreateUserRequest(
        email="test@email.com",
        first_name="Test",
        last_name="Testovich",
    )

    f_use_case.execute(request)
    response = f_use_case.execute(request)

    assert response.result is None
    assert response.error == "User with this email already exists"


@patch("event_logs.client.StubEventLogClient.insert")
def test_event_log_is_inserted(insert: MagicMock, f_use_case: CreateUser) -> None:
    insert.return_value = None

    request = CreateUserRequest(
        email="test2@email.com",
        first_name="Test2",
        last_name="Testovich2",
    )

    f_use_case.execute(request)

    insert.assert_called_once_with([UserCreated(email="test2@email.com", first_name="Test2", last_name="Testovich2")])
