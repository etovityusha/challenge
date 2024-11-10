from typing import Any

import structlog

from core.base_model import Model
from core.use_case import UseCase, UseCaseRequest, UseCaseResponse
from users.models import User

logger = structlog.get_logger(__name__)


class UserCreated(Model):
    email: str
    first_name: str
    last_name: str


class CreateUserRequest(UseCaseRequest):
    email: str
    first_name: str = ""
    last_name: str = ""


class CreateUserResponse(UseCaseResponse):
    result: User | None = None
    error: str = ""


class CreateUser(UseCase):
    def _get_context_vars(self, request: UseCaseRequest) -> dict[str, Any]:
        return {
            "email": request.email,
            "first_name": request.first_name,
            "last_name": request.last_name,
        }

    def _execute(self, request: CreateUserRequest) -> CreateUserResponse:
        logger.info("creating a new user")

        user, created = User.objects.get_or_create(
            email=request.email,
            defaults={
                "first_name": request.first_name,
                "last_name": request.last_name,
            },
        )
        if created:
            return CreateUserResponse(result=user)
        logger.error("unable to create a new user")
        return CreateUserResponse(error="User with this email already exists")

    @classmethod
    def _convert_response_to_log_models(cls, response_result: User) -> list[Model]:
        return [
            UserCreated(
                email=response_result.email,
                first_name=response_result.first_name,
                last_name=response_result.last_name,
            ),
        ]
