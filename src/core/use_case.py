import abc
from typing import Any, Protocol

import structlog
from django.conf import settings
from django.db import transaction

from core.base_model import Model
from event_logs.client import get_event_saver


class UseCaseRequest(Model):
    pass


class UseCaseResponse(Model):
    result: Any = None
    error: str = ""


class UseCase(Protocol):
    is_save_event_logs: bool = True

    def _get_context_vars(
        self,
        request: UseCaseRequest,
    ) -> dict[str, Any]:  # noqa: ARG002
        """
        !!! WARNING:
            This method is calling out of transaction so do not make db
            queries in this method.
        """
        return {
            "use_case": self.__class__.__name__,
        }

    @transaction.atomic()
    def _execute(self, request: UseCaseRequest) -> UseCaseResponse:
        raise NotImplementedError()

    def execute(self, request: UseCaseRequest) -> UseCaseResponse:
        with structlog.contextvars.bound_contextvars(
            **self._get_context_vars(request),
        ):
            response = self._execute(request)
            if self.__class__.is_save_event_logs and response.result is not None:
                event_models = self._convert_response_to_log_models(response.result)
                with get_event_saver(settings.EVENT_SAVER_TYPE)() as es:
                    es.insert(event_models)
        return response

    @classmethod
    @abc.abstractmethod
    def _convert_response_to_log_models(cls, response_result: Any) -> list[Model]:
        pass
