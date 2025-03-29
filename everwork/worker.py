from abc import ABC, abstractmethod
from typing import Any, Annotated

from pydantic import BaseModel, Field


class ExecutorMode(BaseModel):
    limited_args: Annotated[list[dict[str, Any]] | None, Field(default=None)]


class TriggerMode(BaseModel):
    timeout: Annotated[float, Field(gt=0)]
    with_queue_events: Annotated[bool, Field(default=False)]


class Settings(BaseModel):
    name: Annotated[str, Field()]

    mode: Annotated[ExecutorMode | TriggerMode, Field()]
    timeout_reset: Annotated[float, Field(gt=0, default=180)]


class Event(BaseModel):
    target: Annotated[str, Field()]
    kwargs: Annotated[dict[str, Any], Field(default_factory=dict)]


class BaseWorker(ABC):

    @staticmethod
    @abstractmethod
    def settings() -> Settings:
        raise NotImplementedError

    async def startup(self) -> None:
        return

    async def shutdown(self) -> None:
        return

    @abstractmethod
    async def __call__(self, **kwargs) -> list[Event] | None:
        raise NotImplementedError


class Resources(BaseModel):
    kwargs: Annotated[dict[str, Any] | None, Field(default=None)]
    event: Annotated[str | None, Field(default=None)]
    limit_args: Annotated[str | None, Field(default=None)]
