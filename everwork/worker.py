from abc import ABC, abstractmethod
from typing import Any, Annotated

from pydantic import BaseModel, Field


class Timeout(BaseModel):
    inactive: Annotated[float, Field(ge=0, default=5)]
    active: Annotated[float, Field(ge=0, default=0.01)]
    active_lifetime: Annotated[float, Field(ge=0, default=10)]


class ExecutorMode(BaseModel):
    limited_args: Annotated[list[dict[str, Any]] | None, Field(default=None)]


class TriggerMode(BaseModel):
    timeout: Annotated[float, Field(gt=0)]
    with_queue_events: Annotated[bool, Field(default=False)]


class Settings(BaseModel):
    name: Annotated[str, Field()]
    timeout: Annotated[Timeout, Field(default_factory=Timeout)]
    timeout_reset: Annotated[float, Field(gt=0, default=180)]

    mode: Annotated[ExecutorMode | TriggerMode, Field()]


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
