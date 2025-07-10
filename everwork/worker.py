from abc import ABC, abstractmethod
from typing import Any, Annotated

from pydantic import BaseModel, Field


class Timeout(BaseModel):
    inactive: Annotated[float, Field(ge=0)] = 5
    active: Annotated[float, Field(ge=0)] = 0.01
    active_lifetime: Annotated[float, Field(ge=0)] = 10


class ExecutorMode(BaseModel):
    limited_args: list[dict[str, Any]] | None = None


class TriggerMode(BaseModel):
    timeout: Annotated[float, Field(gt=0)]
    with_queue_events: bool = False


class Settings(BaseModel):
    name: str
    timeout: Timeout = Field(default_factory=Timeout)
    timeout_reset: Annotated[float, Field(gt=0)] = 180

    mode: ExecutorMode | TriggerMode


class Event(BaseModel):
    target: str
    kwargs: dict[str, Any] = Field(default_factory=dict)


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
