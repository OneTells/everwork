from abc import ABC, abstractmethod
from typing import Any, Annotated

from pydantic import BaseModel, Field


class ExecutorMode(BaseModel):
    processing_streams: Annotated[list[str], Field(min_length=1)]
    limited_args: Annotated[list[dict[str, Any]], Field(min_length=1)] | None = None


class TriggerMode(BaseModel):
    timeout: Annotated[float, Field(gt=0)]
    processing_streams: Annotated[list[str], Field(min_length=1)] | None = None


class Settings(BaseModel):
    name: Annotated[str, Field(min_length=1)]
    mode: ExecutorMode | TriggerMode

    max_working_timeout: Annotated[float, Field(gt=0)] = 180
    worker_recovery_interval: Annotated[float, Field(gt=0)] = 60


class Event(BaseModel):
    name: Annotated[str, Field(min_length=1)]
    kwargs: dict[str, Any] = Field(default_factory=dict)


class BaseWorker(ABC):
    settings: Settings

    def __init_subclass__(cls) -> None:
        cls.settings = cls.get_settings()

    @staticmethod
    @abstractmethod
    def get_settings() -> Settings:
        raise NotImplementedError

    async def startup(self) -> None:
        return

    async def shutdown(self) -> None:
        return

    @abstractmethod
    async def __call__(self, **kwargs) -> list[Event] | None:
        raise NotImplementedError
