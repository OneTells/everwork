from abc import ABC, abstractmethod
from typing import Annotated, Any, Self

from pydantic import BaseModel, Field, model_validator


class ExecutorMode(BaseModel):
    source_streams: Annotated[list[str], Field(min_length=1)]

    limited_args: Annotated[list[dict[str, Any]], Field(min_length=1)] | None = None


class TriggerMode(BaseModel):
    execution_interval: Annotated[float, Field(gt=0)]

    source_streams: Annotated[list[str], Field(min_length=1)] | None = None


class WorkerSettings(BaseModel):
    name: Annotated[str, Field(min_length=1)]
    mode: ExecutorMode | TriggerMode

    execution_timeout: Annotated[float, Field(gt=0)] = 180


class WorkerEvent(BaseModel):
    target_stream: Annotated[str, Field(min_length=1)]
    data: dict[str, Any] = Field(default_factory=dict)


class BaseWorker(ABC):
    settings: WorkerSettings

    def __init_subclass__(cls) -> None:
        cls.settings = cls.get_settings()

    @staticmethod
    @abstractmethod
    def get_settings() -> WorkerSettings:
        raise NotImplementedError

    async def startup(self) -> None:
        return

    async def shutdown(self) -> None:
        return

    @abstractmethod
    async def __call__(self, **kwargs) -> list[WorkerEvent] | None:
        raise NotImplementedError


class ProcessGroup(BaseModel):
    workers: list[type[BaseWorker]]
    replicas: Annotated[int, Field(ge=1)] = 1

    @model_validator(mode='after')
    def validator(self) -> Self:
        if self.replicas == 1:
            if any(
                isinstance(worker.settings.mode, ExecutorMode) and worker.settings.mode.limited_args is not None
                for worker in self.workers
            ):
                raise ValueError('При использовании LimitArgs должна быть репликация')

        if self.replicas > 1:
            if len(self.workers) > 1:
                raise ValueError('Репликация не работает с несколькими worker')

            if any(isinstance(worker.settings.mode, TriggerMode) for worker in self.workers):
                raise ValueError('Репликация не работает с workers в режиме TriggerMode')

        return self
