from abc import ABC, abstractmethod
from typing import Any, ClassVar, Annotated, Self

from pydantic import BaseModel, Field, model_validator
from redis.asyncio import Redis

from .schemas import WorkerSettings, WorkerEvent, TriggerMode
from .stream_client import StreamClient
from .utils import EventPublisher


class BaseWorker(ABC):
    settings: ClassVar[WorkerSettings]

    def __init_subclass__(cls, /, init_settings: bool = True, **kwargs) -> None:
        super().__init_subclass__(**kwargs)

        if not init_settings:
            return

        cls.settings = cls._get_settings()

    def __init__(self) -> None:
        self.event_publisher: EventPublisher | None = None

    def initialize(self, redis: Redis) -> None:
        self.event_publisher = EventPublisher(StreamClient(redis), self.settings.event_publisher_settings)

    async def _add_event(self, event: WorkerEvent | list[WorkerEvent]) -> None:
        await self.event_publisher.add(event)

    @classmethod
    @abstractmethod
    def _get_settings(cls) -> WorkerSettings:
        raise NotImplementedError

    async def shutdown(self) -> None:
        pass

    async def startup(self) -> None:
        pass

    @abstractmethod
    async def __call__(self, **kwargs: Any) -> None:
        raise NotImplementedError


class Process(BaseModel):
    workers: list[type[BaseWorker]]

    shutdown_timeout: Annotated[float, Field(gt=0)] = 20


class ProcessGroup(BaseModel):
    process: Process
    replicas: Annotated[int, Field(ge=1)] = 1

    @model_validator(mode='after')
    def validator(self) -> Self:
        if self.replicas > 1:
            if len(self.process.workers) > 1:
                raise ValueError('Репликация не работает с несколькими worker')

            if any(isinstance(worker.settings.mode, TriggerMode) for worker in self.process.workers):
                raise ValueError('Репликация не работает с workers в режиме TriggerMode')

        return self
