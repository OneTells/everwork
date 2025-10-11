from abc import ABC, abstractmethod
from typing import Annotated, Any, Self, ClassVar

from pydantic import BaseModel, Field, model_validator
from redis.asyncio import Redis

from .stream_client import StreamClient
from .utils import EventPublisher


class ExecutorMode(BaseModel):
    pass


class TriggerMode(BaseModel):
    execution_interval: Annotated[float, Field(gt=0)]


class EventPublisherSettings(BaseModel):
    max_batch_size: Annotated[int, Field(ge=1)] = 500


class WorkerSettings(BaseModel):
    name: Annotated[str, Field(min_length=1)]

    source_streams: set[str] = Field(default_factory=set)
    mode: ExecutorMode | TriggerMode

    execution_timeout: Annotated[float, Field(gt=0)] = 180
    event_publisher_settings: EventPublisherSettings = Field(default_factory=EventPublisherSettings)


class WorkerEvent(BaseModel):
    target_stream: Annotated[str, Field(min_length=1)]
    data: dict[str, Any] = Field(default_factory=dict)


class BaseWorker(ABC):
    settings: ClassVar[WorkerSettings]

    def __init_subclass__(cls) -> None:
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


class ProcessGroup(BaseModel):
    workers: list[type[BaseWorker]]
    replicas: Annotated[int, Field(ge=1)] = 1

    @model_validator(mode='after')
    def validator(self) -> Self:
        if self.replicas > 1:
            if len(self.workers) > 1:
                raise ValueError('Репликация не работает с несколькими worker')

            if any(isinstance(worker.settings.mode, TriggerMode) for worker in self.workers):
                raise ValueError('Репликация не работает с workers в режиме TriggerMode')

        return self
