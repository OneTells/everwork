from abc import ABC, abstractmethod
from typing import Any, ClassVar, Annotated, Self, final

from pydantic import BaseModel, Field, model_validator, ConfigDict
from redis.backoff import FullJitterBackoff, AbstractBackoff

from ._utils import EventPublisher
from .schemas import WorkerSettings, WorkerEvent, TriggerMode
from .stream_client import StreamClient


class AbstractWorker(ABC):
    settings: ClassVar[WorkerSettings]

    def __init_subclass__(cls, /, init_settings: bool = True, **kwargs) -> None:
        super().__init_subclass__(**kwargs)

        if not init_settings:
            return

        cls.settings = cls._get_settings()

    def __init__(self) -> None:
        self.__event_publisher: EventPublisher | None = None

    @final
    def initialize(self, stream_client: StreamClient) -> None:
        self.__event_publisher = EventPublisher(stream_client, self.settings.event_publisher_settings)

    @final
    @property
    def event_publisher(self) -> EventPublisher:
        if self.__event_publisher is None:
            raise ValueError('Не был вызван метод initialize')

        return self.__event_publisher

    @final
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


@final
class Process(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    workers: list[type[AbstractWorker]]

    shutdown_timeout: Annotated[float, Field(gt=0)] = 20
    redis_backoff_strategy: AbstractBackoff = Field(default_factory=lambda: FullJitterBackoff(cap=30.0, base=1.0))


@final
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
