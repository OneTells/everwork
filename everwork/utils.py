from asyncio import CancelledError
from typing import Self

from redis.asyncio import Redis

from everwork.stream_client import StreamClient
from everwork.worker_base import EventPublisherSettings, WorkerEvent


def timer(*, hours: int = 0, minutes: int = 0, seconds: int = 0, milliseconds: int = 0) -> float:
    return hours * 3600 + minutes * 60 + seconds + milliseconds / 1000


class ShutdownEvent:

    def __init__(self) -> None:
        self.__value = False

    def is_set(self) -> bool:
        return self.__value

    def set(self) -> None:
        self.__value = True


class ShutdownSafeZone:

    def __init__(self, shutdown_event: ShutdownEvent) -> None:
        self.__shutdown_event = shutdown_event
        self.__is_use = False

    def __enter__(self) -> Self:
        if self.__shutdown_event.is_set():
            raise CancelledError()

        self.__is_use = True
        return self

    def __exit__(self, *_) -> None:
        self.__is_use = False

    def is_use(self) -> bool:
        return self.__is_use


class EventPublisher:

    def __init__(self, redis: Redis, settings: EventPublisherSettings) -> None:
        self.__stream_client = StreamClient(redis)
        self.__settings = settings

        self.__events: list[WorkerEvent] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.publish()

    async def add(self, event: WorkerEvent | list[WorkerEvent]) -> None:
        if isinstance(event, WorkerEvent):
            self.__events.append(event)
        else:
            self.__events.extend(event)

        if len(self.__events) >= self.__settings.max_batch_size:
            await self.publish()

    async def publish(self) -> None:
        if not self.__events:
            return

        await self.__stream_client.push_event(self.__events)
        self.__events.clear()
