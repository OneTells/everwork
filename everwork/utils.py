import asyncio
from asyncio import CancelledError
from contextlib import suppress
from typing import Self, Coroutine, Any

from .schemas import EventPublisherSettings, WorkerEvent
from .stream_client import StreamClient


def timer(*, weeks: int = 0, days: int = 0, hours: int = 0, minutes: int = 0, seconds: int = 0, milliseconds: int = 0) -> float:
    return weeks * 604800 + days * 86400 + hours * 3600 + minutes * 60 + seconds + milliseconds / 1000


class EventPublisher:

    def __init__(self, stream_client: StreamClient, settings: EventPublisherSettings) -> None:
        self.__stream_client = stream_client
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


class ThreadSafeEventChannel[T]:

    def __init__(self) -> None:
        self.__loop: asyncio.AbstractEventLoop | None = None

        self.__data: T | None = None
        self.__waiter: asyncio.Future[bool] | None = None

        self.__is_closed = False

    def __notify_waiter(self) -> None:
        if (future := self.__waiter) is None:
            return

        self.__waiter = None
        if not future.done():
            future.set_result(True)

    def __cancel(self) -> None:
        future = self.__waiter

        if not future or future.done():
            return

        future.cancel()

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self.__loop = loop

    def put(self, data: T) -> None:
        self.__data = data
        # noinspection PyTypeChecker
        self.__loop.call_soon_threadsafe(self.__notify_waiter)

    async def get(self) -> T:
        if self.__is_closed:
            raise CancelledError()

        if (item := self.__data) is not None:
            self.__data = None
            return item

        future = self.__loop.create_future()
        self.__waiter = future

        try:
            await future
        except asyncio.CancelledError:
            self.__waiter = None
            raise

        item = self.__data
        self.__data = None
        return item

    def cancel(self) -> None:
        if self.__is_closed:
            return

        self.__is_closed = True
        # noinspection PyTypeChecker
        self.__loop.call_soon_threadsafe(self.__cancel)


async def wait_for_or_cancel[T](coroutine: Coroutine[Any, Any, T], event: asyncio.Event) -> T:
    main_task = asyncio.create_task(coroutine)
    event_task = asyncio.create_task(event.wait())

    tasks = {main_task, event_task}

    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        if event.is_set():
            main_task.cancel()

            with suppress(asyncio.CancelledError):
                await main_task

            raise asyncio.CancelledError()

        return await main_task
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

        await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))
