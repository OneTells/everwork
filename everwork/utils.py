from asyncio import CancelledError
from typing import Self

from orjson import dumps
from pydantic_core import to_jsonable_python
from redis.asyncio import Redis
from redis.exceptions import NoScriptError

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
        self.__redis = redis
        self.__settings = settings

        self.__events: list[WorkerEvent] = []
        self.__script_sha: str | None = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.flush_events()

    async def __register_script(self) -> None:
        self.__script_sha = await self.__redis.script_load(
            """
            for i = 1, #ARGV, 2 do
                local stream_key = ARGV[i]
                local event_data = ARGV[i+1]
                redis.call('XADD', stream_key, '*', 'data', event_data)
            end
            """
        )

    async def add_event(self, event: WorkerEvent) -> None:
        self.__events.append(event)

        if len(self.__events) >= self.__settings.max_batch_size:
            await self.flush_events()

    async def flush_events(self) -> None:
        if not self.__events:
            return

        if self.__script_sha is None:
            await self.__register_script()

        args = [item for event in self.__events for item in (event.target_stream, dumps(to_jsonable_python(event.data)))]

        try:
            await self.__redis.evalsha(self.__script_sha, 0, *args)
        except NoScriptError:
            await self.__register_script()
            await self.__redis.evalsha(self.__script_sha, 0, *args)

        self.__events.clear()
