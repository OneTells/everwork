import asyncio

from orjson import dumps
from redis.asyncio import Redis

from everwork.worker import Event


async def push_event(redis: Redis, event: Event) -> None:
    await redis.rpush(f'worker:{event.target}:events', dumps(event.kwargs))


async def get_worker_parameters(redis: Redis) -> dict[str, dict[str, str]]:
    keys = await redis.keys('worker:*')
    values = await redis.mget(keys)

    stats: dict[str, dict[str, str]] = {}

    for key, value in zip(keys, values):
        worker, parameter = key.removeprefix('worker:').split(':')

        if stats.get(worker, None) is None:
            stats[worker] = {}

        stats.get(worker).update({parameter: value})

    return stats


def timer(*, hours: int = 0, minutes: int = 0, seconds: int = 0, milliseconds: int = 0) -> float:
    return hours * 3600 + minutes * 60 + seconds + milliseconds / 1000


class CloseEvent:

    def __init__(self):
        self.__is_close = False

    def get(self):
        return self.__is_close

    def set(self):
        self.__is_close = True


class SafeCancellationZone:

    def __init__(self, close_event: CloseEvent):
        self.__is_use = False
        self.__close_event = close_event

    def is_use(self):
        return self.__is_use

    def __enter__(self):
        self.__is_use = True

        if self.__close_event.get():
            self.__is_use = False
            raise asyncio.CancelledError()

        return self

    def __exit__(self, *_):
        self.__is_use = False
