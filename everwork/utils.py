import asyncio

from orjson import dumps
from redis.asyncio import Redis

from everwork.worker import Event


async def register_move_by_value_script(redis: Redis) -> str:
    return await redis.script_load(
        """
        local source_list = KEYS[1]
        local destination_list = KEYS[2]
        local value = ARGV[1]
    
        redis.call('LREM', source_list, 1, value)
        redis.call('RPUSH', destination_list, value)
        """
    )


async def register_set_state_script(redis: Redis) -> str:
    return await redis.script_load(
        """
        local key = KEYS[1]
        local value = ARGV[1]

        redis.call("DEL", key)
        redis.call("LPUSH", key, value)
        """
    )


async def return_limit_args(redis: Redis, script_sha: str, worker_name: str, raw_value: str | None) -> None:
    if raw_value is not None:
        await redis.evalsha(
            script_sha, 2, f'worker:{worker_name}:taken_limit_args', f'worker:{worker_name}:limit_args', raw_value
        )

    return None


async def cancel_event(redis: Redis, script_sha: str, worker_name: str, raw_value: str | None) -> None:
    if raw_value is not None:
        await redis.evalsha(
            script_sha,
            2, f'worker:{worker_name}:taken_events', f'worker:{worker_name}:events',
            raw_value
        )

    return None


async def set_error_event(redis: Redis, script_sha: str, worker_name: str, raw_value: str | None) -> None:
    if raw_value is not None:
        await redis.evalsha(
            script_sha,
            2, f'worker:{worker_name}:taken_events', f'worker:{worker_name}:error_events',
            raw_value
        )

    return None


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


async def get_is_worker_on(redis: Redis, worker_name: str) -> bool:
    worker_is_on = await redis.get(f'worker:{worker_name}:is_worker_on')
    return bool(int(worker_is_on))


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
