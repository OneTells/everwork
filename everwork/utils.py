import asyncio

from loguru import logger
from orjson import dumps
from redis.asyncio import Redis

from everwork.process import ProcessState
from everwork.worker import Event


async def register_move_by_value_script(redis: Redis) -> str:
    return await redis.script_load(
        """
        local value = ARGV[1]
        local source_list = KEYS[1]
        local destination_list = KEYS[2]
    
        local index = redis.call('LPOS', source_list, value)
    
        if index then
            redis.call('LREM', source_list, 0, value)
            redis.call('RPUSH', destination_list, value)
            return 1
        else
            return 0
        end
        """
    )


async def return_limit_args(redis: Redis, script_sha: str, worker_name: str, raw_value: str | None) -> None:
    if raw_value is None:
        return None

    value = await redis.evalsha(
        script_sha, 2, f'worker:{worker_name}:taken_limit_args', f'worker:{worker_name}:limit_args', raw_value
    )

    if bool(int(value)):
        return None

    logger.warning(f'Невозможно вернуть {raw_value} в {worker_name}')


async def cancel_event(redis: Redis, script_sha: str, worker_name: str, raw_value: str | None) -> None:
    if raw_value is None:
        return None

    value = await redis.evalsha(
        script_sha,
        2, f'worker:{worker_name}:taken_events', f'worker:{worker_name}:events',
        raw_value
    )

    if bool(int(value)):
        return None

    logger.warning(f'Невозможно отменить событие {raw_value} в {worker_name}')


async def remove_event(redis: Redis, worker_name: str, raw_value: str | None) -> None:
    if raw_value is None:
        return None

    await redis.lrem(f'worker:{worker_name}:taken_events', 1, raw_value)


async def set_error_event(redis: Redis, script_sha: str, worker_name: str, raw_value: str | None) -> None:
    if raw_value is None:
        return None

    value = await redis.evalsha(
        script_sha,
        2, f'worker:{worker_name}:taken_events', f'worker:{worker_name}:error_events',
        raw_value
    )

    if bool(int(value)):
        return None

    logger.warning(f'Невозможно поместить событие {raw_value} в ошибки в {worker_name}')


async def set_process_state(redis: Redis, index: int, end_time: float | None) -> None:
    await redis.set(
        f'process:{index}:state',
        ProcessState(status='waiting' if end_time is None else 'running', end_time=end_time).model_dump_json()
    )


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


class AwaitLock:

    def __init__(self, close_event: CloseEvent):
        self.__is_await = False
        self.__close_event = close_event

    def __call__(self):
        return self.__is_await

    def __enter__(self):
        self.__is_await = True

        if self.__close_event.get():
            self.__is_await = False
            raise asyncio.CancelledError()

        return self

    def __exit__(self, *_):
        self.__is_await = False
