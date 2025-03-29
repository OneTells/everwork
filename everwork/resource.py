from abc import ABC, abstractmethod

from loguru import logger
from redis.asyncio import Redis


class BaseResource(ABC):

    @abstractmethod
    async def cancel(self, redis: Redis) -> None:
        raise NotImplementedError

    @abstractmethod
    async def success(self, redis: Redis) -> None:
        raise NotImplementedError

    @abstractmethod
    async def error(self, redis: Redis) -> None:
        raise NotImplementedError


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


class LimitArgsResource(BaseResource):

    def __init__(self, worker_name: str, raw_kwargs: str, script_sha: str):
        self.__worker_name = worker_name
        self.__raw_kwargs = raw_kwargs
        self.__script_sha = script_sha

    async def cancel(self, redis: Redis) -> None:
        value = await redis.evalsha(
            self.__script_sha,
            2, f'worker:{self.__worker_name}:taken_limit_args', f'worker:{self.__worker_name}:limit_args',
            self.__raw_kwargs
        )

        if value:
            return None

        logger.warning(f'Невозможно отменить событие {self.__raw_kwargs} в {self.__worker_name}')

    async def success(self, redis: Redis) -> None:
        value = await redis.evalsha(
            self.__script_sha,
            2, f'worker:{self.__worker_name}:taken_limit_args', f'worker:{self.__worker_name}:limit_args',
            self.__raw_kwargs
        )

        if value:
            return None

        logger.warning(f'Невозможно отменить событие {self.__raw_kwargs} в {self.__worker_name}')

    async def error(self, redis: Redis) -> None:
        value = await redis.evalsha(
            self.__script_sha,
            2, f'worker:{self.__worker_name}:taken_limit_args', f'worker:{self.__worker_name}:limit_args',
            self.__raw_kwargs
        )

        if value:
            return None

        logger.warning(f'Невозможно отменить событие {self.__raw_kwargs} в {self.__worker_name}')


class EventResource(BaseResource):

    def __init__(self, worker_name: str, raw_kwargs: str, script_sha: str):
        self.__worker_name = worker_name
        self.__raw_kwargs = raw_kwargs
        self.__script_sha = script_sha

    async def cancel(self, redis: Redis) -> None:
        value = await redis.evalsha(
            self.__script_sha,
            2, f'worker:{self.__worker_name}:taken_events', f'worker:{self.__worker_name}:events',
            self.__raw_kwargs
        )

        if value:
            return None

        logger.warning(f'Невозможно отменить событие {self.__raw_kwargs} в {self.__worker_name}')

    async def success(self, redis: Redis) -> None:
        await redis.lrem(f'worker:{self.__worker_name}:taken_events', 1, self.__raw_kwargs)

    async def error(self, redis: Redis) -> None:
        value = await redis.evalsha(
            self.__script_sha,
            2, f'worker:{self.__worker_name}:taken_events', f'worker:{self.__worker_name}:error_events',
            self.__raw_kwargs
        )

        if value:
            return None

        logger.warning(f'Невозможно поместить событие {self.__raw_kwargs} в ошибки в {self.__worker_name}')
