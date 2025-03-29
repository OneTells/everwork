import time
from abc import ABC, abstractmethod
from typing import Any

from orjson import loads
from redis.asyncio import Redis

from everwork.resource import BaseResource, EventResource, LimitArgsResource
from everwork.worker import BaseWorker


class BaseWorkerWrapper(ABC):

    def __init__(self, redis: Redis, worker: type[BaseWorker], move_by_value_script_sha: str):
        self.__redis = redis
        self.__worker = worker()
        self.__move_by_value_script_sha = move_by_value_script_sha

        self.__worker_sleep_end_time = 0

    @property
    def worker(self) -> BaseWorker:
        return self.__worker

    async def check_worker_is_on(self) -> bool:
        if time.time() < self.__worker_sleep_end_time:
            return False

        worker_is_on = await self.__redis.get(f'worker:{self.__worker.settings().name}:is_worker_on')

        if not worker_is_on:
            self.__worker_sleep_end_time = time.time() + 60
            return False

        return True

    @abstractmethod
    async def get_kwargs(self) -> tuple[dict[str, Any] | None, list[BaseResource]]:
        raise NotImplementedError


class TriggerWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> tuple[dict[str, Any] | None, list[BaseResource]]:
        last_time = await self.__redis.get(f'worker:{self.__worker.settings().name}:last_time')

        if last_time is not None and time.time() < last_time + self.__worker.settings().mode.timeout:
            return None, []

        await self.__redis.set(f'worker:{self.__worker.settings().name}:last_time', time.time())

        return {}, []


class TriggerWithQueueWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> tuple[dict[str, Any] | None, list[BaseResource]]:
        last_time = await self.__redis.get(f'worker:{self.__worker.settings().name}:last_time')

        if last_time is not None and time.time() < last_time + self.__worker.settings().mode.timeout:
            event = await self.__redis.lmove(
                f'worker:{self.__worker.settings().name}:events',
                f'worker:{self.__worker.settings().name}:taken_events'
            )

            if event is None:
                return None, []

            return {}, [EventResource(self.__worker.settings().name, event, self.__move_by_value_script_sha)]

        await self.__redis.set(f'worker:{self.__worker.settings().name}:last_time', time.time())

        return {}, []


class ExecutorWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> tuple[dict[str, Any] | None, list[BaseResource]]:
        event = await self.__redis.lmove(
            f'worker:{self.__worker.settings().name}:events',
            f'worker:{self.__worker.settings().name}:taken_events'
        )

        if event is None:
            return None, []

        return loads(event), [EventResource(self.__worker.settings().name, event, self.__move_by_value_script_sha)]


class ExecutorWithLimitArgsWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> tuple[dict[str, Any] | None, list[BaseResource]]:
        event = await self.__redis.lmove(
            f'worker:{self.__worker.settings().name}:events',
            f'worker:{self.__worker.settings().name}:taken_events'
        )

        if event is None:
            return None, []

        limit_args = await self.__redis.blmove(
            f'worker:{self.__worker.settings().name}:limit_args',
            f'worker:{self.__worker.settings().name}:taken_limit_args',
            timeout=0
        )

        resources = [
            EventResource(self.__worker.settings().name, event, self.__move_by_value_script_sha),
            LimitArgsResource(self.__worker.settings().name, limit_args, self.__move_by_value_script_sha)
        ]

        worker_is_on = await self.check_worker_is_on()

        if not worker_is_on:
            return None, resources

        return loads(event), resources
