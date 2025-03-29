import time
from abc import ABC, abstractmethod
from typing import Any

from orjson import dumps
from redis.asyncio import Redis

from everwork.resource import BaseResource, EventResource
from everwork.worker import BaseWorker, Event


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

    async def push_events(self, events: list[Event] | None) -> None:
        if events is None:
            return None

        pipeline = self.__redis.pipeline()

        for event in events:
            await pipeline.rpush(f'worker:{event.target}:events', dumps(event))

        await pipeline.execute()

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
        resources = []

        last_time = await self.__redis.get(f'worker:{self.__worker.settings().name}:last_time')

        if last_time is not None and time.time() < last_time + self.__worker.settings().mode.timeout:
            event = await self.__redis.lmove(
                f'worker:{self.__worker.settings().name}:events',
                f'worker:{self.__worker.settings().name}:taken_events'
            )

            if event is None:
                return None, resources

            resources.append(EventResource(self.__worker.settings().name, event, self.__move_by_value_script_sha))
            return {}, resources

        await self.__redis.set(f'worker:{self.__worker.settings().name}:last_time', time.time())

        return {}, resources


class ExecutorWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> tuple[dict[str, Any] | None, list[BaseResource]]:
        return None, []


class ExecutorWithLimitArgsWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> tuple[dict[str, Any] | None, list[BaseResource]]:
        return None, []
