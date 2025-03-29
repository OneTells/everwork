import time
from abc import ABC, abstractmethod

from orjson import loads
from redis.asyncio import Redis

from everwork.worker import BaseWorker, Resources


class BaseWorkerWrapper(ABC):

    def __init__(self, redis: Redis, worker: type[BaseWorker]):
        self.__redis = redis
        self.__worker = worker()

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
    async def get_kwargs(self) -> Resources:
        raise NotImplementedError


class TriggerWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> Resources:
        last_time = await self.__redis.get(f'worker:{self.__worker.settings().name}:last_time')

        if last_time is not None and time.time() < last_time + self.__worker.settings().mode.timeout:
            return Resources()

        await self.__redis.set(f'worker:{self.__worker.settings().name}:last_time', time.time())

        return Resources(kwargs={})


class TriggerWithQueueWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> Resources:
        last_time = await self.__redis.get(f'worker:{self.__worker.settings().name}:last_time')

        if last_time is not None and time.time() < last_time + self.__worker.settings().mode.timeout:
            event = await self.__redis.lmove(
                f'worker:{self.__worker.settings().name}:events',
                f'worker:{self.__worker.settings().name}:taken_events'
            )

            if event is None:
                return Resources()

            return Resources(kwargs={}, event=event)

        await self.__redis.set(f'worker:{self.__worker.settings().name}:last_time', time.time())

        return Resources(kwargs={})


class ExecutorWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> Resources:
        event = await self.__redis.lmove(
            f'worker:{self.__worker.settings().name}:events',
            f'worker:{self.__worker.settings().name}:taken_events'
        )

        if event is None:
            return Resources()

        return Resources(kwargs=loads(event), event=event)


class ExecutorWithLimitArgsWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> Resources:
        event = await self.__redis.lmove(
            f'worker:{self.__worker.settings().name}:events',
            f'worker:{self.__worker.settings().name}:taken_events'
        )

        if event is None:
            return Resources()

        limit_args = await self.__redis.blmove(
            f'worker:{self.__worker.settings().name}:limit_args',
            f'worker:{self.__worker.settings().name}:taken_limit_args',
            timeout=0
        )

        worker_is_on = await self.check_worker_is_on()

        if not worker_is_on:
            return Resources(event=event, limit_args=limit_args)

        return Resources(kwargs=loads(event), event=event, limit_args=limit_args)
