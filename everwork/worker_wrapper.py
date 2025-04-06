import asyncio
import time
from abc import ABC, abstractmethod

from loguru import logger
from orjson import loads, JSONDecodeError
from redis.asyncio import Redis

from everwork.process import Resources
from everwork.utils import AwaitLock
from everwork.worker import BaseWorker


class BaseWorkerWrapper(ABC):

    def __init__(self, redis: Redis, worker: BaseWorker, await_lock: AwaitLock):
        self._redis = redis
        self.await_lock = await_lock

        self.worker = worker
        self.settings = self.worker.settings()

    @abstractmethod
    async def get_kwargs(self) -> Resources:
        raise NotImplementedError


class TriggerWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> Resources:
        last_time = await self._redis.get(f'worker:{self.settings.name}:last_time')

        with self.await_lock:
            await asyncio.sleep(max(self.settings.mode.timeout - (time.time() - float(last_time or 0)), 0))

        await self._redis.set(f'worker:{self.settings.name}:last_time', time.time())

        return Resources(kwargs={})


class TriggerWithQueueWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> Resources:
        last_time = await self._redis.get(f'worker:{self.settings.name}:last_time')

        now = time.time()
        timeout = max(self.settings.mode.timeout - (now - float(last_time or 0)), 0)

        if int(timeout) > 0:
            with self.await_lock:
                event = await self._redis.blmove(
                    f'worker:{self.settings.name}:events',
                    f'worker:{self.settings.name}:taken_events',
                    timeout=int(timeout)
                )

            if event is not None:
                try:
                    event_obj = loads(event)
                except JSONDecodeError as error:
                    logger.exception(f'Ошибка при преобразовании события: {error}')
                    return Resources(event=event, status='error')

                return Resources(kwargs=event_obj, event=event)

        with self.await_lock:
            await asyncio.sleep(timeout - (time.time() - now))

        await self._redis.set(f'worker:{self.settings.name}:last_time', time.time())

        return Resources(kwargs={})


class ExecutorWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> Resources:
        with self.await_lock:
            event = await self._redis.blmove(
                f'worker:{self.settings.name}:events',
                f'worker:{self.settings.name}:taken_events',
                timeout=0
            )

        try:
            event_obj = loads(event)
        except JSONDecodeError as error:
            logger.exception(f'Ошибка при преобразовании события: {error}')
            return Resources(event=event, status='error')

        return Resources(kwargs=event_obj, event=event)


class ExecutorWithLimitArgsWorkerWrapper(BaseWorkerWrapper):

    async def get_kwargs(self) -> Resources:
        with self.await_lock:
            event = await self._redis.blmove(
                f'worker:{self.settings.name}:events',
                f'worker:{self.settings.name}:taken_events',
                timeout=0
            )

        try:
            event_obj = loads(event)
        except JSONDecodeError as error:
            logger.exception(f'Ошибка при преобразовании события: {error}')
            return Resources(event=event, status='error')

        with self.await_lock:
            limit_args = await self._redis.blmove(
                f'worker:{self.settings.name}:limit_args',
                f'worker:{self.settings.name}:taken_limit_args',
                timeout=0
            )

        try:
            limit_args_obj = loads(limit_args)
        except JSONDecodeError as error:
            logger.exception(f'Ошибка при преобразовании limit_args: {error}')
            return Resources(event=event, limit_args=limit_args, status='error')

        return Resources(kwargs=event_obj | limit_args_obj, event=event, limit_args=limit_args)
