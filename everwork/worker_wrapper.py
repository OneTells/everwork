import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any

from loguru import logger
from orjson import loads, JSONDecodeError
from redis.asyncio import Redis

from everwork.process import Resources
from everwork.utils import SafeCancellationZone
from everwork.worker import BaseWorker


class BaseWorkerWrapper(ABC):

    def __init__(self, redis: Redis, worker: BaseWorker, safe_cancellation_zone: SafeCancellationZone):
        self._redis = redis
        self.safe_cancellation_zone = safe_cancellation_zone

        self.worker = worker
        self.settings = self.worker.settings()

        self._event: str | None = None
        self._limit_args: str | None = None

    def clear(self):
        self._event = None
        self._limit_args = None

    async def get_resources(self) -> Resources:
        try:
            kwargs = await self._get_kwargs()
        except asyncio.CancelledError:
            return Resources(event=self._event, limit_args=self._limit_args, status='cancel')
        except Exception as error:
            _ = error
            return Resources(event=self._event, limit_args=self._limit_args, status='error')

        return Resources(kwargs=kwargs, event=self._event, limit_args=self._limit_args, status='success')

    @abstractmethod
    async def _get_kwargs(self) -> dict[str, Any]:
        raise NotImplementedError


class TriggerWorkerWrapper(BaseWorkerWrapper):

    async def _get_kwargs(self) -> dict[str, Any]:
        last_time = await self._redis.get(f'worker:{self.settings.name}:last_time')

        with self.safe_cancellation_zone:
            await asyncio.sleep(max(self.settings.mode.timeout - (time.time() - float(last_time or 0)), 0))

        await self._redis.set(f'worker:{self.settings.name}:last_time', time.time())

        return {}


class TriggerWithQueueWorkerWrapper(BaseWorkerWrapper):

    async def _get_kwargs(self) -> dict[str, Any]:
        last_time = await self._redis.get(f'worker:{self.settings.name}:last_time')

        now = time.time()
        timeout = max(self.settings.mode.timeout - (now - float(last_time or 0)), 0)

        if int(timeout) > 0:
            with self.safe_cancellation_zone:
                self._event = await self._redis.blmove(
                    f'worker:{self.settings.name}:events',
                    f'worker:{self.settings.name}:taken_events',
                    timeout=int(timeout)
                )

            if self._event is not None:
                try:
                    event_obj = loads(self._event)
                except JSONDecodeError as error:
                    logger.exception(f'Ошибка при преобразовании события: {error}')
                    raise error

                return event_obj

        with self.safe_cancellation_zone:
            await asyncio.sleep(timeout - (time.time() - now))

        await self._redis.set(f'worker:{self.settings.name}:last_time', time.time())

        return {}


class ExecutorWorkerWrapper(BaseWorkerWrapper):

    async def _get_kwargs(self) -> dict[str, Any]:
        with self.safe_cancellation_zone:
            self._event = await self._redis.blmove(
                f'worker:{self.settings.name}:events',
                f'worker:{self.settings.name}:taken_events',
                timeout=0
            )

        try:
            event_obj = loads(self._event)
        except JSONDecodeError as error:
            logger.exception(f'Ошибка при преобразовании события: {error}')
            raise error

        return event_obj


class ExecutorWithLimitArgsWorkerWrapper(BaseWorkerWrapper):

    async def _get_kwargs(self) -> dict[str, Any]:
        with self.safe_cancellation_zone:
            self._event = await self._redis.blmove(
                f'worker:{self.settings.name}:events',
                f'worker:{self.settings.name}:taken_events',
                timeout=0
            )

        try:
            event_obj = loads(self._event)
        except JSONDecodeError as error:
            logger.exception(f'Ошибка при преобразовании события: {error}')
            raise error

        with self.safe_cancellation_zone:
            self._limit_args = await self._redis.blmove(
                f'worker:{self.settings.name}:limit_args',
                f'worker:{self.settings.name}:taken_limit_args',
                timeout=0
            )

        try:
            limit_args_obj = loads(self._limit_args)
        except JSONDecodeError as error:
            logger.exception(f'Ошибка при преобразовании limit_args: {error}')
            raise error

        return event_obj | limit_args_obj
